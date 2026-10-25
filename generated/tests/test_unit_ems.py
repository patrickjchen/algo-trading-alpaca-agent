"""EMS unit tests. EMS is the Alpaca gateway: we patch ems.trader.* (never the network)."""
import json
from types import SimpleNamespace

import pytest
from alpaca.common.exceptions import APIError

import ems
import oms
import runtime

RULES = {"symbol": "BTC/USD", "min_order_size": 0.0001, "min_trade_increment": 0.0001,
         "price_increment": 0.01, "tradable": True}


def _api_error(code, msg):
    return APIError(json.dumps({"code": code * 100000, "message": msg}),
                    SimpleNamespace(response=SimpleNamespace(status_code=code), request=None))


class FakeTrader:
    """Records submit/cancel calls; returns Alpaca-like order dicts."""

    def __init__(self):
        self.submits = []
        self.cancels = []

    def submit_limit(self, symbol, qty, side, limit_price, client_order_id, live=False, tif="gtc"):
        self.submits.append({"symbol": symbol, "qty": qty, "side": side,
                             "limit_price": limit_price, "client_order_id": client_order_id,
                             "live": live, "tif": tif})
        n = len(self.submits)
        return {"id": f"oid-{n}", "client_order_id": client_order_id, "symbol": symbol,
                "side": side, "type": "limit", "time_in_force": tif, "qty": str(qty),
                "limit_price": str(limit_price), "filled_qty": "0", "status": "accepted"}

    def cancel(self, order_id, live=False):
        self.cancels.append(order_id)


@pytest.fixture
def ft(monkeypatch):
    f = FakeTrader()
    monkeypatch.setattr(ems.trader, "get_asset_rules", lambda *a, **k: dict(RULES))
    monkeypatch.setattr(ems.trader, "latest_quote",
                        lambda *a, **k: {"bid_price": 100.0, "ask_price": 101.0})
    monkeypatch.setattr(ems.trader, "submit_limit", f.submit_limit)
    monkeypatch.setattr(ems.trader, "submit_market",
                        lambda *a, **k: pytest.fail("unexpected market order"))
    monkeypatch.setattr(ems.trader, "cancel", f.cancel)
    monkeypatch.setattr(ems.trader, "get_order_by_client_id", lambda *a, **k: None)
    monkeypatch.setattr(ems.trader, "get_order",
                        lambda oid, live=False: {"id": oid, "filled_qty": "0", "status": "canceled"})
    monkeypatch.setattr(ems, "RETRY_DELAYS", (0.0, 0.0, 0.0))
    monkeypatch.setattr(ems, "_last_lookup", {})
    return f


def _reports(r, event=None):
    out = []
    for _id, fields in r.xrange(runtime.STREAM_TRADE_UPDATES):
        d = json.loads(fields["json"])
        if event is None or d["event"] == event:
            out.append(d)
    return out


def _create(conn, r, total=1.5, n=3, duration=60, strategy="twap", symbol="BTC/USD"):
    return ems.handle_request(conn, r, {"cmd_id": "r1", "kind": "create", "strategy": strategy,
                                        "account": "paper", "symbol": symbol, "side": "buy",
                                        "total_qty": total, "duration_s": duration, "n_slices": n,
                                        "limit_cap": None, "params": {}, "source": "test"})


def _slice_ts(conn, algo_id, idx):
    return ems._ts(ems._get_slices(conn, algo_id)[idx]["scheduled_ts"])


def _single(cmd_id="s1", coid="tc-single1"):
    return {"cmd_id": cmd_id, "kind": "submit", "account": "paper", "client_order_id": coid,
            "symbol": "BTC/USD", "side": "buy", "type": "limit", "qty": 0.01,
            "limit_price": 100.0, "tif": "gtc", "source": "cli"}


# ----------------------------------------------------------------- algo create / cancel
def test_create(db_conn, fake_redis, ft):
    rep = _create(db_conn, fake_redis)
    assert rep["ok"] and rep["status"] == "pending"
    aid = rep["algo_id"]
    assert ems._get_algo(db_conn, aid)["status"] == "pending"
    sl = ems._get_slices(db_conn, aid)
    assert len(sl) == 3
    assert [s["client_order_id"] for s in sl] == [f"algo-{aid[:12]}-{i:03d}" for i in range(3)]
    assert all(s["status"] == "scheduled" for s in sl)
    assert abs(sum(s["qty"] for s in sl) - 1.5) < 1e-9
    assert fake_redis.llen(runtime.reply_key("r1")) == 1


def test_create_vwap_rejected(db_conn, fake_redis, ft):
    rep = _create(db_conn, fake_redis, strategy="vwap")
    assert rep["ok"] is False
    assert "not available" in rep["error"] and "volume" in rep["error"].lower()
    assert db_conn.execute("SELECT COUNT(*) FROM algo_orders").fetchone()[0] == 0


def test_cancel_request(db_conn, fake_redis, ft):
    aid = _create(db_conn, fake_redis)["algo_id"]
    rep = ems.handle_request(db_conn, fake_redis, {"cmd_id": "c1", "kind": "cancel",
                                                   "algo_id": aid})
    assert rep["ok"] and rep["status"] == "cancelled"
    assert ems._get_algo(db_conn, aid)["status"] == "cancelled"
    assert all(s["status"] == "cancelled" for s in ems._get_slices(db_conn, aid))
    unknown = ems.handle_request(db_conn, fake_redis, {"cmd_id": "c2", "kind": "cancel",
                                                       "algo_id": "nope"})
    assert unknown["ok"] is False and unknown["status"] == "not_found"


# ----------------------------------------------------------------- single-order gateway
def test_submit_single_acked_with_report(db_conn, fake_redis, ft):
    rep = ems.handle_request(db_conn, fake_redis, _single())
    assert rep["ok"] and rep["status"] == "acked" and rep["order_id"] == "oid-1"
    assert rep["order"]["client_order_id"] == "tc-single1"
    assert len(ft.submits) == 1 and ft.submits[0]["tif"] == "gtc"
    reps = _reports(fake_redis)
    assert len(reps) == 1
    r0 = reps[0]
    assert r0["event"] == "ems_submitted" and r0["client_order_id"] == "tc-single1"
    assert r0["source"] == "cli" and r0["order"]["id"] == "oid-1"
    assert r0["execution_id"] is None and r0["account"] == "paper"
    assert json.loads(fake_redis.lpop(runtime.reply_key("s1")))["status"] == "acked"


def test_submit_duplicate_coid_already_submitted(db_conn, fake_redis, ft, monkeypatch):
    def boom(*a, **k):
        raise _api_error(422, "client_order_id must be unique")

    monkeypatch.setattr(ems.trader, "submit_limit", boom)
    monkeypatch.setattr(ems.trader, "get_order_by_client_id",
                        lambda coid, live=False: {"id": "oid-old", "client_order_id": coid,
                                                  "status": "new"})
    rep = ems.handle_request(db_conn, fake_redis, _single())
    assert rep["ok"] and rep["status"] == "already_submitted" and rep["order_id"] == "oid-old"
    assert len(_reports(fake_redis, "ems_submitted")) == 1


def test_submit_transient_then_unknown(db_conn, fake_redis, ft, monkeypatch):
    calls = []

    def boom(*a, **k):
        calls.append(1)
        raise RuntimeError("socket hiccup")

    monkeypatch.setattr(ems.trader, "submit_limit", boom)
    monkeypatch.setattr(ems.trader, "is_transient", lambda e: True)
    rep = ems.handle_request(db_conn, fake_redis, _single())
    assert rep["ok"] is False and rep["status"] == "unknown"
    assert len(calls) == 4
    assert _reports(fake_redis) == []


def test_submit_non_transient_rejected(db_conn, fake_redis, ft, monkeypatch):
    def boom(*a, **k):
        raise _api_error(403, "insufficient balance")

    monkeypatch.setattr(ems.trader, "submit_limit", boom)
    rep = ems.handle_request(db_conn, fake_redis, _single())
    assert rep["ok"] is False and rep["status"] == "rejected"


def test_cancel_order_by_coid(db_conn, fake_redis, ft):
    oms.upsert_order(db_conn, {"id": "oid-c", "client_order_id": "tc-c", "symbol": "BTC/USD",
                               "side": "buy", "qty": "1", "status": "new"}, account="paper")
    db_conn.commit()
    rep = ems.handle_request(db_conn, fake_redis, {"cmd_id": "k1", "kind": "cancel_order",
                                                   "client_order_id": "tc-c", "source": "cli"})
    assert rep["ok"] and rep["status"] == "cancel_requested"
    assert ft.cancels == ["oid-c"]
    reps = _reports(fake_redis, "ems_cancel_requested")
    assert len(reps) == 1 and reps[0]["order_id"] == "oid-c"
    assert reps[0]["client_order_id"] == "tc-c"


def test_cancel_order_unknown(db_conn, fake_redis, ft):
    rep = ems.handle_request(db_conn, fake_redis, {"cmd_id": "k2", "kind": "cancel_order",
                                                   "client_order_id": "tc-ghost"})
    assert rep["ok"] is False and rep["status"] == "not_found"
    assert ft.cancels == []


# ----------------------------------------------------------------- TWAP engine
def test_tick_submits_first_slice(db_conn, fake_redis, ft):
    aid = _create(db_conn, fake_redis)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    assert len(ft.submits) == 1
    s = ft.submits[0]
    assert s["limit_price"] == 101.0 and s["side"] == "buy"
    assert s["client_order_id"] == f"algo-{aid[:12]}-000" and s["tif"] == "gtc"
    assert abs(s["qty"] - 0.5) < 1e-9
    assert ems._get_algo(db_conn, aid)["status"] == "running"
    sl0 = ems._get_slices(db_conn, aid)[0]
    assert sl0["status"] == "submitted" and sl0["order_id"] == "oid-1"
    reps = _reports(fake_redis, "ems_submitted")
    assert len(reps) == 1 and reps[0]["algo_id"] == aid and reps[0]["slice_idx"] == 0
    assert reps[0]["source"] == "ems"


def test_final_slice_ioc_crossed(db_conn, fake_redis, ft):
    aid = _create(db_conn, fake_redis)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 2) + 1)
    assert len(ft.submits) == 1
    s = ft.submits[0]
    assert s["tif"] == "ioc" and s["client_order_id"] == f"algo-{aid[:12]}-002"
    assert s["limit_price"] == pytest.approx(101.05) and s["limit_price"] > 101.0
    assert abs(s["qty"] - 1.5) < 1e-9  # missed slices merged into the final one


def test_refresh_from_oms(db_conn, fake_redis, ft):
    aid = _create(db_conn, fake_redis)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    coid = f"algo-{aid[:12]}-000"
    oms.upsert_order(db_conn, {"id": "oid-1", "client_order_id": coid, "symbol": "BTC/USD",
                               "side": "buy", "qty": "0.5", "filled_qty": "0.5",
                               "filled_avg_price": "101", "status": "filled"},
                     account="paper", source="ems")
    db_conn.commit()
    ems.refresh_from_oms(db_conn, aid)
    sl0 = ems._get_slices(db_conn, aid)[0]
    assert sl0["status"] == "filled" and sl0["filled_qty"] == pytest.approx(0.5)
    algo = ems._get_algo(db_conn, aid)
    assert algo["filled_qty"] == pytest.approx(0.5)
    assert algo["avg_px"] == pytest.approx(101.0)


def test_roll_forward(db_conn, fake_redis, ft):
    aid = _create(db_conn, fake_redis)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 1) + 1)
    assert ft.cancels == ["oid-1"]
    sl = ems._get_slices(db_conn, aid)
    assert sl[0]["status"] == "rolled"
    assert sl[1]["qty"] == pytest.approx(1.0) and sl[1]["status"] == "submitted"
    assert len(ft.submits) == 2 and ft.submits[1]["qty"] == pytest.approx(1.0)
    assert ft.submits[1]["tif"] == "gtc"
    assert len(_reports(fake_redis, "ems_cancel_requested")) == 1


def test_broken_algo_isolated(db_conn, fake_redis, ft, monkeypatch):
    good = _create(db_conn, fake_redis, symbol="BTC/USD")["algo_id"]
    bad = _create(db_conn, fake_redis, symbol="ETH/USD")["algo_id"]
    # push the bad algo's window far into the past so a quote failure is fatal for it
    db_conn.execute("UPDATE algo_orders SET end_ts=? WHERE algo_id=?",
                    (ems._iso(1_000_000.0), bad))
    db_conn.commit()

    def quote(symbol, r=None):
        if symbol == "ETH/USD":
            raise RuntimeError("quote feed down")
        return {"bid_price": 100.0, "ask_price": 101.0}

    monkeypatch.setattr(ems.trader, "latest_quote", quote)
    now = max(_slice_ts(db_conn, good, 0), _slice_ts(db_conn, bad, 0)) + 1
    assert ems.tick(db_conn, fake_redis, now_ts=now) == 2
    assert ems._get_algo(db_conn, bad)["status"] == "failed"
    assert ems._get_algo(db_conn, good)["status"] == "running"
    assert [s["symbol"] for s in ft.submits] == ["BTC/USD"]


def test_quote_failure_within_window_waits(db_conn, fake_redis, ft, monkeypatch):
    aid = _create(db_conn, fake_redis)["algo_id"]

    def quote(*a, **k):
        raise RuntimeError("no quote")

    monkeypatch.setattr(ems.trader, "latest_quote", quote)
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    assert ft.submits == []
    assert ems._get_algo(db_conn, aid)["status"] == "running"
