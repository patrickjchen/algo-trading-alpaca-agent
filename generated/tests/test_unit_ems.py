import pytest

import ems
import oms

RULES = {"symbol": "BTC/USD", "min_order_size": 0.0001, "min_trade_increment": 0.0001,
         "price_increment": 0.01, "tradable": True}


@pytest.fixture(autouse=True)
def _stubs(monkeypatch):
    monkeypatch.setattr(ems, "_rules_cache", {})
    monkeypatch.setattr(ems.trader, "get_asset_rules", lambda *a, **k: dict(RULES))
    monkeypatch.setattr(ems.trader, "latest_quote",
                        lambda *a, **k: {"bid_price": 100.0, "ask_price": 101.0})


class Capture:
    def __init__(self):
        self.sent = []

    def __call__(self, r, stream, payload, timeout=10.0):
        self.sent.append(dict(payload))
        if payload["kind"] == "cancel":
            return {"ok": True, "status": "cancel_requested", "order_id": payload.get("order_id")}
        n = len(self.sent)
        return {"ok": True, "status": "acked", "order_id": f"oid-{n}",
                "client_order_id": payload["client_order_id"],
                "order": {"status": "new", "filled_qty": "0"}}

    def submits(self):
        return [p for p in self.sent if p["kind"] == "submit"]

    def cancels(self):
        return [p for p in self.sent if p["kind"] == "cancel"]


def _create(conn, r, total=1.5, n=3, duration=60, strategy="twap"):
    return ems.handle_request(conn, r, {"req_id": "r1", "kind": "create", "strategy": strategy,
                                        "account": "paper", "symbol": "BTC/USD", "side": "buy",
                                        "total_qty": total, "duration_s": duration, "n_slices": n,
                                        "limit_cap": None, "params": {}, "source": "test"})


def _slice_ts(conn, algo_id, idx):
    return ems._parse_ts(ems.get_slices(conn, algo_id)[idx]["scheduled_ts"])


def test_create(db_conn, fake_redis):
    rep = _create(db_conn, fake_redis)
    assert rep["ok"] and rep["status"] == "pending"
    aid = rep["algo_id"]
    a = ems.get_algo(db_conn, aid)
    assert a["status"] == "pending"
    sl = ems.get_slices(db_conn, aid)
    assert len(sl) == 3
    assert [s["client_order_id"] for s in sl] == [f"algo-{aid[:12]}-{i:03d}" for i in range(3)]
    assert all(s["status"] == "scheduled" for s in sl)
    assert fake_redis.llen("alpaca:reply:r1") == 1


def test_create_vwap_rejected(db_conn, fake_redis):
    rep = _create(db_conn, fake_redis, strategy="vwap")
    assert rep["ok"] is False
    assert "volume profile" in rep["error"]


def test_tick_submits_first_slice(db_conn, fake_redis, monkeypatch):
    cap = Capture()
    monkeypatch.setattr(ems.runtime, "send_command", cap)
    aid = _create(db_conn, fake_redis)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    subs = cap.submits()
    assert len(subs) == 1
    p = subs[0]
    assert p["type"] == "limit" and p["limit_price"] == pytest.approx(101.0)
    assert p["client_order_id"] == ems.coid_for(aid, 0)
    assert p["tif"] == "gtc" and p["qty"] == pytest.approx(0.5)
    assert ems.get_algo(db_conn, aid)["status"] == "running"
    s0 = ems.get_slices(db_conn, aid)[0]
    assert s0["status"] == "working" and s0["order_id"] == "oid-1"


def test_refresh_from_oms(db_conn, fake_redis, monkeypatch):
    cap = Capture()
    monkeypatch.setattr(ems.runtime, "send_command", cap)
    aid = _create(db_conn, fake_redis)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    oms.upsert_order(db_conn, {"id": "oid-1", "client_order_id": ems.coid_for(aid, 0),
                               "symbol": "BTC/USD", "side": "buy", "qty": "0.5",
                               "filled_qty": "0.5", "filled_avg_price": "100.5",
                               "status": "filled"}, account="paper")
    db_conn.commit()
    ems.refresh_from_oms(db_conn, aid)
    s0 = ems.get_slices(db_conn, aid)[0]
    assert s0["status"] == "filled" and s0["filled_qty"] == pytest.approx(0.5)
    a = ems.get_algo(db_conn, aid)
    assert a["filled_qty"] == pytest.approx(0.5)
    assert a["avg_px"] == pytest.approx(100.5)


def test_roll_forward_and_final_ioc(db_conn, fake_redis, monkeypatch):
    cap = Capture()
    monkeypatch.setattr(ems.runtime, "send_command", cap)
    aid = _create(db_conn, fake_redis, total=1.0, n=2, duration=40)["algo_id"]
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1)
    assert len(cap.submits()) == 1
    ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 1) + 1)
    cancels = cap.cancels()
    assert len(cancels) == 1 and cancels[0]["client_order_id"] == ems.coid_for(aid, 0)
    subs = cap.submits()
    assert len(subs) == 2
    final = subs[1]
    assert final["tif"] == "ioc"
    assert final["client_order_id"] == ems.coid_for(aid, 1)
    assert final["qty"] == pytest.approx(1.0)  # 0.5 own + 0.5 rolled
    assert 101.0 < final["limit_price"] <= 101.06
    sl = ems.get_slices(db_conn, aid)
    assert sl[0]["status"] == "rolled"
    assert sl[1]["qty"] == pytest.approx(1.0)


def test_cancel_request(db_conn, fake_redis):
    aid = _create(db_conn, fake_redis)["algo_id"]
    rep = ems.handle_request(db_conn, fake_redis, {"req_id": "r2", "kind": "cancel", "algo_id": aid})
    assert rep["ok"] and rep["status"] == "cancelled"
    assert ems.get_algo(db_conn, aid)["status"] == "cancelled"
    assert all(s["status"] == "cancelled" for s in ems.get_slices(db_conn, aid))


def test_cancel_unknown_algo(db_conn, fake_redis):
    rep = ems.handle_request(db_conn, fake_redis, {"req_id": "r3", "kind": "cancel", "algo_id": "zzz"})
    assert rep["ok"] is False and rep["status"] == "not_found"


def test_broken_algo_isolated(db_conn, fake_redis, monkeypatch):
    cap = Capture()
    monkeypatch.setattr(ems.runtime, "send_command", cap)
    good = _create(db_conn, fake_redis)["algo_id"]
    bad = _create(db_conn, fake_redis)["algo_id"]
    db_conn.execute("UPDATE algo_orders SET strategy='bogus' WHERE algo_id=?", (bad,))
    db_conn.commit()
    now = max(_slice_ts(db_conn, good, 0), _slice_ts(db_conn, bad, 0)) + 1
    n = ems.tick(db_conn, fake_redis, now_ts=now)
    assert n == 2
    assert ems.get_algo(db_conn, bad)["status"] == "failed"
    assert ems.get_algo(db_conn, good)["status"] == "running"
    assert [p["client_order_id"] for p in cap.submits()] == [ems.coid_for(good, 0)]


def test_quote_failure_does_not_crash(db_conn, fake_redis, monkeypatch):
    cap = Capture()
    monkeypatch.setattr(ems.runtime, "send_command", cap)

    def boom(*a, **k):
        raise RuntimeError("no quote")

    monkeypatch.setattr(ems.trader, "latest_quote", boom)
    aid = _create(db_conn, fake_redis)["algo_id"]
    assert ems.tick(db_conn, fake_redis, now_ts=_slice_ts(db_conn, aid, 0) + 1) == 1
    assert cap.submits() == []
    assert ems.get_algo(db_conn, aid)["status"] in ("running", "failed")
