import json

import pytest

import algo_engine
import persister
import runtime

RULES = {"symbol": "BTC/USD", "min_order_size": 0.0001, "min_trade_increment": 0.0001,
         "price_increment": 0.01, "tradable": True}
QUOTE = {"bid_price": 100.0, "ask_price": 101.0, "bid_size": 1.0, "ask_size": 1.0,
         "timestamp": None, "source": "test"}


@pytest.fixture
def capture(monkeypatch):
    """Capture every command the engine sends to OMS; reply configurable per kind."""
    sent = []
    replies = {"submit": None, "cancel": {"ok": True, "status": "cancel_requested"}}

    def fake_send(r, stream, payload, timeout=10.0):
        sent.append((stream, dict(payload)))
        kind = payload.get("kind")
        rep = replies.get(kind)
        if kind == "submit" and rep is None:
            oid = "oid-" + payload["client_order_id"]
            return {"ok": True, "status": "acked", "order_id": oid,
                    "client_order_id": payload["client_order_id"], "order": {"order_id": oid}}
        return rep or {"ok": False, "status": "error"}

    monkeypatch.setattr(algo_engine.runtime, "send_command", fake_send)
    monkeypatch.setattr(algo_engine.trader, "get_asset_rules", lambda sym, live=False, r=None: dict(RULES))
    monkeypatch.setattr(algo_engine.trader, "latest_quote", lambda sym, r=None: dict(QUOTE))
    return sent, replies


def _create(fake, **kw):
    p = {"cmd_id": runtime.new_id(), "kind": "create", "strategy": "twap", "account": "paper",
         "symbol": "BTC/USD", "side": "buy", "total_qty": 0.03, "duration_s": 30, "n_slices": 3,
         "limit_cap": None, "params": {}, "source": "test"}
    p.update(kw)
    rep = algo_engine.handle_request(fake, p)
    return rep


def _start(fake, aid):
    return float(fake.hget(runtime.algo_key(aid), "start_ts"))


def _slices(fake, aid):
    return {int(k): json.loads(v) for k, v in fake.hgetall(runtime.algo_slices_key(aid)).items()}


def _algo_events(fake, typ):
    out = [json.loads(f["json"]) for _, f in fake.xrange(runtime.STREAM_ALGO_EVENTS)]
    return [e for e in out if e["type"] == typ]


def _submits(sent):
    return [p for s, p in sent if p["kind"] == "submit"]


def test_create(fake_redis, capture):
    rep = _create(fake_redis)
    assert rep["ok"] is True
    aid = rep["algo_id"]
    h = fake_redis.hgetall(runtime.algo_key(aid))
    assert h["status"] == "pending" and h["symbol"] == "BTC/USD"
    sl = _slices(fake_redis, aid)
    assert sorted(sl) == [0, 1, 2]
    assert [sl[i]["client_order_id"] for i in range(3)] == [f"algo-{aid[:12]}-{i:03d}" for i in range(3)]
    assert fake_redis.sismember(runtime.SET_ACTIVE_ALGOS, aid)
    evs = _algo_events(fake_redis, "algo")
    assert len(evs) == 1 and len(evs[0]["slices"]) == 3


def test_create_vwap_rejected(fake_redis, capture):
    rep = _create(fake_redis, strategy="vwap")
    assert rep["ok"] is False and "not implemented" in rep["error"]


def test_tick_submits_first_slice(fake_redis, capture):
    sent, _ = capture
    aid = _create(fake_redis)["algo_id"]
    start = _start(fake_redis, aid)
    algo_engine.tick(fake_redis, now_ts=start + 0.5)
    subs = _submits(sent)
    assert len(subs) == 1
    assert sent[0][0] == runtime.STREAM_OMS_COMMANDS
    p = subs[0]
    assert p["source"] == "algo" and p["type"] == "limit" and p["tif"] == "gtc"
    assert p["limit_price"] == 101.0 and p["client_order_id"] == f"algo-{aid[:12]}-000"
    assert _slices(fake_redis, aid)[0]["status"] == "submitted"
    assert len(_algo_events(fake_redis, "slice")) >= 1
    assert fake_redis.hget(runtime.algo_key(aid), "status") == "running"


def test_final_slice_ioc_crossed(fake_redis, capture):
    sent, _ = capture
    aid = _create(fake_redis, n_slices=1, duration_s=10, total_qty=0.01)["algo_id"]
    algo_engine.tick(fake_redis, now_ts=_start(fake_redis, aid) + 0.5)
    subs = _submits(sent)
    assert len(subs) == 1
    assert subs[0]["tif"] == "ioc" and subs[0]["limit_price"] > 101.0


def test_fill_tracked(fake_redis, capture):
    aid = _create(fake_redis)["algo_id"]
    start = _start(fake_redis, aid)
    algo_engine.tick(fake_redis, now_ts=start + 0.5)
    coid = f"algo-{aid[:12]}-000"
    fake_redis.hset(runtime.order_key("o0"), mapping={"order_id": "o0", "status": "filled",
                                                      "filled_qty": "0.01", "filled_avg_price": "100.5"})
    fake_redis.set(runtime.coid_key(coid), "o0")
    algo_engine.tick(fake_redis, now_ts=start + 1.0)
    sl = _slices(fake_redis, aid)[0]
    assert sl["status"] == "filled" and sl["order_id"] == "o0"
    h = fake_redis.hgetall(runtime.algo_key(aid))
    assert float(h["filled_qty"]) == pytest.approx(0.01)
    assert float(h["avg_px"]) == pytest.approx(100.5)


def test_roll_forward(fake_redis, capture):
    sent, _ = capture
    aid = _create(fake_redis)["algo_id"]
    start = _start(fake_redis, aid)
    algo_engine.tick(fake_redis, now_ts=start + 0.5)
    coid = f"algo-{aid[:12]}-000"
    fake_redis.hset(runtime.order_key("o0"), mapping={"order_id": "o0", "status": "new",
                                                      "filled_qty": "0"})
    fake_redis.set(runtime.coid_key(coid), "o0")
    sent.clear()
    sched1 = _slices(fake_redis, aid)[1]["scheduled_ts"]
    algo_engine.tick(fake_redis, now_ts=float(sched1) + 0.5)
    cancels = [p for _, p in sent if p["kind"] == "cancel"]
    assert len(cancels) == 1 and cancels[0]["client_order_id"] == coid
    subs = _submits(sent)
    assert len(subs) == 1 and subs[0]["qty"] == pytest.approx(0.02)
    sl = _slices(fake_redis, aid)
    assert sl[0]["status"] == "rolled"
    assert sl[1]["qty"] == pytest.approx(0.02)


def test_tick_no_submit(fake_redis, capture):
    sent, _ = capture
    aid = _create(fake_redis)["algo_id"]
    algo_engine.tick(fake_redis, now_ts=_start(fake_redis, aid) + 0.5, allow_submit=False)
    assert _submits(sent) == []


def test_cancel_request(fake_redis, capture):
    aid = _create(fake_redis)["algo_id"]
    rep = algo_engine.handle_request(fake_redis, {"cmd_id": "cx", "kind": "cancel", "algo_id": aid})
    assert rep["ok"] is True and rep["status"] == "cancelled"
    assert fake_redis.hget(runtime.algo_key(aid), "status") == "cancelled"
    assert not fake_redis.sismember(runtime.SET_ACTIVE_ALGOS, aid)
    assert fake_redis.ttl(runtime.algo_key(aid)) > 0
    assert all(s["status"] == "cancelled" for s in _slices(fake_redis, aid).values())


def test_broken_algo_isolated(fake_redis, capture, monkeypatch):
    sent, _ = capture

    def quote(sym, r=None):
        if sym == "ETH/USD":
            raise RuntimeError("no quote")
        return dict(QUOTE)

    monkeypatch.setattr(algo_engine.trader, "latest_quote", quote)
    bad = _create(fake_redis, symbol="ETH/USD", n_slices=1, duration_s=10, total_qty=0.01)["algo_id"]
    good = _create(fake_redis, n_slices=3, duration_s=600)["algo_id"]
    now = max(float(fake_redis.hget(runtime.algo_key(bad), "end_ts")) + 10, _start(fake_redis, good) + 1)
    algo_engine.tick(fake_redis, now_ts=now)
    assert fake_redis.hget(runtime.algo_key(bad), "status") == "failed"
    assert fake_redis.hget(runtime.algo_key(good), "status") == "running"
    assert len(_submits(sent)) == 1


def test_reload_from_db(fake_redis, tmp_path):
    path = str(tmp_path / "reload.db")
    conn = runtime.sqlite_connect(path)
    persister.setup_db(conn)
    conn.execute("INSERT INTO algo_orders(algo_id, strategy, account, symbol, side, total_qty, n_slices, "
                 "start_ts, end_ts, status, filled_qty, params_json) "
                 "VALUES('aaaabbbbcccc1111','twap','paper','BTC/USD','buy',0.02,2,'1000','1100','running',0,'{}')")
    for i in range(2):
        conn.execute("INSERT INTO algo_slices(algo_id, slice_idx, scheduled_ts, qty, client_order_id, status) "
                     "VALUES(?,?,?,?,?,?)", ("aaaabbbbcccc1111", i, str(1000 + 50 * i), 0.01,
                                             f"algo-aaaabbbbcccc-{i:03d}", "scheduled"))
    conn.commit()
    conn.close()
    assert algo_engine.reload_from_db(fake_redis, path) == 1
    assert fake_redis.hget(runtime.algo_key("aaaabbbbcccc1111"), "status") == "running"
    assert fake_redis.sismember(runtime.SET_ACTIVE_ALGOS, "aaaabbbbcccc1111")
    assert len(_slices(fake_redis, "aaaabbbbcccc1111")) == 2
    # active set non-empty -> no reload
    assert algo_engine.reload_from_db(fake_redis, path) == 0
