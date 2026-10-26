"""OMS: Redis-only live state; forwards submits/cancels to crossing and algos to the algo engine."""
import json

import pytest

import oms
import runtime


def _order(oid, coid, status, filled="0", **extra):
    d = {"id": oid, "client_order_id": coid, "symbol": "BTC/USD", "side": "buy", "type": "limit",
         "time_in_force": "gtc", "qty": "0.01", "limit_price": "100", "filled_qty": filled,
         "status": status}
    d.update(extra)
    return d


def _events(fake, typ):
    out = [json.loads(f["json"]) for _, f in fake.xrange(runtime.STREAM_OMS_EVENTS)]
    return [e for e in out if e["type"] == typ]


@pytest.fixture(autouse=True)
def _no_exchange(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("OMS must never call trader.submit_* / cancel")
    for name in ("submit_limit", "submit_market", "cancel", "cancel_all"):
        monkeypatch.setattr(oms.trader, name, boom)


@pytest.fixture
def cross(monkeypatch, fake_redis):
    runtime.Heartbeat(fake_redis, "crossing").beat()
    calls = []
    state = {"reply": None}

    def fwd(r, payload, timeout=10.0):
        calls.append(dict(payload))
        if state["reply"] is not None:
            return state["reply"]
        coid = payload.get("client_order_id")
        if payload.get("kind") == "cancel_order":
            return {"ok": True, "status": "cancel_requested", "order_id": payload.get("order_id"),
                    "client_order_id": coid}
        oid = runtime.internal_order_id(coid)
        return {"ok": True, "status": "acked", "order_id": oid, "client_order_id": coid,
                "order": {"id": oid, "client_order_id": coid, "symbol": "BTC/USD", "side": "buy",
                          "type": "limit", "time_in_force": "gtc", "qty": 0.01, "limit_price": 100.0,
                          "filled_qty": 0.0, "status": "accepted", "venue": "internal", "book": "A"},
                "venue": "internal", "crossed_qty": 0.0, "remaining": payload.get("qty")}

    monkeypatch.setattr(oms, "forward_to_cross", fwd)
    return calls, state


def _submit(fake, cmd_id="c1", coid="tc-abc123", **kw):
    p = {"cmd_id": cmd_id, "kind": "submit", "account": "paper", "client_order_id": coid,
         "symbol": "BTC/USD", "side": "buy", "type": "limit", "qty": 0.01, "limit_price": 100.0,
         "tif": "gtc", "source": "test", "book": "A"}
    p.update(kw)
    return oms.handle_command(fake, p)


def test_module_has_no_sqlite():
    assert not hasattr(oms, "sqlite3")


def test_handle_payload_new_then_filled(fake_redis):
    oms.handle_payload(fake_redis, "1-0", {"event": "new", "account": "paper",
                                           "order": _order("o1", "tc-1", "new")})
    h = fake_redis.hgetall(runtime.order_key("o1"))
    assert h["status"] == "new" and float(h["qty"]) == 0.01
    assert fake_redis.get(runtime.coid_key("tc-1")) == "o1"
    assert fake_redis.sismember(runtime.SET_OPEN_ORDERS, "o1")
    oms.handle_payload(fake_redis, "2-0", {"event": "fill", "account": "paper",
                                           "order": _order("o1", "tc-1", "filled", "0.01")})
    h = fake_redis.hgetall(runtime.order_key("o1"))
    assert h["status"] == "filled" and float(h["filled_qty"]) == 0.01
    assert not fake_redis.sismember(runtime.SET_OPEN_ORDERS, "o1")
    assert fake_redis.ttl(runtime.order_key("o1")) > 0


def test_stale_new_after_filled(fake_redis):
    oms.handle_payload(fake_redis, "1-0", {"account": "paper", "order": _order("o1", "tc-1", "filled", "0.01")})
    oms.handle_payload(fake_redis, "2-0", {"account": "paper", "order": _order("o1", "tc-1", "new", "0")})
    h = fake_redis.hgetall(runtime.order_key("o1"))
    assert h["status"] == "filled" and float(h["filled_qty"]) == 0.01


def test_ems_report_algo_fields(fake_redis):
    oms.handle_payload(fake_redis, "1-0", {"event": "ems_submitted", "account": "paper", "source": "algo",
                                           "algo_id": "abcabcabcabc", "slice_idx": 2,
                                           "order": _order("o9", "x-9", "accepted")})
    h = fake_redis.hgetall(runtime.order_key("o9"))
    assert h["source"] == "algo" and h["algo_id"] == "abcabcabcabc" and h["slice_idx"] == "2"


def test_payload_without_order_ignored(fake_redis):
    assert oms.handle_payload(fake_redis, "1-0", {"event": "new"}) is None


def test_submit_forwarded_to_crossing(fake_redis, cross):
    calls, _ = cross
    rep = _submit(fake_redis)
    assert rep["ok"] is True and rep["status"] == "acked"
    assert len(calls) == 1 and calls[0]["kind"] == "submit"
    assert calls[0]["client_order_id"] == "tc-abc123" and calls[0]["book"] == "A"
    oid = runtime.internal_order_id("tc-abc123")
    h = fake_redis.hgetall(runtime.order_key(oid))
    assert h["venue"] == "internal" and h["book"] == "A"
    assert fake_redis.get(runtime.coid_key("tc-abc123")) == oid
    orders, cmds = _events(fake_redis, "order"), _events(fake_redis, "command")
    assert len(orders) == 1 and len(cmds) == 1
    assert cmds[0]["command"]["status"] == "acked" and cmds[0]["command"]["source"] == "test"
    pushed = fake_redis.lrange(runtime.reply_key("c1"), 0, -1)
    assert pushed and json.loads(pushed[0])["status"] == "acked"
    assert json.loads(fake_redis.get(runtime.cmd_key("c1")))["status"] == "acked"


def test_replay_same_cmd_id(fake_redis, cross):
    calls, _ = cross
    rep1 = _submit(fake_redis)
    rep2 = _submit(fake_redis)
    assert rep2 == json.loads(json.dumps(rep1, default=str))
    assert len(calls) == 1


def test_duplicate_coid_already_submitted(fake_redis, cross):
    calls, _ = cross
    _submit(fake_redis)
    rep = _submit(fake_redis, cmd_id="c2")
    assert rep["status"] == "already_submitted" and rep["ok"] is True
    assert len(calls) == 1


def test_crossing_down(fake_redis, monkeypatch):
    calls = []
    monkeypatch.setattr(oms, "forward_to_cross", lambda *a, **k: calls.append(a) or {})
    rep = _submit(fake_redis)
    assert rep["ok"] is False and "crossing" in rep["error"]
    assert calls == []
    assert _events(fake_redis, "command")[0]["command"]["status"] == "error"


def test_exchange_report_repoints_coid(fake_redis, cross):
    _submit(fake_redis)
    coid = "tc-abc123"
    oms.handle_payload(fake_redis, "5-0", {"event": "ems_submitted", "account": "paper",
                                           "order": _order("x-ex1", coid, "accepted")})
    assert fake_redis.get(runtime.coid_key(coid)) == "x-ex1"
    assert fake_redis.exists(runtime.order_key(runtime.internal_order_id(coid)))
    oms.handle_payload(fake_redis, "6-0", {"event": "internal_routed", "account": "paper", "venue": "internal",
                                           "order": {"id": runtime.internal_order_id(coid),
                                                     "client_order_id": coid, "status": "replaced",
                                                     "venue": "internal", "routed_order_id": "x-ex1"}})
    assert fake_redis.get(runtime.coid_key(coid)) == "x-ex1"
    ih = fake_redis.hgetall(runtime.order_key(runtime.internal_order_id(coid)))
    assert ih["status"] == "replaced" and ih["routed_order_id"] == "x-ex1"


def test_forward_rejected_no_hash(fake_redis, cross):
    _, state = cross
    state["reply"] = {"ok": False, "status": "rejected", "error": "bad"}
    rep = _submit(fake_redis)
    assert rep["ok"] is False and rep["status"] == "rejected"
    assert not fake_redis.exists(runtime.order_key(runtime.internal_order_id("tc-abc123")))
    assert fake_redis.get(runtime.coid_key("tc-abc123")) is None


def test_forward_timeout_unknown(fake_redis, cross):
    _, state = cross
    state["reply"] = {"ok": False, "status": "timeout", "error": "no reply"}
    rep = _submit(fake_redis)
    assert rep["ok"] is False and rep["status"] == "unknown"


def test_algo_create_forwarded(fake_redis, monkeypatch):
    runtime.Heartbeat(fake_redis, "algo_engine").beat()
    calls = []

    def fwd(r, payload, timeout=10.0):
        calls.append(dict(payload))
        return {"ok": True, "status": "pending", "algo_id": "a1", "algo": {"algo_id": "a1"}}

    monkeypatch.setattr(oms, "forward_to_algo", fwd)
    rep = oms.handle_command(fake_redis, {"cmd_id": "ac1", "kind": "algo_create", "strategy": "twap",
                                          "account": "paper", "symbol": "BTC/USD", "side": "buy",
                                          "total_qty": 0.01, "duration_s": 60, "n_slices": 3,
                                          "limit_cap": None, "params": {}, "source": "test"})
    assert rep["ok"] is True and rep["algo_id"] == "a1"
    assert len(calls) == 1 and calls[0]["kind"] == "create"


def test_algo_engine_down(fake_redis, monkeypatch):
    calls = []
    monkeypatch.setattr(oms, "forward_to_algo", lambda *a, **k: calls.append(a) or {})
    rep = oms.handle_command(fake_redis, {"cmd_id": "ac2", "kind": "algo_create", "strategy": "twap",
                                          "symbol": "BTC/USD", "side": "buy", "total_qty": 0.01})
    assert rep["ok"] is False and "algo_engine" in rep["error"]
    rep = oms.handle_command(fake_redis, {"cmd_id": "ac3", "kind": "algo_cancel", "algo_id": "a1"})
    assert rep["ok"] is False and "algo_engine" in rep["error"]
    assert calls == []


def test_cancel_without_ids_not_found(fake_redis, cross):
    calls, _ = cross
    rep = oms.handle_command(fake_redis, {"cmd_id": "k1", "kind": "cancel", "source": "test"})
    assert rep["status"] == "not_found" and rep["ok"] is False
    assert calls == []


def test_cancel_unknown_id_relays_not_found(fake_redis, cross):
    calls, state = cross
    state["reply"] = {"ok": False, "status": "not_found", "error": "order not found"}
    rep = oms.handle_command(fake_redis, {"cmd_id": "k2", "kind": "cancel", "order_id": "nope",
                                          "source": "test"})
    assert rep["status"] == "not_found"
    assert calls[0]["kind"] == "cancel_order"


def test_cancel_known_coid(fake_redis, cross):
    calls, _ = cross
    oms.handle_payload(fake_redis, "1-0", {"account": "paper", "order": _order("x1", "tc-k", "new")})
    rep = oms.handle_command(fake_redis, {"cmd_id": "k3", "kind": "cancel", "client_order_id": "tc-k",
                                          "source": "test"})
    assert rep["ok"] is True and rep["status"] == "cancel_requested"
    assert len(calls) == 1
    assert calls[0]["kind"] == "cancel_order"
    assert calls[0]["order_id"] == "x1" and calls[0]["client_order_id"] == "tc-k"
    assert fake_redis.hget(runtime.order_key("x1"), "status") == "pending_cancel"


@pytest.mark.parametrize("bad", [
    {"side": "hold"}, {"qty": 0}, {"tif": "day"}, {"client_order_id": "bad id!"},
    {"type": "limit", "limit_price": None}, {"account": "margin"},
])
def test_invalid_submit(fake_redis, cross, bad):
    calls, _ = cross
    rep = _submit(fake_redis, cmd_id="bad-" + str(len(str(bad))) + list(bad)[0], **bad)
    assert rep["ok"] is False
    assert calls == []


def test_unknown_kind(fake_redis):
    rep = oms.handle_command(fake_redis, {"cmd_id": "u1", "kind": "explode"})
    assert rep["ok"] is False and rep["status"] == "error"
    assert oms.handle_command(fake_redis, "garbage")["ok"] is False
