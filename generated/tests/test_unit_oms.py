import json

import pytest

import oms
import runtime


def _order(oid="o1", coid="tc-abc", status="new", filled="0", **kw):
    d = {"id": oid, "client_order_id": coid, "symbol": "BTC/USD", "side": "buy", "type": "limit",
         "time_in_force": "gtc", "qty": "0.01", "limit_price": "100", "filled_qty": filled,
         "filled_avg_price": None, "status": status}
    d.update(kw)
    return d


def _events(fake, typ=None):
    out = [json.loads(f["json"]) for _, f in fake.xrange(runtime.STREAM_OMS_EVENTS)]
    return [e for e in out if typ is None or e.get("type") == typ]


@pytest.fixture
def ems_fwd(monkeypatch, fake_redis):
    runtime.Heartbeat(fake_redis, "ems").beat()
    calls = []
    state = {"reply": None}

    def fwd(r, payload, timeout=10.0):
        calls.append(dict(payload))
        if state["reply"] is not None:
            return state["reply"]
        return {"ok": True, "status": "acked", "order_id": "o1",
                "client_order_id": payload.get("client_order_id"),
                "order": _order("o1", payload.get("client_order_id"), "accepted")}

    monkeypatch.setattr(oms, "forward_to_ems", fwd)
    return calls, state


@pytest.fixture
def algo_fwd(monkeypatch):
    calls = []

    def fwd(r, payload, timeout=10.0):
        calls.append(dict(payload))
        return {"ok": True, "status": "pending", "algo_id": "algo123", "algo": {"algo_id": "algo123"}}

    monkeypatch.setattr(oms, "forward_to_algo", fwd)
    return calls


def _submit(cmd_id="c1", coid="tc-abc", **kw):
    p = {"cmd_id": cmd_id, "kind": "submit", "account": "paper", "client_order_id": coid,
         "symbol": "BTC/USD", "side": "buy", "type": "limit", "qty": 0.01, "limit_price": 100.0,
         "tif": "gtc", "source": "webgui"}
    p.update(kw)
    return p


@pytest.fixture(autouse=True)
def _no_exchange_calls(monkeypatch):
    """OMS must never call Alpaca order endpoints: any trader.submit_*/cancel call fails the test."""
    def boom(*a, **k):
        raise AssertionError("OMS must not call trader order endpoints")

    for name in ("submit_limit", "submit_market", "cancel", "cancel_all"):
        monkeypatch.setattr(oms.trader, name, boom)


def test_no_sqlite():
    assert not hasattr(oms, "sqlite3")


def test_handle_payload_new_then_filled(fake_redis):
    oms.handle_payload(fake_redis, "1-0", {"event": "new", "account": "paper", "order": _order()})
    key = runtime.order_key("o1")
    h = fake_redis.hgetall(key)
    assert h["status"] == "new" and float(h["qty"]) == 0.01
    assert fake_redis.get(runtime.coid_key("tc-abc")) == "o1"
    assert fake_redis.sismember(runtime.SET_OPEN_ORDERS, "o1")

    oms.handle_payload(fake_redis, "2-0", {"event": "fill", "account": "paper",
                                           "order": _order(status="filled", filled="0.01",
                                                           filled_avg_price="100")})
    h = fake_redis.hgetall(key)
    assert h["status"] == "filled" and float(h["filled_qty"]) == 0.01
    assert not fake_redis.sismember(runtime.SET_OPEN_ORDERS, "o1")
    assert fake_redis.ttl(key) > 0

    oms.handle_payload(fake_redis, "3-0", {"event": "new", "account": "paper", "order": _order()})
    h = fake_redis.hgetall(key)
    assert h["status"] == "filled" and float(h["filled_qty"]) == 0.01


def test_ems_report_algo_fields(fake_redis):
    oms.handle_payload(fake_redis, "1-0", {"event": "ems_submitted", "account": "paper",
                                           "source": "algo", "algo_id": "abcdef", "slice_idx": 2,
                                           "order": _order(coid="x-1", status="accepted")})
    h = fake_redis.hgetall(runtime.order_key("o1"))
    assert h["source"] == "algo" and h["algo_id"] == "abcdef" and h["slice_idx"] == "2"


def test_submit_forwarded(fake_redis, ems_fwd):
    calls, _ = ems_fwd
    rep = oms.handle_command(fake_redis, _submit())
    assert rep["ok"] is True and rep["status"] == "acked" and rep["order_id"] == "o1"
    assert len(calls) == 1 and calls[0]["kind"] == "submit"
    assert calls[0]["client_order_id"] == "tc-abc"
    assert fake_redis.hgetall(runtime.order_key("o1"))
    assert fake_redis.get(runtime.coid_key("tc-abc")) == "o1"
    orders = _events(fake_redis, "order")
    cmds = _events(fake_redis, "command")
    assert len(orders) == 1 and len(cmds) == 1
    assert cmds[0]["command"]["status"] == "acked" and cmds[0]["command"]["source"] == "webgui"
    pushed = json.loads(fake_redis.lindex(runtime.reply_key("c1"), 0))
    assert pushed["status"] == "acked"
    assert json.loads(fake_redis.get(runtime.cmd_key("c1")))["status"] == "acked"

    # same cmd_id -> replay
    rep2 = oms.handle_command(fake_redis, _submit())
    assert rep2 == rep and len(calls) == 1

    # same coid, new cmd_id -> already_submitted
    rep3 = oms.handle_command(fake_redis, _submit(cmd_id="c2"))
    assert rep3["status"] == "already_submitted" and len(calls) == 1


def test_submit_ems_down(fake_redis, monkeypatch):
    calls = []
    monkeypatch.setattr(oms, "forward_to_ems", lambda *a, **k: calls.append(a) or {})
    rep = oms.handle_command(fake_redis, _submit())
    assert rep["ok"] is False and "EMS" in rep["error"]
    assert calls == []
    cmds = _events(fake_redis, "command")
    assert cmds[0]["command"]["status"] == "error"


def test_submit_rejected_no_hash(fake_redis, ems_fwd):
    _, state = ems_fwd
    state["reply"] = {"ok": False, "status": "rejected", "error": "bad"}
    rep = oms.handle_command(fake_redis, _submit())
    assert rep["status"] == "rejected" and rep["ok"] is False
    assert not fake_redis.exists(runtime.order_key("o1"))
    assert fake_redis.get(runtime.coid_key("tc-abc")) is None


def test_submit_timeout_unknown(fake_redis, ems_fwd):
    _, state = ems_fwd
    state["reply"] = {"ok": False, "status": "timeout"}
    rep = oms.handle_command(fake_redis, _submit())
    assert rep["status"] == "unknown" and rep["ok"] is False


def test_algo_create(fake_redis, algo_fwd):
    runtime.Heartbeat(fake_redis, "algo_engine").beat()
    rep = oms.handle_command(fake_redis, {"cmd_id": "a1", "kind": "algo_create", "strategy": "twap",
                                          "account": "paper", "symbol": "BTC/USD", "side": "buy",
                                          "total_qty": 0.01, "duration_s": 60, "n_slices": 3,
                                          "source": "test"})
    assert rep["ok"] is True and rep["algo_id"] == "algo123"
    assert algo_fwd[0]["kind"] == "create"


def test_algo_create_engine_down(fake_redis, algo_fwd):
    rep = oms.handle_command(fake_redis, {"cmd_id": "a2", "kind": "algo_create", "strategy": "twap",
                                          "symbol": "BTC/USD", "side": "buy", "total_qty": 0.01})
    assert rep["ok"] is False and "algo_engine" in rep["error"]
    assert algo_fwd == []


def test_cancel_unknown(fake_redis, ems_fwd):
    calls, _ = ems_fwd
    rep = oms.handle_command(fake_redis, {"cmd_id": "x1", "kind": "cancel", "client_order_id": "tc-nope",
                                          "source": "test"})
    assert rep["status"] == "not_found" and calls == []


def test_cancel_known_coid(fake_redis, ems_fwd):
    calls, state = ems_fwd
    oms.handle_payload(fake_redis, "1-0", {"event": "new", "account": "paper", "order": _order()})
    state["reply"] = {"ok": True, "status": "cancel_requested", "order_id": "o1"}
    rep = oms.handle_command(fake_redis, {"cmd_id": "x2", "kind": "cancel", "client_order_id": "tc-abc",
                                          "source": "test"})
    assert rep["ok"] is True and rep["status"] == "cancel_requested"
    assert calls[0]["kind"] == "cancel_order" and calls[0]["order_id"] == "o1"
    assert fake_redis.hget(runtime.order_key("o1"), "status") == "pending_cancel"


def test_invalid_payload(fake_redis, ems_fwd):
    calls, _ = ems_fwd
    rep = oms.handle_command(fake_redis, _submit(side="hold"))
    assert rep["ok"] is False and calls == []
    rep = oms.handle_command(fake_redis, {"cmd_id": "z", "kind": "bogus"})
    assert rep["ok"] is False
