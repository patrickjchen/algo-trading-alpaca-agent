"""OMS unit tests. OMS makes NO Alpaca order calls: it forwards to EMS, so we patch
``runtime.send_command`` (used by ``oms.forward_to_ems``), never ``trader.submit_*``."""
import json

import pytest

import oms
import runtime


def _order(oid="o1", coid="tc-aaa", status="new", filled="0"):
    return {"id": oid, "client_order_id": coid, "symbol": "BTC/USD", "side": "buy", "type": "limit",
            "time_in_force": "gtc", "qty": "1", "limit_price": "100", "filled_qty": filled,
            "filled_avg_price": None, "status": status}


def _payload(event="new", exec_id=None, **kw):
    o = _order(**kw)
    return {"event": event, "execution_id": exec_id, "order_id": o["id"],
            "client_order_id": o["client_order_id"], "symbol": "BTC/USD", "side": "buy",
            "price": 100.0 if exec_id else None, "qty": 0.5 if exec_id else None,
            "ts": "2026-01-01T00:00:00+00:00", "account": "paper", "order": o}


def _count(conn, table, where="", params=()):
    return conn.execute(f"SELECT COUNT(*) FROM {table} {where}", params).fetchone()[0]


def _one(conn, sql, params=()):
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row is not None else None


class FakeSend:
    def __init__(self, reply=None):
        self.calls = []
        self.reply = reply

    def __call__(self, r, stream, payload, timeout=10.0):
        self.calls.append((stream, dict(payload)))
        rep = self.reply(payload) if callable(self.reply) else dict(self.reply or {})
        rep.setdefault("cmd_id", payload.get("cmd_id"))
        return rep


def _acked(payload):
    coid = payload.get("client_order_id")
    return {"ok": True, "status": "acked", "order_id": "oid-1", "client_order_id": coid,
            "order": {"id": "oid-1", "client_order_id": coid, "symbol": "BTC/USD", "side": "buy",
                      "type": "limit", "time_in_force": "gtc", "qty": "0.01",
                      "limit_price": "100", "filled_qty": "0", "status": "accepted"}}


def _submit(cmd_id, coid="tc-abc123", source="cli"):
    return {"cmd_id": cmd_id, "kind": "submit", "account": "paper", "client_order_id": coid,
            "symbol": "BTC/USD", "side": "buy", "type": "limit", "qty": 0.01,
            "limit_price": 100.0, "tif": "gtc", "source": source}


@pytest.fixture
def ems_up(fake_redis):
    runtime.Heartbeat(fake_redis, "ems").beat()
    return fake_redis


# ----------------------------------------------------------------- trade updates
def test_trade_event_idempotent(db_conn):
    p = _payload()
    oms.handle_payload(db_conn, "1-0", p)
    oms.handle_payload(db_conn, "1-0", p)
    db_conn.commit()
    assert _count(db_conn, "trade_events") == 1
    assert _count(db_conn, "orders") == 1


def test_fill_idempotent_on_execution_id(db_conn):
    p = _payload("fill", exec_id="ex-1", status="filled", filled="1")
    oms.handle_payload(db_conn, "2-0", p)
    oms.handle_payload(db_conn, "3-0", p)  # replay under a different stream id
    db_conn.commit()
    assert _count(db_conn, "fills") == 1
    f = _one(db_conn, "SELECT * FROM fills")
    assert f["execution_id"] == "ex-1" and f["price"] == 100.0 and f["qty"] == 0.5
    assert _count(db_conn, "trade_events") == 2


def test_status_never_regresses(db_conn):
    oms.handle_payload(db_conn, "1-0", _payload("fill", exec_id="e1", status="filled", filled="1"))
    oms.handle_payload(db_conn, "2-0", _payload("new", status="new", filled="0"))
    db_conn.commit()
    o = _one(db_conn, "SELECT * FROM orders WHERE order_id='o1'")
    assert o["status"] == "filled"
    assert o["filled_qty"] == 1.0


def test_filled_qty_monotonic(db_conn):
    oms.handle_payload(db_conn, "1-0", _payload("partial_fill", exec_id="e1",
                                                status="partially_filled", filled="0.5"))
    oms.handle_payload(db_conn, "2-0", _payload("partial_fill", exec_id="e0",
                                                status="partially_filled", filled="0.2"))
    db_conn.commit()
    assert _one(db_conn, "SELECT filled_qty FROM orders")["filled_qty"] == 0.5


def test_ems_execution_report_upserts_order_without_fill(db_conn):
    o = _order(oid="oid-x", coid="algo-abcdef123456-002", status="accepted")
    p = {"event": "ems_submitted", "execution_id": None, "order_id": "oid-x",
         "client_order_id": o["client_order_id"], "symbol": "BTC/USD", "side": "buy",
         "price": None, "qty": 1.0, "ts": runtime.now_iso(), "account": "paper", "order": o,
         "source": "ems", "algo_id": "abcdef1234567890", "slice_idx": 2}
    oms.handle_payload(db_conn, "5-0", p)
    db_conn.commit()
    row = _one(db_conn, "SELECT * FROM orders WHERE order_id='oid-x'")
    assert row["source"] == "ems" and row["algo_id"] == "abcdef1234567890"
    assert row["slice_idx"] == 2 and row["status"] == "accepted"
    assert _count(db_conn, "fills") == 0


# ----------------------------------------------------------------- commands
def test_submit_forwards_to_ems(db_conn, ems_up, monkeypatch):
    fake = FakeSend(_acked)
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, _submit("c1", source="cli"))
    assert rep["ok"] is True and rep["status"] == "acked" and rep["order_id"] == "oid-1"
    assert len(fake.calls) == 1
    stream, fwd = fake.calls[0]
    assert stream == runtime.STREAM_EMS_REQUESTS
    assert fwd["kind"] == "submit" and fwd["client_order_id"] == "tc-abc123"
    o = _one(db_conn, "SELECT * FROM orders WHERE client_order_id='tc-abc123'")
    assert o is not None and o["source"] == "cli" and o["order_id"] == "oid-1"
    c = _one(db_conn, "SELECT * FROM commands WHERE cmd_id='c1'")
    assert c["status"] == "acked" and c["kind"] == "submit"
    raw = ems_up.lrange(runtime.reply_key("c1"), 0, -1)
    assert len(raw) == 1 and json.loads(raw[0])["status"] == "acked"


def test_submit_idempotent_on_client_order_id(db_conn, ems_up, monkeypatch):
    fake = FakeSend(_acked)
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    oms.handle_command(db_conn, ems_up, _submit("c1", source="webgui"))
    rep = oms.handle_command(db_conn, ems_up, _submit("c2", source="webgui"))
    assert rep["ok"] is True and rep["status"] == "already_submitted"
    assert rep["order_id"] == "oid-1"
    assert len(fake.calls) == 1
    # same cmd_id replay returns the stored outcome without forwarding
    rep3 = oms.handle_command(db_conn, ems_up, _submit("c1", source="webgui"))
    assert rep3["status"] == "acked" and len(fake.calls) == 1


def test_submit_ems_down(db_conn, fake_redis, monkeypatch):
    fake = FakeSend(_acked)
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, fake_redis, _submit("c3"))
    assert rep["ok"] is False and "EMS" in rep["error"]
    assert fake.calls == []
    assert _one(db_conn, "SELECT status FROM commands WHERE cmd_id='c3'")["status"] == "error"


def test_submit_rejected(db_conn, ems_up, monkeypatch):
    fake = FakeSend({"ok": False, "status": "rejected", "error": "APIError 403: insufficient"})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, _submit("c4"))
    assert rep["ok"] is False and rep["status"] == "rejected"
    assert _one(db_conn, "SELECT status FROM commands WHERE cmd_id='c4'")["status"] == "rejected"
    assert _count(db_conn, "orders") == 0


def test_submit_timeout_unknown(db_conn, ems_up, monkeypatch):
    fake = FakeSend({"ok": False, "status": "timeout", "error": "no reply"})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, _submit("c5"))
    assert rep["ok"] is False and rep["status"] == "unknown"
    assert _one(db_conn, "SELECT status FROM commands WHERE cmd_id='c5'")["status"] == "unknown"


def test_submit_invalid_payload(db_conn, ems_up, monkeypatch):
    fake = FakeSend(_acked)
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    bad = _submit("c6")
    bad["side"] = "hold"
    rep = oms.handle_command(db_conn, ems_up, bad)
    assert rep["ok"] is False and rep["status"] == "rejected"
    rep2 = oms.handle_command(db_conn, ems_up, {"cmd_id": "c7", "kind": "bogus"})
    assert rep2["ok"] is False
    bad3 = _submit("c8", coid="has spaces!")
    assert oms.handle_command(db_conn, ems_up, bad3)["ok"] is False
    assert fake.calls == []


def test_algo_create_forwarded(db_conn, ems_up, monkeypatch):
    fake = FakeSend({"ok": True, "status": "pending", "algo_id": "a" * 32,
                     "algo": {"algo_id": "a" * 32, "status": "pending"}})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, {
        "cmd_id": "ac1", "kind": "algo_create", "strategy": "twap", "account": "paper",
        "symbol": "BTC/USD", "side": "buy", "total_qty": 0.01, "duration_s": 60, "n_slices": 3,
        "limit_cap": None, "params": {}, "source": "test"})
    assert rep["ok"] is True and rep["algo_id"] == "a" * 32 and rep["status"] == "pending"
    stream, fwd = fake.calls[0]
    assert stream == runtime.STREAM_EMS_REQUESTS and fwd["kind"] == "create"
    assert fwd["strategy"] == "twap" and fwd["total_qty"] == 0.01
    c = _one(db_conn, "SELECT * FROM commands WHERE cmd_id='ac1'")
    assert c["kind"] == "algo_create" and c["algo_id"] == "a" * 32


def test_algo_cancel_forwarded(db_conn, ems_up, monkeypatch):
    fake = FakeSend({"ok": True, "status": "cancelled", "algo_id": "abc"})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, {"cmd_id": "ax1", "kind": "algo_cancel",
                                               "algo_id": "abc", "source": "cli"})
    assert rep["ok"] is True and rep["status"] == "cancelled"
    stream, fwd = fake.calls[0]
    assert stream == runtime.STREAM_EMS_REQUESTS
    assert fwd["kind"] == "cancel" and fwd["algo_id"] == "abc"


def test_cancel_unknown_not_found(db_conn, ems_up, monkeypatch):
    fake = FakeSend({"ok": True, "status": "cancel_requested"})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, {"cmd_id": "x1", "kind": "cancel",
                                               "client_order_id": "tc-nope", "source": "cli"})
    assert rep["ok"] is False and rep["status"] == "not_found"
    assert fake.calls == []


def test_cancel_known_forwarded(db_conn, ems_up, monkeypatch):
    oms.upsert_order(db_conn, _order(oid="oid-c", coid="tc-c"), account="paper")
    db_conn.commit()
    fake = FakeSend(lambda p: {"ok": True, "status": "cancel_requested",
                               "order_id": p.get("order_id")})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    rep = oms.handle_command(db_conn, ems_up, {"cmd_id": "x3", "kind": "cancel",
                                               "client_order_id": "tc-c", "source": "cli"})
    assert rep["ok"] and rep["status"] == "cancel_requested"
    stream, fwd = fake.calls[0]
    assert stream == runtime.STREAM_EMS_REQUESTS
    assert fwd["kind"] == "cancel_order" and fwd["order_id"] == "oid-c"
    assert _one(db_conn, "SELECT status FROM orders WHERE order_id='oid-c'")["status"] == \
        "pending_cancel"


def test_forward_to_ems_uses_requests_stream(fake_redis, monkeypatch):
    fake = FakeSend({"ok": True, "status": "acked"})
    monkeypatch.setattr(oms.runtime, "send_command", fake)
    oms.forward_to_ems(fake_redis, {"kind": "submit", "cmd_id": "orig"})
    stream, fwd = fake.calls[0]
    assert stream == runtime.STREAM_EMS_REQUESTS and fwd["cmd_id"] != "orig"
