import json
from types import SimpleNamespace

import pytest
from alpaca.common.exceptions import APIError

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


def _api_error(code, msg):
    return APIError(json.dumps({"code": code * 100000, "message": msg}),
                    SimpleNamespace(response=SimpleNamespace(status_code=code), request=None))


def _count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_trade_event_idempotent(db_conn):
    p = _payload()
    oms.handle_payload(db_conn, "1-0", p)
    oms.handle_payload(db_conn, "1-0", p)
    db_conn.commit()
    assert _count(db_conn, "trade_events") == 1
    assert _count(db_conn, "orders") == 1


def test_fill_idempotent(db_conn):
    p = _payload("partial_fill", exec_id="e1", status="partially_filled", filled="0.5")
    oms.handle_payload(db_conn, "1-0", p)
    oms.handle_payload(db_conn, "2-0", p)
    db_conn.commit()
    assert _count(db_conn, "fills") == 1
    row = db_conn.execute("SELECT * FROM fills").fetchone()
    assert row["qty"] == 0.5 and row["price"] == 100.0


def test_status_never_regresses(db_conn):
    oms.handle_payload(db_conn, "1-0", _payload("fill", exec_id="e1", status="filled", filled="1"))
    oms.handle_payload(db_conn, "2-0", _payload("new", status="new", filled="0"))
    db_conn.commit()
    row = oms.get_order_row(db_conn, order_id="o1")
    assert row["status"] == "filled"
    assert row["filled_qty"] == 1.0


def test_filled_qty_monotonic(db_conn):
    oms.upsert_order(db_conn, _order(status="partially_filled", filled="0.5"), account="paper")
    oms.upsert_order(db_conn, _order(status="partially_filled", filled="0.3"), account="paper")
    assert oms.get_order_row(db_conn, order_id="o1")["filled_qty"] == 0.5


def _submit_cmd(cmd_id, coid="tc-abc123"):
    return {"cmd_id": cmd_id, "kind": "submit", "account": "paper", "client_order_id": coid,
            "symbol": "BTC/USD", "side": "buy", "type": "limit", "qty": 0.01, "limit_price": 100,
            "tif": "gtc", "source": "cli"}


def test_submit_and_idempotent(db_conn, fake_redis, monkeypatch):
    calls = []

    def fake_submit(symbol, qty, side, limit_price, coid, live=False, tif="gtc"):
        calls.append(coid)
        return _order(oid="oid-1", coid=coid, status="accepted")

    monkeypatch.setattr(oms.trader, "submit_limit", fake_submit)
    rep = oms.handle_command(db_conn, fake_redis, _submit_cmd("c1"))
    assert rep["ok"] and rep["status"] == "acked" and rep["order_id"] == "oid-1"
    assert _count(db_conn, "orders") == 1
    cmd = db_conn.execute("SELECT * FROM commands WHERE cmd_id='c1'").fetchone()
    assert cmd["status"] == "acked"
    pushed = fake_redis.lrange(runtime.reply_key("c1"), 0, -1)
    assert len(pushed) == 1 and json.loads(pushed[0])["status"] == "acked"

    rep2 = oms.handle_command(db_conn, fake_redis, _submit_cmd("c2"))
    assert rep2["status"] == "already_submitted" and rep2["ok"]
    assert rep2["order_id"] == "oid-1"
    assert calls == ["tc-abc123"]


def test_submit_422_duplicate_lookup(db_conn, fake_redis, monkeypatch):
    def boom(*a, **k):
        raise _api_error(422, "client_order_id must be unique")

    monkeypatch.setattr(oms.trader, "submit_limit", boom)
    monkeypatch.setattr(oms.trader, "get_order_by_client_id",
                        lambda coid, live=False: _order(oid="oid-9", coid=coid, status="new"))
    rep = oms.handle_command(db_conn, fake_redis, _submit_cmd("c3", coid="tc-dup1"))
    assert rep["ok"] is True
    assert rep["status"] in ("acked", "already_submitted")
    assert rep["order_id"] == "oid-9"


def test_submit_transient_then_unknown(db_conn, fake_redis, monkeypatch):
    calls = []

    def boom(*a, **k):
        calls.append(1)
        raise RuntimeError("socket hiccup")

    monkeypatch.setattr(oms, "RETRY_DELAYS", (0.0, 0.0, 0.0))
    monkeypatch.setattr(oms.trader, "submit_limit", boom)
    monkeypatch.setattr(oms.trader, "is_transient", lambda e: True)
    monkeypatch.setattr(oms.trader, "get_order_by_client_id", lambda coid, live=False: None)
    rep = oms.handle_command(db_conn, fake_redis, _submit_cmd("c4", coid="tc-tr1"))
    assert rep["ok"] is False and rep["status"] == "unknown"
    assert len(calls) == 4
    assert db_conn.execute("SELECT status FROM commands WHERE cmd_id='c4'").fetchone()[0] == "unknown"


def test_submit_non_transient_rejected(db_conn, fake_redis, monkeypatch):
    def boom(*a, **k):
        raise _api_error(403, "insufficient balance")

    monkeypatch.setattr(oms.trader, "submit_limit", boom)
    rep = oms.handle_command(db_conn, fake_redis, _submit_cmd("c5", coid="tc-rej1"))
    assert rep["ok"] is False and rep["status"] == "rejected"


def test_cancel_unknown_not_found(db_conn, fake_redis, monkeypatch):
    monkeypatch.setattr(oms.trader, "get_order_by_client_id", lambda coid, live=False: None)
    rep = oms.handle_command(db_conn, fake_redis, {"cmd_id": "x1", "kind": "cancel",
                                                   "client_order_id": "tc-nope", "account": "paper"})
    assert rep["ok"] is False and rep["status"] == "not_found"


def test_cancel_404_not_found(db_conn, fake_redis, monkeypatch):
    def boom(*a, **k):
        raise _api_error(404, "order not found")
    monkeypatch.setattr(oms.trader, "cancel", boom)
    rep = oms.handle_command(db_conn, fake_redis, {"cmd_id": "x2", "kind": "cancel",
                                                   "order_id": "11111111-1111-1111-1111-111111111111"})
    assert rep["status"] == "not_found"


def test_cancel_ok(db_conn, fake_redis, monkeypatch):
    oms.upsert_order(db_conn, _order(oid="oid-c", coid="tc-c"), account="paper")
    seen = []
    monkeypatch.setattr(oms.trader, "cancel", lambda oid, live=False: seen.append(oid))
    rep = oms.handle_command(db_conn, fake_redis, {"cmd_id": "x3", "kind": "cancel",
                                                   "client_order_id": "tc-c"})
    assert rep["ok"] and rep["status"] == "cancel_requested" and seen == ["oid-c"]


@pytest.mark.parametrize("payload", [
    {"cmd_id": "b1", "kind": "bogus"},
    {"cmd_id": "b2", "kind": "submit", "side": "buy", "qty": 1},
    {"cmd_id": "b3", "kind": "submit", "symbol": "BTC/USD", "side": "buy", "qty": 1, "tif": "day"},
    {"cmd_id": "b4", "kind": "submit", "account": "margin", "symbol": "BTC/USD", "side": "buy", "qty": 1},
])
def test_invalid_payload(db_conn, fake_redis, payload):
    rep = oms.handle_command(db_conn, fake_redis, payload)
    assert rep["ok"] is False


def test_parse_ems_coid():
    assert oms.parse_ems_coid("algo-abcdef123456-002") == ("abcdef123456", 2)
    assert oms.parse_ems_coid("tc-xyz") == (None, None)
