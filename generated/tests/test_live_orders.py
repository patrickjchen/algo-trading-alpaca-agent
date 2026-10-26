"""Live: order lifecycle through OMS -> EMS -> Alpaca PAPER, live Redis state and persister history.

Orders are non-marketable LIMIT BUYs 20% below the bid and are always cancelled in ``finally``.
"""
from __future__ import annotations

import json

import pytest

import runtime
from tests.conftest import (db, ensure_oms_ems, ensure_worker, paper_only, safe_limit_buy,
                            wait_until)

pytestmark = pytest.mark.live

OPEN_STATES = {"new", "accepted", "pending_new"}


def _require_workers():
    paper_only()
    ensure_worker("trade_stream")
    ensure_oms_ems()
    ensure_worker("persister")


def _submit_payload(spec: dict, coid: str) -> dict:
    return {"kind": "submit", "account": "paper", "client_order_id": coid, "symbol": spec["symbol"],
            "side": "buy", "type": "limit", "qty": spec["qty"], "limit_price": spec["limit_price"],
            "tif": "gtc", "source": "test"}


def _safety_cancel(r, order_id, coid):
    """Best-effort: make sure the order is not left resting on the paper account."""
    if not order_id and not coid:
        return
    try:
        h = r.hgetall(runtime.order_key(order_id)) if order_id else {}
        if h.get("status") in runtime.TERMINAL_STATUSES:
            return
        if runtime.worker_alive(r, "oms"):
            runtime.send_command(r, runtime.STREAM_OMS_COMMANDS,
                                 {"kind": "cancel", "account": "paper", "order_id": order_id,
                                  "client_order_id": coid, "source": "test"}, timeout=15)
        import trader
        o = trader.get_order_by_client_id(coid) if coid else None
        st = str((o or {}).get("status") or "").lower()
        if o and st not in runtime.TERMINAL_STATUSES:
            ok = wait_until(lambda: str((trader.get_order_by_client_id(coid) or {}).get("status") or "")
                            .lower() in runtime.TERMINAL_STATUSES, 10)
            if not ok:
                trader.cancel(o.get("id"), False)  # last-resort safety net
    except Exception as e:  # noqa: BLE001
        print("safety cancel failed:", e)


def test_order_lifecycle_via_oms(live_redis):
    r = live_redis
    _require_workers()
    spec = safe_limit_buy("BTC/USD")
    print("order spec:", {k: spec[k] for k in ("symbol", "qty", "limit_price")})
    coid = runtime.new_client_order_id("tc")
    order_id = None
    conn = db()
    try:
        payload = _submit_payload(spec, coid)
        reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=20)
        print("submit reply:", reply)
        assert reply.get("ok") is True and reply.get("status") == "acked", reply
        order_id = reply.get("order_id")
        assert order_id
        assert reply.get("client_order_id") == coid
        order = reply.get("order") or {}
        assert str(order.get("status")).lower() in OPEN_STATES, order
        first_cmd_id = reply["cmd_id"]

        # live Redis state is written synchronously by OMS before replying
        h = r.hgetall(runtime.order_key(order_id))
        assert h, "live order hash missing"
        assert h.get("client_order_id") == coid
        assert r.get(runtime.coid_key(coid)) == order_id
        assert r.sismember(runtime.SET_OPEN_ORDERS, order_id)

        import trader
        ex = trader.get_order_by_client_id(coid)
        assert ex is not None and str(ex.get("id")) == order_id

        # persister history
        assert wait_until(lambda: conn.execute(
            "SELECT 1 FROM trade_events WHERE order_id=? AND event IN "
            "('ems_submitted','new','accepted','pending_new')", (order_id,)).fetchone(), 20), \
            "no trade_events row for submitted order"
        row = wait_until(lambda: conn.execute(
            "SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone(), 20)
        assert row is not None, "orders row missing"
        assert row["account"] == "paper"
        assert wait_until(lambda: conn.execute(
            "SELECT source FROM orders WHERE order_id=?", (order_id,)).fetchone()[0] == "test", 20)
        crow = wait_until(lambda: conn.execute(
            "SELECT * FROM commands WHERE cmd_id=?", (first_cmd_id,)).fetchone(), 20)
        assert crow is not None, "commands row missing"
        assert crow["status"] == "acked"

        # duplicate client_order_id with a new cmd_id -> already_submitted
        dup = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, _submit_payload(spec, coid), timeout=20)
        print("duplicate reply:", dup)
        assert dup.get("status") == "already_submitted", dup
        assert dup.get("order_id") == order_id

        # replay of the SAME cmd_id -> identical stored reply
        again = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS,
                                     dict(payload, cmd_id=first_cmd_id), timeout=20)
        assert json.loads(json.dumps(again, default=str)) == json.loads(json.dumps(reply, default=str))

        # cancel through OMS
        cr = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS,
                                  {"kind": "cancel", "account": "paper", "order_id": order_id,
                                   "source": "test"}, timeout=20)
        print("cancel reply:", cr)
        assert cr.get("status") == "cancel_requested", cr

        assert wait_until(lambda: r.hget(runtime.order_key(order_id), "status") == "canceled", 20), \
            f"hash status is {r.hget(runtime.order_key(order_id), 'status')}"
        assert wait_until(lambda: not r.sismember(runtime.SET_OPEN_ORDERS, order_id), 10)
        assert r.ttl(runtime.order_key(order_id)) > 0

        assert wait_until(lambda: conn.execute(
            "SELECT status FROM orders WHERE order_id=?", (order_id,)).fetchone()[0] == "canceled", 20), \
            "orders.status never became canceled"
        assert wait_until(lambda: conn.execute(
            "SELECT 1 FROM trade_events WHERE order_id=? AND event='canceled'", (order_id,)).fetchone(), 20), \
            "no canceled trade_event"
    finally:
        _safety_cancel(r, order_id, coid)
        conn.close()


def test_unknown_symbol_rejected(live_redis):
    r = live_redis
    _require_workers()
    coid = runtime.new_client_order_id("tc")
    order_id = None
    try:
        reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
            "kind": "submit", "account": "paper", "client_order_id": coid, "symbol": "NOPE/XYZ",
            "side": "buy", "type": "limit", "qty": 1.0, "limit_price": 1.0, "tif": "gtc",
            "source": "test"}, timeout=20)
        print("unknown symbol reply:", reply)
        order_id = reply.get("order_id")
        assert reply.get("ok") is False
        assert reply.get("status") == "rejected", reply
        assert isinstance(reply.get("error"), str) and reply["error"]
    finally:
        if order_id:
            _safety_cancel(r, order_id, coid)
