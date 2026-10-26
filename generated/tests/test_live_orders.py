"""Live: order lifecycle OMS -> crossing -> EMS -> Alpaca PAPER, live Redis state and persister history.

Orders are non-marketable LIMIT BUYs 20% below the bid (``hold_s: 0`` so crossing routes them
immediately) and are always cancelled in ``finally``.
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
    ensure_worker("crossing")
    ensure_worker("persister")


def _submit_payload(spec: dict, coid: str) -> dict:
    return {"kind": "submit", "account": "paper", "client_order_id": coid, "symbol": spec["symbol"],
            "side": "buy", "type": "limit", "qty": spec["qty"], "limit_price": spec["limit_price"],
            "tif": "gtc", "source": "test", "hold_s": 0}


def _safety_cancel(r, coid):
    """Best-effort: make sure nothing is left resting (internally or on the paper account)."""
    if not coid:
        return
    try:
        if runtime.worker_alive(r, "oms"):
            rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS,
                                       {"kind": "cancel", "account": "paper", "client_order_id": coid,
                                        "source": "test"}, timeout=15)
            print("safety cancel reply:", rep.get("status"), rep.get("error"))
        import trader
        o = trader.get_order_by_client_id(coid)
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
    int_id = runtime.internal_order_id(coid)
    conn = db()
    try:
        payload = _submit_payload(spec, coid)
        reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=25)
        print("submit reply:", reply)
        assert reply.get("ok") is True and reply.get("status") == "acked", reply
        assert reply.get("venue") == "internal", reply
        assert reply.get("order_id") == "int-" + coid, reply
        assert reply.get("client_order_id") == coid
        assert float(reply.get("crossed_qty") or 0) == 0.0, reply
        xid = reply.get("routed_order_id")
        assert xid, f"no routed_order_id: {reply}"
        first_cmd_id = reply["cmd_id"]

        # live Redis state: coid -> exchange order once OMS has seen the EMS execution report
        assert wait_until(lambda: r.get(runtime.coid_key(coid)) == xid, 10), \
            f"coid key is {r.get(runtime.coid_key(coid))}, expected {xid}"
        assert not str(r.get(runtime.coid_key(coid))).startswith("int-")
        h = wait_until(lambda: r.hgetall(runtime.order_key(xid)), 10)
        assert h, "exchange order hash missing"
        print("exchange hash:", h)
        assert h.get("client_order_id") == coid
        assert h.get("status") in OPEN_STATES, h
        assert h.get("venue") == "alpaca", h
        assert r.sismember(runtime.SET_OPEN_ORDERS, xid)
        assert wait_until(lambda: r.hget(runtime.order_key(int_id), "status") == "replaced", 10), \
            f"internal status is {r.hget(runtime.order_key(int_id), 'status')}"

        import trader
        ex = trader.get_order_by_client_id(coid)
        assert ex is not None and str(ex.get("id")) == xid

        # persister history
        assert wait_until(lambda: conn.execute(
            "SELECT 1 FROM trade_events WHERE order_id=? AND event IN "
            "('ems_submitted','new','accepted','pending_new')", (xid,)).fetchone(), 20), \
            "no trade_events row for exchange order"
        for ev in ("internal_accepted", "internal_routed"):
            assert wait_until(lambda ev=ev: conn.execute(
                "SELECT 1 FROM trade_events WHERE order_id=? AND event=?", (int_id, ev)).fetchone(), 20), \
                f"no {ev} trade_event for {int_id}"
        for oid, venue in ((xid, "alpaca"), (int_id, "internal")):
            row = wait_until(lambda oid=oid: conn.execute(
                "SELECT * FROM orders WHERE order_id=?", (oid,)).fetchone(), 20)
            assert row is not None, f"orders row missing for {oid}"
            assert row["account"] == "paper"
            assert row["venue"] == venue, (oid, row["venue"])
            assert wait_until(lambda oid=oid: conn.execute(
                "SELECT source FROM orders WHERE order_id=?", (oid,)).fetchone()[0] == "test", 20), \
                f"orders.source for {oid} is not 'test'"
        crow = wait_until(lambda: conn.execute(
            "SELECT * FROM commands WHERE cmd_id=?", (first_cmd_id,)).fetchone(), 20)
        assert crow is not None, "commands row missing"
        assert crow["status"] == "acked"

        # duplicate client_order_id with a new cmd_id -> already_submitted
        dup = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, _submit_payload(spec, coid), timeout=20)
        print("duplicate reply:", dup)
        assert dup.get("status") == "already_submitted", dup

        # replay of the SAME cmd_id -> identical stored reply
        again = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS,
                                     dict(payload, cmd_id=first_cmd_id), timeout=20)
        assert json.loads(json.dumps(again, default=str)) == json.loads(json.dumps(reply, default=str))

        # cancel through OMS
        cr = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS,
                                  {"kind": "cancel", "account": "paper", "client_order_id": coid,
                                   "order_id": xid, "source": "test"}, timeout=25)
        print("cancel reply:", cr)
        assert cr.get("status") == "cancel_requested", cr

        assert wait_until(lambda: r.hget(runtime.order_key(xid), "status") == "canceled", 20), \
            f"hash status is {r.hget(runtime.order_key(xid), 'status')}"
        assert wait_until(lambda: not r.sismember(runtime.SET_OPEN_ORDERS, xid), 10)
        assert r.ttl(runtime.order_key(xid)) > 0

        assert wait_until(lambda: conn.execute(
            "SELECT status FROM orders WHERE order_id=?", (xid,)).fetchone()[0] == "canceled", 20), \
            "orders.status never became canceled"
        assert wait_until(lambda: conn.execute(
            "SELECT 1 FROM trade_events WHERE order_id=? AND event='canceled'", (xid,)).fetchone(), 20), \
            "no canceled trade_event"
    finally:
        _safety_cancel(r, coid)
        conn.close()


def test_unknown_symbol_rejected(live_redis):
    r = live_redis
    _require_workers()
    coid = runtime.new_client_order_id("tc")
    try:
        reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
            "kind": "submit", "account": "paper", "client_order_id": coid, "symbol": "NOPE/XYZ",
            "side": "buy", "type": "limit", "qty": 1.0, "limit_price": 1.0, "tif": "gtc",
            "source": "test", "hold_s": 0}, timeout=25)
        print("unknown symbol reply:", reply)
        assert reply.get("ok") is False
        assert reply.get("status") == "rejected", reply
        assert isinstance(reply.get("error"), str) and reply["error"]
    finally:
        _safety_cancel(r, coid)
