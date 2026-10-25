"""Live: submit / duplicate / cancel a non-marketable paper limit BUY through the OMS."""
from __future__ import annotations

import pytest

import runtime
from tests.conftest import db, ensure_worker, safe_limit_buy, wait_until

pytestmark = pytest.mark.live


def _row(sql, args):
    conn = db()
    try:
        row = conn.execute(sql, args).fetchone()
        return {k: row[k] for k in row.keys()} if row is not None else None
    finally:
        conn.close()


def _cleanup(r, order_id, cid):
    """Best-effort cancel: via OMS first, direct paper REST as a safety fallback."""
    import trader
    try:
        if runtime.worker_alive(r, "oms"):
            rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
                "kind": "cancel", "account": "paper", "order_id": order_id,
                "client_order_id": cid, "source": "test"}, timeout=10)
            if rep.get("ok") or rep.get("status") == "not_found":
                return
    except Exception:  # noqa: BLE001
        pass
    try:
        o = trader.get_order_by_client_id(cid, live=False)
        if o and str(o.get("status")) not in runtime.TERMINAL_STATUSES:
            trader.cancel(o["id"], live=False)
    except Exception:  # noqa: BLE001
        pass


def test_submit_duplicate_cancel(live_redis):
    import trader
    r = live_redis
    ensure_worker("trade_stream")
    ensure_worker("oms")

    o = safe_limit_buy("BTC/USD")
    print("placing", o["qty"], "@", o["limit_price"], "bid", o["quote"]["bid_price"])
    cid = runtime.new_client_order_id("tc")
    submit = {"kind": "submit", "account": "paper", "client_order_id": cid, "symbol": "BTC/USD",
              "side": "buy", "type": "limit", "qty": o["qty"], "limit_price": o["limit_price"],
              "tif": "gtc", "source": "test", "algo_id": None, "slice_idx": None}
    order_id = None
    submit_cmd = runtime.new_id()
    try:
        rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {**submit, "cmd_id": submit_cmd},
                                   timeout=20)
        print("submit reply:", rep)
        assert rep.get("status") == "acked", rep
        assert rep.get("ok") is True
        order_id = rep.get("order_id")
        assert order_id
        status = (rep.get("order") or {}).get("status")
        assert status in {"new", "accepted", "pending_new"}, status

        broker = trader.get_order_by_client_id(cid, live=False)
        assert broker is not None
        assert str(broker["id"]) == order_id
        assert broker["client_order_id"] == cid

        ev = wait_until(lambda: _row(
            "SELECT * FROM trade_events WHERE order_id=? AND event IN ('new','accepted','pending_new')",
            (order_id,)), 20)
        assert ev, "no new/accepted trade_event for the order"
        orow = wait_until(lambda: _row("SELECT * FROM orders WHERE order_id=?", (order_id,)), 20)
        assert orow and orow["account"] == "paper"

        dup = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, dict(submit), timeout=20)
        assert dup.get("status") == "already_submitted", dup
        assert dup.get("order_id") == order_id

        cancel_cmd = runtime.new_id()
        crep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
            "cmd_id": cancel_cmd, "kind": "cancel", "account": "paper", "order_id": order_id,
            "source": "test"}, timeout=20)
        assert crep.get("status") == "cancel_requested", crep

        assert wait_until(lambda: (_row("SELECT status FROM orders WHERE order_id=?", (order_id,))
                                   or {}).get("status") == "canceled", 20), "order not canceled in DB"
        assert wait_until(lambda: _row(
            "SELECT 1 AS x FROM trade_events WHERE order_id=? AND event='canceled'", (order_id,)), 20)

        s_row = _row("SELECT * FROM commands WHERE cmd_id=?", (submit_cmd,))
        c_row = _row("SELECT * FROM commands WHERE cmd_id=?", (cancel_cmd,))
        assert s_row and s_row["kind"] == "submit" and s_row["client_order_id"] == cid
        assert c_row and c_row["kind"] == "cancel"
    finally:
        _cleanup(r, order_id, cid)


def test_submit_unknown_symbol_rejected(live_redis):
    r = live_redis
    ensure_worker("oms")
    cid = runtime.new_client_order_id("tc")
    rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
        "kind": "submit", "account": "paper", "client_order_id": cid, "symbol": "NOPE/XYZ",
        "side": "buy", "type": "limit", "qty": 1, "limit_price": 1, "tif": "gtc",
        "source": "test"}, timeout=20)
    try:
        print("reply:", rep)
        assert rep.get("status") == "rejected", rep
        assert rep.get("ok") is False
        assert isinstance(rep.get("error"), str) and rep["error"]
    finally:
        if rep.get("order_id"):
            _cleanup(r, rep["order_id"], cid)
