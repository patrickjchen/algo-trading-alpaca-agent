"""Live: persister catches up on trade_updates written while it was down (consumer-group replay).

Only runs when the persister process was started by this test session (never kills the user's).
OMS consumes the same stream with its own group, so (if alive) it also builds live hashes.
"""
from __future__ import annotations

import uuid

import pytest

import runtime
from tests.conftest import (db, ensure_worker, paper_only, started_by_tests, stop_worker,
                            wait_until)

pytestmark = pytest.mark.live


def _fake_payload(order_id: str) -> dict:
    now = runtime.now_iso()
    coid = f"test-{uuid.uuid4().hex[:12]}"
    order = {"id": order_id, "client_order_id": coid, "symbol": "BTC/USD", "side": "buy",
             "type": "limit", "time_in_force": "gtc", "qty": "0.0001", "limit_price": "1",
             "filled_qty": "0", "filled_avg_price": None, "status": "canceled",
             "submitted_at": now, "created_at": now, "updated_at": now}
    return {"event": "canceled", "execution_id": None, "order_id": order_id, "client_order_id": coid,
            "symbol": "BTC/USD", "side": "buy", "price": None, "qty": None, "position_qty": None,
            "ts": now, "account": "paper", "order": order, "source": "test"}


def _pending(r) -> int:
    info = r.xpending(runtime.STREAM_TRADE_UPDATES, "persister")
    return int(info.get("pending", 0) if isinstance(info, dict) else info[0])


def test_persister_replays_backlog(live_redis):
    paper_only()
    r = live_redis
    if runtime.worker_alive(r, "persister") and not started_by_tests("persister"):
        pytest.skip("persister is run by the user; this test never stops it")
    ensure_worker("persister")
    if not started_by_tests("persister"):
        pytest.skip("persister not started by tests")

    oms_alive = runtime.worker_alive(r, "oms") is not None
    payloads = [_fake_payload(f"test-{uuid.uuid4()}") for _ in range(3)]
    ids = [p["order_id"] for p in payloads]
    conn = None
    try:
        stop_worker("persister")
        assert wait_until(lambda: runtime.worker_alive(r, "persister") is None, 20), \
            "persister still heartbeating after stop"
        for p in payloads:
            runtime.publish(r, runtime.STREAM_TRADE_UPDATES, p)

        ensure_worker("persister")
        conn = db()
        q = "SELECT COUNT(DISTINCT order_id) FROM trade_events WHERE order_id IN (?,?,?)"
        assert wait_until(lambda: conn.execute(q, ids).fetchone()[0] == 3, 15), \
            "synthetic trade updates did not reach trade_events"
        assert wait_until(lambda: _pending(r) == 0, 15), "persister group still has pending entries"

        if oms_alive:
            for oid in ids:
                assert wait_until(lambda oid=oid: r.hgetall(runtime.order_key(oid)), 15), \
                    f"OMS did not build live hash for {oid}"
    finally:
        try:
            ensure_worker("persister")
        except BaseException as e:  # noqa: BLE001
            print("restart persister failed:", e)
        for p in payloads:
            try:
                r.delete(runtime.order_key(p["order_id"]), runtime.coid_key(p["client_order_id"]))
                r.srem(runtime.SET_OPEN_ORDERS, p["order_id"])
            except Exception:  # noqa: BLE001
                pass
        if conn is not None:
            conn.close()
