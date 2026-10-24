"""Live: trade updates buffered in the stream while OMS is down are consumed after restart."""
from __future__ import annotations

import json
import uuid

import pytest

import runtime
from tests.conftest import db, ensure_worker, started_by_tests, stop_worker, wait_until

pytestmark = pytest.mark.live


def _count_events(ids):
    conn = db()
    try:
        ph = ",".join("?" * len(ids))
        return conn.execute(f"SELECT COUNT(DISTINCT order_id) FROM trade_events WHERE order_id IN ({ph})",
                            ids).fetchone()[0]
    finally:
        conn.close()


def _pending(r):
    try:
        info = r.xpending(runtime.STREAM_TRADE_UPDATES, "oms")
        return int(info.get("pending", 0)) if isinstance(info, dict) else int(info[0])
    except Exception:  # noqa: BLE001
        return -1


def test_oms_resumes_buffered_updates(live_redis):
    r = live_redis
    ensure_worker("oms")
    if not started_by_tests("oms"):
        pytest.skip("oms is run by the user; not killing it")

    stop_worker("oms")
    assert not runtime.worker_alive(r, "oms")

    ids = []
    for _ in range(3):
        oid = "test-" + uuid.uuid4().hex
        ids.append(oid)
        order = {"id": oid, "client_order_id": oid[:40], "symbol": "BTC/USD", "side": "buy",
                 "type": "limit", "time_in_force": "gtc", "qty": "0.0001", "limit_price": "1",
                 "filled_qty": "0", "status": "canceled", "created_at": runtime.now_iso(),
                 "submitted_at": runtime.now_iso(), "updated_at": runtime.now_iso()}
        payload = {"event": "canceled", "execution_id": None, "order_id": oid,
                   "client_order_id": order["client_order_id"], "symbol": "BTC/USD", "side": "buy",
                   "price": None, "qty": None, "position_qty": None, "ts": runtime.now_iso(),
                   "account": "paper", "order": order}
        r.xadd(runtime.STREAM_TRADE_UPDATES, {"json": json.dumps(payload, default=str)})

    ensure_worker("oms")
    assert wait_until(lambda: _count_events(ids) == 3, 15), \
        f"only {_count_events(ids)}/3 synthetic updates reached trade_events"
    assert wait_until(lambda: _pending(r) == 0, 15), f"XPENDING oms = {_pending(r)}"
