"""Live: TWAP end-to-end through OMS (algo_create) -> algo engine -> OMS -> EMS -> PAPER.

Runs only with ``--run-twap`` (fills tiny paper quantities).
"""
from __future__ import annotations

import json

import pytest

import runtime
from tests.conftest import (db, ensure_oms_ems, ensure_worker, paper_only, round_up, wait_until)

pytestmark = pytest.mark.live

DONE = {"completed", "partial"}


def test_twap_end_to_end(live_redis, request):
    if not request.config.getoption("--run-twap"):
        pytest.skip("needs --run-twap")
    paper_only()
    r = live_redis
    ensure_worker("subscriber")
    ensure_worker("trade_stream")
    ensure_oms_ems()
    ensure_worker("persister")
    ensure_worker("algo_engine")

    import trader
    rules = trader.get_asset_rules("BTC/USD")
    quote = trader.latest_quote("BTC/USD")
    px = float(quote["ask_price"])
    min_qty = round_up(max(rules.get("min_order_size") or 0.0, 2.0 / px), rules.get("min_trade_increment"))
    total = round_up(2 * min_qty, rules.get("min_trade_increment"))
    print("TWAP total_qty:", total)

    algo_id = None
    conn = db()
    try:
        rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
            "kind": "algo_create", "strategy": "twap", "account": "paper", "symbol": "BTC/USD",
            "side": "buy", "total_qty": total, "duration_s": 30, "n_slices": 2, "limit_cap": None,
            "params": {}, "source": "test"}, timeout=20)
        print("algo_create reply:", rep)
        assert rep.get("ok") is True, rep
        algo_id = rep.get("algo_id")
        assert algo_id

        status = wait_until(lambda: (lambda s: s if s in DONE else None)(
            r.hget(runtime.algo_key(algo_id), "status")), 120, every=1.0)
        h = r.hgetall(runtime.algo_key(algo_id))
        print("algo:", h)
        assert status in DONE, f"algo status {h.get('status')} error={h.get('error')}"

        slices = [json.loads(v) for v in r.hgetall(runtime.algo_slices_key(algo_id)).values()]
        assert slices
        child_ids = []
        for sl in slices:
            print("slice:", sl)
            assert str(sl.get("client_order_id") or "").startswith("algo-")
            if sl.get("status") in ("filled", "cancelled", "submitted", "working") or sl.get("order_id"):
                assert sl.get("order_id"), f"slice {sl.get('slice_idx')} has no order_id"
                child_ids.append(sl["order_id"])
        assert child_ids

        row = wait_until(lambda: (lambda row: row if row and row["status"] == status else None)(
            conn.execute("SELECT * FROM algo_orders WHERE algo_id=?", (algo_id,)).fetchone()), 20)
        assert row is not None, "algo_orders row missing or status mismatch"
        marks = ",".join("?" for _ in child_ids)
        assert wait_until(lambda: conn.execute(
            f"SELECT COUNT(*) FROM fills WHERE order_id IN ({marks})", child_ids).fetchone()[0] >= 1, 20), \
            "no fills persisted for TWAP children"
    finally:
        if algo_id:
            st = r.hget(runtime.algo_key(algo_id), "status")
            if st not in ("completed", "partial", "cancelled", "failed"):
                try:
                    print("cancel algo:", runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
                        "kind": "algo_cancel", "algo_id": algo_id, "source": "test"}, timeout=20))
                except Exception as e:  # noqa: BLE001
                    print("algo cancel failed:", e)
        conn.close()
