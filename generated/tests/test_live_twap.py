"""Live: end-to-end TWAP through EMS -> OMS -> Alpaca paper (fills tiny qty). --run-twap only."""
from __future__ import annotations

import pytest

import runtime
from tests.conftest import db, ensure_worker, round_up, wait_until

pytestmark = pytest.mark.live

DONE = {"completed", "partial"}


def _q(sql, args=()):
    conn = db()
    try:
        return [{k: row[k] for k in row.keys()} for row in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def test_twap_end_to_end(live_redis, request):
    if not request.config.getoption("--run-twap"):
        pytest.skip("needs --run-twap (places and fills real paper orders)")
    import trader
    r = live_redis
    for w in ("subscriber", "trade_stream", "oms", "ems"):
        ensure_worker(w)

    rules = trader.get_asset_rules("BTC/USD")
    min_qty = round_up(max(rules["min_order_size"] or 0.0, rules["min_trade_increment"] or 0.0),
                       rules["min_trade_increment"])
    total = round_up(2 * min_qty, rules["min_trade_increment"])
    algo_id = None
    try:
        rep = runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, {
            "req_id": runtime.new_id(), "kind": "create", "strategy": "twap", "account": "paper",
            "symbol": "BTC/USD", "side": "buy", "total_qty": total, "duration_s": 30, "n_slices": 2,
            "limit_cap": None, "params": {}, "source": "test"}, timeout=20)
        print("create reply:", {k: rep.get(k) for k in ("ok", "status", "algo_id", "error")})
        assert rep.get("ok") is True, rep
        algo_id = rep["algo_id"]

        def status():
            rows = _q("SELECT status FROM algo_orders WHERE algo_id=?", (algo_id,))
            return rows[0]["status"] if rows else None

        final = wait_until(lambda: status() if status() in DONE | {"failed", "cancelled"} else None,
                           120, every=1.0)
        assert final in DONE, f"algo ended {final!r}"

        children = _q("SELECT order_id, client_order_id FROM orders WHERE client_order_id LIKE ?",
                      (f"algo-{algo_id[:12]}-%",))
        assert children, "no child orders recorded"
        for c in children:
            assert c["client_order_id"].startswith("algo-")
        ids = [c["order_id"] for c in children]
        ph = ",".join("?" * len(ids))
        fills = wait_until(lambda: _q(f"SELECT * FROM fills WHERE order_id IN ({ph})", ids), 30)
        assert fills and len(fills) >= 1, "no fills for algo children"
    finally:
        if algo_id:
            st = _q("SELECT status FROM algo_orders WHERE algo_id=?", (algo_id,))
            if st and st[0]["status"] in ("pending", "running"):
                runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, {
                    "req_id": runtime.new_id(), "kind": "cancel", "algo_id": algo_id}, timeout=20)
