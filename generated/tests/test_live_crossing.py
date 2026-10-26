"""Live: internal crossing of two offsetting intents from different books (SAFE: nothing reaches Alpaca).

Both legs are priced far below market (BUY at 80% of bid, SELL at 79%), so they cross each other
inside crossing.py but could never fill at the exchange. ``hold_s: 30`` keeps the first leg
resting long enough for the second to arrive; ``finally`` cancels both (expected ``not_found``).
"""
from __future__ import annotations

import pytest

import runtime
from tests.conftest import (db, ensure_oms_ems, ensure_worker, paper_only, round_down, round_up,
                            wait_until)

pytestmark = pytest.mark.live

SYM = "BTC/USD"


def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _submit(r, coid, side, qty, px, book):
    return runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
        "kind": "submit", "account": "paper", "client_order_id": coid, "symbol": SYM, "side": side,
        "type": "limit", "qty": qty, "limit_price": px, "tif": "gtc", "source": "test",
        "book": book, "cross": "allow", "hold_s": 30}, timeout=25)


def test_internal_cross(live_redis):
    paper_only()
    r = live_redis
    ensure_oms_ems()
    ensure_worker("crossing")
    ensure_worker("persister")

    import trader
    rules = trader.get_asset_rules(SYM)
    quote = trader.latest_quote(SYM)
    bid = float(quote["bid_price"])
    assert bid > 0
    inc = rules.get("price_increment") or 0.0
    buy_px = round_down(bid * 0.80, inc) if inc else round(bid * 0.80)
    sell_px = round_down(bid * 0.79, inc) if inc else round(bid * 0.79)
    assert buy_px >= sell_px > 0
    qty = round_up(max(rules.get("min_order_size") or 0.0, 2.0 / buy_px), rules.get("min_trade_increment"))
    print("cross spec:", {"qty": qty, "buy_px": buy_px, "sell_px": sell_px, "rules": rules})

    sell_coid = runtime.new_client_order_id("tc")
    buy_coid = runtime.new_client_order_id("tc")
    crosses_before = _f(r.hget(runtime.CROSS_STATS_KEY, "crosses"))
    fee_before = _f(r.hget(runtime.CROSS_STATS_KEY, "est_fee_saved"))
    conn = db()
    try:
        srep = _submit(r, sell_coid, "sell", qty, sell_px, "B")
        print("sell reply:", srep)
        assert srep.get("ok") is True and srep.get("status") == "acked", srep
        assert srep.get("venue") == "internal"
        assert float(srep.get("crossed_qty") or 0) == 0.0, srep
        assert not srep.get("routed_order_id"), srep

        brep = _submit(r, buy_coid, "buy", qty, buy_px, "A")
        print("buy reply:", brep)
        assert brep.get("ok") is True and brep.get("status") == "acked", brep
        assert brep.get("venue") == "internal"
        assert abs(float(brep.get("crossed_qty") or 0) - qty) < 1e-9, brep
        assert len(brep.get("crosses") or []) == 1, brep
        assert float(brep.get("remaining") or 0) == 0.0, brep
        cross_id = brep["crosses"][0]

        mid = (buy_px + sell_px) / 2.0
        tol = (inc or 0.0) + 1e-6
        for coid in (sell_coid, buy_coid):
            key = runtime.order_key(runtime.internal_order_id(coid))
            h = wait_until(lambda key=key: (lambda h: h if h.get("status") == "filled" else None)(
                r.hgetall(key)), 15)
            assert h, f"{key} not filled: {r.hgetall(key)}"
            print(coid, "internal hash:", h)
            assert h.get("venue") == "internal"
            assert abs(_f(h.get("filled_avg_price")) - mid) <= tol, (h.get("filled_avg_price"), mid)
            assert abs(_f(h.get("filled_qty")) - qty) < 1e-9
            cur = r.get(runtime.coid_key(coid))
            assert cur is None or str(cur).startswith("int-"), cur
            assert trader.get_order_by_client_id(coid) is None

        assert _f(r.hget(runtime.CROSS_STATS_KEY, "crosses")) >= crosses_before + 1
        assert _f(r.hget(runtime.CROSS_STATS_KEY, "est_fee_saved")) > fee_before

        rows = wait_until(lambda: (lambda rows: rows if len(rows) >= 2 else None)(conn.execute(
            "SELECT * FROM fills WHERE cross_id=?", (cross_id,)).fetchall()), 20)
        assert rows, "internal fills not persisted"
        assert len(rows) == 2
        assert all(row["venue"] == "internal" for row in rows)
        assert {row["order_id"] for row in rows} == {runtime.internal_order_id(sell_coid),
                                                      runtime.internal_order_id(buy_coid)}
        for coid in (sell_coid, buy_coid):
            oid = runtime.internal_order_id(coid)
            row = wait_until(lambda oid=oid: (lambda row: row if row and row["status"] == "filled" else None)(
                conn.execute("SELECT * FROM orders WHERE order_id=?", (oid,)).fetchone()), 20)
            assert row is not None, f"orders row for {oid} missing or not filled"
            assert row["venue"] == "internal"
    finally:
        for coid in (sell_coid, buy_coid):
            try:
                rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
                    "kind": "cancel", "account": "paper", "client_order_id": coid, "source": "test"},
                    timeout=15)
                print("cleanup cancel", coid, rep.get("status"))
            except Exception as e:  # noqa: BLE001
                print("cleanup cancel failed:", e)
            try:
                import trader
                o = trader.get_order_by_client_id(coid)
                if o and str(o.get("status") or "").lower() not in runtime.TERMINAL_STATUSES:
                    trader.cancel(o.get("id"), False)
            except Exception as e:  # noqa: BLE001
                print("exchange safety cancel failed:", e)
        conn.close()
