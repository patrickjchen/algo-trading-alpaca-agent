from decimal import Decimal

import pytest

import algos

RULES = algos.AssetRules(min_order_size=0.0001, min_trade_increment=0.0001, price_increment=0.01)


def test_twap_equal_slices():
    sl = algos.build_schedule("twap", 1.0, 0, 400, 4, RULES)
    assert len(sl) == 4
    assert [s.scheduled_ts for s in sl] == [0, 100, 200, 300]
    assert all(s.qty == pytest.approx(0.25) for s in sl)


def test_twap_remainder_on_last():
    sl = algos.build_schedule("twap", 1.0003, 0, 400, 4, RULES)
    assert [s.qty for s in sl[:3]] == [0.25, 0.25, 0.25]
    assert sl[-1].qty == pytest.approx(0.2503)
    assert sum(Decimal(str(s.qty)) for s in sl) == Decimal("1.0003")


def test_twap_tail_merged():
    rules = algos.AssetRules(min_order_size=0.001, min_trade_increment=0.0001)
    sl = algos.build_schedule("twap", 0.0035, 0, 400, 4, rules)
    assert len(sl) < 4
    assert all(s.qty >= 0.001 - 1e-12 for s in sl)
    assert sum(s.qty for s in sl) == pytest.approx(0.0035)
    assert [s.idx for s in sl] == list(range(len(sl)))


def test_twap_interval_too_short():
    with pytest.raises(ValueError):
        algos.build_schedule("twap", 1.0, 0, 30, 4, RULES)


def test_round_qty_floors():
    assert algos.round_qty(0.00123456, 0.0001) == pytest.approx(0.0012)
    assert algos.round_qty(0.00019, 0.0001) == pytest.approx(0.0001)


def test_round_price_sides():
    assert algos.round_price(100.007, 0.01, "buy") == pytest.approx(100.0)
    assert algos.round_price(100.001, 0.01, "sell") == pytest.approx(100.01)


def test_child_price_touch():
    t = algos.TWAP()
    q = {"bid_price": 100.0, "ask_price": 101.0}
    assert t.child_price("buy", q, None, False, RULES) == pytest.approx(101.0)
    assert t.child_price("sell", q, None, False, RULES) == pytest.approx(100.0)


def test_child_price_final_crosses():
    t = algos.TWAP({"cross_bps": 5})
    q = {"bid_price": 100.0, "ask_price": 101.0}
    b = t.child_price("buy", q, None, True, RULES)
    s = t.child_price("sell", q, None, True, RULES)
    assert 101.0 < b <= 101.06
    assert 99.94 <= s < 100.0


def test_child_price_cap():
    t = algos.TWAP()
    q = {"bid_price": 100.0, "ask_price": 101.0}
    assert t.child_price("buy", q, 100.5, False, RULES) == pytest.approx(100.5)
    assert t.child_price("sell", q, 100.5, False, RULES) == pytest.approx(100.5)


def test_vwap_is_unavailable():
    for sid in ("vwap", "is"):
        st = algos.get_strategy(sid)
        assert st.available is False
        with pytest.raises(NotImplementedError):
            st.build_schedule(1, 0, 100, 2, RULES)
        with pytest.raises(NotImplementedError):
            st.child_price("buy", {"ask_price": 1}, None, False, RULES)


def test_available_strategies():
    ids = {d["id"] for d in algos.available_strategies()}
    assert ids == {"twap", "vwap", "is"}
    twap = [d for d in algos.available_strategies() if d["id"] == "twap"][0]
    assert twap["available"] is True


def test_unknown_strategy():
    with pytest.raises(ValueError):
        algos.get_strategy("nope")
