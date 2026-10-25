"""Live: asset rules, latest quote, account, client-id lookup (REAL paper account, read-only)."""
from __future__ import annotations

import pytest

from tests.conftest import paper_only

pytestmark = pytest.mark.live


def test_asset_rules_btc():
    paper_only()
    import trader
    rules = trader.get_asset_rules("BTC/USD")
    print("BTC/USD rules:", rules)
    for k in ("min_order_size", "min_trade_increment", "price_increment"):
        assert isinstance(rules[k], float), f"{k} not numeric: {rules[k]!r}"
        assert rules[k] > 0
    assert rules["tradable"] is True


def test_latest_quote_btc():
    paper_only()
    import trader
    q = trader.latest_quote("BTC/USD")
    print("BTC/USD quote:", q)
    assert q["bid_price"] is not None and q["ask_price"] is not None
    assert 0 < q["bid_price"] < q["ask_price"]


def test_account_active_and_paper():
    paper_only()
    import trader
    acct = trader.account(False)
    st = str(acct.get("status")).upper().split(".")[-1]
    assert st == "ACTIVE", acct.get("status")
    client = trader.get_client(False)
    base = str(getattr(client, "_base_url", "") or getattr(client, "base_url", "") or "")
    if base:
        assert "paper" in base, base


def test_get_order_by_client_id_missing():
    paper_only()
    import trader
    assert trader.get_order_by_client_id("does-not-exist-xyz") is None
