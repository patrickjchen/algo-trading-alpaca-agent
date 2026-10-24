"""Live: subscriber populates Redis market keys for BTC/USD."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.conftest import ensure_worker, wait_until

pytestmark = pytest.mark.live

SYM = "BTC/USD"


def _parse(ts):
    s = str(ts).strip()
    try:
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except ValueError:
        pass
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def test_subscriber_market_keys(live_redis):
    r = live_redis
    ensure_worker("subscriber")

    latest = wait_until(lambda: (lambda h: h if h.get("bid_price") and h.get("ask_price") else None)(
        r.hgetall(f"alpaca:latest:{SYM}")), 60)
    assert latest, "alpaca:latest:BTC/USD never got bid/ask"
    assert float(latest["bid_price"]) > 0 and float(latest["ask_price"]) > 0

    assert wait_until(lambda: r.sismember("alpaca:symbols", SYM), 60), "BTC/USD not in alpaca:symbols"
    assert wait_until(lambda: r.hlen(f"alpaca:ob:bids:{SYM}") >= 1, 60), "no bid levels"

    def uncrossed():
        bids = r.hgetall(f"alpaca:ob:bids:{SYM}")
        asks = r.hgetall(f"alpaca:ob:asks:{SYM}")
        if not bids or not asks:
            return None
        mb = max(float(p) for p in bids)
        ma = min(float(p) for p in asks)
        return (mb, ma) if mb < ma else None

    res = wait_until(uncrossed, 30)
    assert res, "orderbook crossed or empty"
    print("best bid/ask from book:", res)

    meta = wait_until(lambda: r.hgetall(f"alpaca:orderbook:{SYM}").get("timestamp"), 60)
    assert meta, "no orderbook metadata timestamp"
    age = (datetime.now(timezone.utc) - _parse(meta)).total_seconds()
    assert age < 120, f"orderbook metadata is {age:.0f}s old"
