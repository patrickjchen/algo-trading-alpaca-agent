"""Pure execution-algorithm math for the EMS (no Redis, no SQLite, no Alpaca).

Strategies turn a parent order (total qty over a time window) into a schedule of
child ``Slice`` objects and price each child against the current quote.

TWAP roll-forward rule (implemented by ems.py, not here): each slice is submitted
as a resting GTC limit order at its scheduled time. At the next slice time any
unfilled quantity of the previous slice is cancelled and ADDED to the next
slice's quantity. The final slice is sent IOC, crossing the touch by
``cross_bps`` so the remainder has the best chance to complete while still
respecting ``limit_cap``.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP, InvalidOperation

MIN_INTERVAL_S = 10.0
_DEFAULT_QTY_STEP = Decimal("0.00000001")  # 8 dp


@dataclass
class Slice:
    idx: int
    scheduled_ts: float      # epoch seconds
    qty: float               # rounded to the asset increment

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AssetRules:
    """Mirrors trader.get_asset_rules(); any field may be None (unknown)."""
    min_order_size: float | None = None
    min_trade_increment: float | None = None
    price_increment: float | None = None


def _dec(x) -> Decimal:
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _step(increment) -> Decimal | None:
    if increment is None:
        return None
    try:
        d = _dec(increment)
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not d.is_finite() or d <= 0:
        return None
    return d


def _round_to(value: Decimal, step: Decimal, mode) -> Decimal:
    units = (value / step).to_integral_value(rounding=mode)
    return units * step


def _round_qty_dec(qty, increment) -> Decimal:
    q = _dec(qty)
    step = _step(increment) or _DEFAULT_QTY_STEP
    if q <= 0:
        return Decimal(0)
    return _round_to(q, step, ROUND_FLOOR)


def round_qty(qty: float, increment: float | None) -> float:
    """Floor ``qty`` to a multiple of ``increment`` using Decimal math (None -> 8 dp)."""
    return float(_round_qty_dec(qty, increment))


def round_price(px: float, increment: float | None, side: str | None = None) -> float:
    """Round a price to ``increment`` (None -> 8 dp).

    Without side: nearest. buy -> floor (never pay more than intended);
    sell -> ceil (never receive less than intended).
    """
    p = _dec(px)
    step = _step(increment) or _DEFAULT_QTY_STEP
    s = (side or "").lower()
    if s == "buy":
        mode = ROUND_FLOOR
    elif s == "sell":
        mode = ROUND_CEILING
    else:
        mode = ROUND_HALF_UP
    return float(_round_to(p, step, mode))


def rules_from_dict(d: dict | None) -> AssetRules:
    if d is None:
        return AssetRules()
    if isinstance(d, AssetRules):
        return d

    def f(key):
        v = d.get(key) if isinstance(d, dict) else getattr(d, key, None)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return AssetRules(min_order_size=f("min_order_size"),
                      min_trade_increment=f("min_trade_increment"),
                      price_increment=f("price_increment"))


class Strategy:
    id: str = "base"
    name: str = "Base"
    available: bool = False
    note: str = ""
    params_schema: dict = {}

    def __init__(self, params: dict | None = None):
        merged = {k: spec.get("default") for k, spec in (self.params_schema or {}).items()}
        for k, v in (params or {}).items():
            if v is None:
                continue
            spec = (self.params_schema or {}).get(k)
            if spec and spec.get("type") == "number":
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    raise ValueError(f"parameter {k!r} must be a number, got {v!r}")
            merged[k] = v
        self.params = merged

    def build_schedule(self, total_qty: float, start_ts: float, end_ts: float, n_slices: int,
                       rules: AssetRules) -> list[Slice]:
        raise NotImplementedError(self.note or f"{self.name} is not implemented")

    def child_price(self, side: str, quote: dict, limit_cap: float | None, final: bool,
                    rules: AssetRules) -> float | None:
        raise NotImplementedError(self.note or f"{self.name} is not implemented")

    def describe(self) -> dict:
        return {"id": self.id, "name": self.name, "available": self.available,
                "note": self.note, "params_schema": dict(self.params_schema or {})}


class TWAP(Strategy):
    """Time-weighted average price.

    Splits ``total_qty`` into ``n_slices`` equal slots over [start_ts, end_ts).
    Slice i is scheduled at ``start_ts + i * interval``. Children rest at the touch
    (GTC limit); the EMS rolls any unfilled qty forward into the next slice
    (cancel at next slice time, add to that slice). The final slice crosses the
    touch by ``cross_bps`` and is sent IOC. ``limit_cap`` is always respected
    because TWAP never sends market orders.
    """
    id = "twap"
    name = "TWAP (time-weighted)"
    available = True
    note = ""
    params_schema = {
        "cross_bps": {"type": "number", "default": 5.0,
                      "help": "How far (basis points) the FINAL slice crosses the touch."},
    }

    def build_schedule(self, total_qty: float, start_ts: float, end_ts: float, n_slices: int,
                       rules: AssetRules) -> list[Slice]:
        rules = rules_from_dict(rules) if not isinstance(rules, AssetRules) else rules
        try:
            n = int(n_slices)
        except (TypeError, ValueError):
            raise ValueError(f"n_slices must be an integer, got {n_slices!r}")
        if n < 1:
            raise ValueError(f"n_slices must be >= 1 (got {n_slices})")
        try:
            total = _dec(total_qty)
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError(f"total_qty must be a number, got {total_qty!r}")
        if not total.is_finite() or total <= 0:
            raise ValueError(f"total_qty must be > 0 (got {total_qty})")
        start, end = float(start_ts), float(end_ts)
        interval = (end - start) / n
        if interval < MIN_INTERVAL_S:
            raise ValueError(
                f"slice interval {interval:.2f}s is below the minimum {MIN_INTERVAL_S:.0f}s: "
                f"duration {end - start:.1f}s / {n} slices; use a longer duration or fewer slices")

        inc = rules.min_trade_increment
        total_r = _round_qty_dec(total, inc)
        if total_r <= 0:
            raise ValueError(f"total_qty {total_qty} rounds to 0 at increment {inc}")
        min_size = _dec(rules.min_order_size) if rules.min_order_size else None
        if min_size is not None and total_r < min_size:
            raise ValueError(f"total_qty {total_qty} is below the asset min_order_size {rules.min_order_size}")

        base = _round_qty_dec(total_r / n, inc)
        qtys = [base] * n
        remainder = total_r - base * (n - 1)
        qtys[-1] = _round_qty_dec(remainder, inc)
        times = [start + i * interval for i in range(n)]
        items = [[t, q] for t, q in zip(times, qtys)]

        # merge undersized slices into the previous slice (or next for the first)
        if min_size is not None:
            i = 0
            while i < len(items):
                q = items[i][1]
                if 0 < q < min_size and len(items) > 1:
                    if i > 0:
                        items[i - 1][1] += q
                        del items[i]
                        i = max(i - 1, 0)
                        continue
                    items[1][1] += q
                    del items[0]
                    continue
                i += 1

        items = [it for it in items if it[1] > 0]
        return [Slice(idx=k, scheduled_ts=float(t), qty=float(q)) for k, (t, q) in enumerate(items)]

    def child_price(self, side: str, quote: dict, limit_cap: float | None, final: bool,
                    rules: AssetRules) -> float | None:
        rules = rules_from_dict(rules) if not isinstance(rules, AssetRules) else rules
        s = (side or "").lower()
        if s not in ("buy", "sell"):
            raise ValueError(f"side must be 'buy' or 'sell', got {side!r}")
        quote = quote or {}
        key = "ask_price" if s == "buy" else "bid_price"
        raw = quote.get(key)
        try:
            px = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            px = None
        if px is None or px <= 0:
            raise ValueError(f"quote has no usable {key}: {raw!r}")
        if final:
            bps = float(self.params.get("cross_bps") or 0.0)
            px = px * (1 + bps / 1e4) if s == "buy" else px * (1 - bps / 1e4)
        if limit_cap is not None:
            cap = float(limit_cap)
            px = min(px, cap) if s == "buy" else max(px, cap)
        return round_price(px, rules.price_increment, s)


class VWAP(Strategy):
    id = "vwap"
    name = "VWAP (volume-weighted)"
    available = False
    note = "Needs an intraday volume profile (historical bars per time bucket); not implemented yet."
    params_schema = {}

    def build_schedule(self, total_qty, start_ts, end_ts, n_slices, rules):
        raise NotImplementedError(self.note)

    def child_price(self, side, quote, limit_cap, final, rules):
        raise NotImplementedError(self.note)


class ImplementationShortfall(Strategy):
    id = "is"
    name = "Implementation Shortfall (Almgren-Chriss)"
    available = False
    note = "Needs risk aversion, volatility and temporary/permanent impact estimates; not implemented yet."
    params_schema = {}

    def build_schedule(self, total_qty, start_ts, end_ts, n_slices, rules):
        raise NotImplementedError(self.note)

    def child_price(self, side, quote, limit_cap, final, rules):
        raise NotImplementedError(self.note)


REGISTRY: dict[str, type[Strategy]] = {"twap": TWAP, "vwap": VWAP, "is": ImplementationShortfall}


def get_strategy(strategy_id: str, params: dict | None = None) -> Strategy:
    key = (strategy_id or "").strip().lower()
    try:
        cls = REGISTRY[key]
    except KeyError:
        raise ValueError(f"unknown strategy {strategy_id!r}; valid ids: {', '.join(REGISTRY)}")
    return cls(params)


def available_strategies() -> list[dict]:
    return [cls().describe() for cls in REGISTRY.values()]


def build_schedule(strategy_id, total_qty, start_ts, end_ts, n_slices,
                   rules: AssetRules | dict | None, params=None) -> list[Slice]:
    strat = get_strategy(strategy_id, params)
    r = rules if isinstance(rules, AssetRules) else rules_from_dict(rules)
    return strat.build_schedule(total_qty, start_ts, end_ts, n_slices, r)
