You are generating: algos.py — pure execution-algorithm math. No Redis, no SQLite, no alpaca imports (only `runtime` for constants if needed). Fully unit-testable.

```python
@dataclass
class Slice:
    idx: int
    scheduled_ts: float      # epoch seconds
    qty: float               # rounded to the asset increment

@dataclass
class AssetRules:           # mirrors trader.get_asset_rules(); fields may be None
    min_order_size: float | None = None
    min_trade_increment: float | None = None
    price_increment: float | None = None

def round_qty(qty: float, increment: float | None) -> float      # floor to increment (Decimal math, no float drift); None → 8 dp
def round_price(px: float, increment: float | None, side: str | None = None) -> float
    # nearest; if side given: buy → floor (never pay more than intended), sell → ceil

class Strategy:
    id: str = "base"; name: str = "Base"; available: bool = False
    note: str = ""                      # shown in the GUI dropdown for unavailable strategies
    params_schema: dict = {}            # {param: {"type": "number"|"string", "default": ..., "help": ...}}
    def __init__(self, params: dict | None = None)
    def build_schedule(self, total_qty: float, start_ts: float, end_ts: float, n_slices: int, rules: AssetRules) -> list[Slice]
    def child_price(self, side: str, quote: dict, limit_cap: float | None, final: bool, rules: AssetRules) -> float | None
        # quote = {"bid_price":..,"ask_price":..}; return None => use a market order
    def describe(self) -> dict        # {id, name, available, note, params_schema}

class TWAP(Strategy):   id="twap", name="TWAP (time-weighted)", available=True
    params: cross_bps (default 5.0)  — how far the FINAL slice crosses the touch
    build_schedule: n_slices equal slots across [start_ts, end_ts): slice i at start + i*interval; interval = (end-start)/n;
        require end_ts - start_ts >= 10*n_slices? No: require interval >= 10 s and n_slices >= 1, total_qty > 0 → else ValueError with a clear message.
        qty per slice = round_qty(total/n); accumulate; put the remainder (total - sum) on the LAST slice (rounded); if any slice qty < min_order_size (when known) merge it into the previous slice (or next if it is the first); drop zero-qty slices; re-index.
    child_price: price = ask for buy / bid for sell (touch). If final: cross by cross_bps (buy: ask*(1+bps/1e4), sell: bid*(1-bps/1e4)). Apply limit_cap: buy → min(price, cap); sell → max(price, cap). Round with round_price(side). Returns float (TWAP never uses market orders so the cap is always respected).

class VWAP(Strategy): id="vwap", name="VWAP (volume-weighted)", available=False,
    note="Needs an intraday volume profile (historical bars per time bucket); not implemented yet."
    build_schedule → raise NotImplementedError(note)
class ImplementationShortfall(Strategy): id="is", name="Implementation Shortfall (Almgren-Chriss)", available=False,
    note="Needs risk aversion, volatility and temporary/permanent impact estimates; not implemented yet."

REGISTRY: dict[str, type[Strategy]] = {"twap": TWAP, "vwap": VWAP, "is": ImplementationShortfall}
def get_strategy(strategy_id: str, params: dict | None = None) -> Strategy   # KeyError → ValueError listing valid ids
def available_strategies() -> list[dict]    # [cls().describe() for cls in REGISTRY.values()]
def build_schedule(strategy_id, total_qty, start_ts, end_ts, n_slices, rules: AssetRules | dict | None, params=None) -> list[Slice]
def rules_from_dict(d: dict | None) -> AssetRules
```
Docstrings explaining the TWAP roll-forward rule (done by EMS): unfilled qty of a slice is cancelled at the next slice time and added to that slice. Call `mcp__verify__verify_stage(stage="algos")`.
