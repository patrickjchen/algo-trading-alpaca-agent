You are generating: trader.py — the ONLY module that imports alpaca.trading. Thin, synchronous, importable.

REQUIREMENTS
- Imports: `TradingClient`, `MarketOrderRequest`, `LimitOrderRequest`, `GetOrdersRequest`, enums `OrderSide, TimeInForce, QueryOrderStatus`, `APIError` from `alpaca.common.exceptions`; `CryptoHistoricalDataClient` + `CryptoLatestQuoteRequest` from `alpaca.data`; `import runtime`.
- `get_client(live: bool = False) -> TradingClient`: cached per `live` flag in a module dict (create lazily; env read inside). `TradingClient(key, secret, paper=not live)`. Raise `RuntimeError("ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY not set")` if missing.
- `_to_dict(obj)`: `model_dump(mode="json")` → `dict()` → `__dict__`; lists recurse; enums → `.value`; UUID/datetime → str.
- `_side(s: str) -> OrderSide`, `_tif(s: str) -> TimeInForce` (accept "gtc"/"ioc" case-insensitively; anything else → ValueError "crypto supports gtc or ioc").
- `submit_market(symbol, qty, side, client_order_id, live=False, tif="gtc") -> dict` and `submit_limit(symbol, qty, side, limit_price, client_order_id, live=False, tif="gtc") -> dict`: build the request with `qty=str(qty)`? No — pass `qty=float(qty)`, `limit_price=float(limit_price)`, `client_order_id=client_order_id`, `time_in_force=_tif(tif)`; return `_to_dict(client.submit_order(req))`.
- `cancel(order_id, live=False) -> None`; `cancel_all(live=False) -> list`.
- `get_order(order_id, live=False) -> dict`; `get_order_by_client_id(client_order_id, live=False) -> dict | None` (None when `APIError` with status 404 / code 40410000 / "not found" in message).
- `open_orders(live=False, symbols=None) -> list[dict]` (`GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500, symbols=symbols)`).
- `list_orders(status="all", after=None, limit=500, live=False) -> list[dict]` — `status` in {"open","closed","all"}; `after` ISO string or datetime → pass as datetime; `direction="asc"` when `after` given.
- `account(live=False) -> dict`, `positions(live=False) -> list[dict]`.
- `get_asset_rules(symbol, live=False, r=None) -> dict`: if `r` given, try `HGETALL alpaca:asset:<SYM>` (floats parsed) first; else `client.get_asset(symbol)` → `{symbol, min_order_size, min_trade_increment, price_increment, tradable}` as floats/bool (fields may be None → use conservative defaults: min_order_size 0.0001? NO — if None, set `None` and let callers decide; do not invent numbers). Cache into Redis (HSET + EXPIRE 3600) when `r` given, via `runtime.redis_call`.
- `latest_quote(symbol, r=None) -> dict`: `HGETALL alpaca:latest:<SYM>` from Redis if `r` given and both bid_price and ask_price present and timestamp younger than 30s → `source="redis"`; else `CryptoHistoricalDataClient()` (no creds needed) `.get_crypto_latest_quote(CryptoLatestQuoteRequest(symbol_or_symbols=symbol))[symbol]` → `source="rest"`. Return floats + `timestamp` ISO.
- `is_transient(exc) -> bool`: True for connection errors / timeouts (requests exceptions, OSError), `APIError` with status_code in {408, 429} or ≥ 500; False otherwise.
- `error_info(exc) -> dict {type, status_code, code, message}` best-effort for logging/replies.
- No top-level network; env read inside `get_client`. Call `mcp__verify__verify_stage(stage="trader")`.
