You are a senior engineer documenting the Alpaca API for a teammate who will
write code from your notes without reading the docs. Your ground truth is the
INSTALLED alpaca-py package source (path given in the task); read it with
Read/Grep. You may WebFetch only the whitelisted Alpaca doc URLs. Every claim
you cannot verify in source or docs is marked `# TODO verify`.

Write `RESEARCH.md` (raw markdown, no outer code fence) with EXACTLY these
three top-level sections. Be concrete: class names, constructor signatures,
method signatures, model field names + types, enum values. Cite the source
file and line (e.g. `alpaca/trading/stream.py:189`) for every non-obvious
claim.

## Part A -- Crypto market data websocket (`alpaca.data.live.crypto.CryptoDataStream`)
- constructor args; `subscribe_trades/quotes/orderbooks(handler, *symbols)`; handler signature (async, one model arg); `run()` is sync, `_run_forever()` is the async coroutine, `close()` is async, `stop()` sync.
- Models: `Trade` (price, size, timestamp, exchange, id, conditions, tbt?), `Quote` (bid_price, bid_size, ask_price, ask_size, bid_exchange, ask_exchange, timestamp), `Orderbook` (symbol, timestamp, bids, asks, reset) and the level item type (attrs price/size). State explicitly that the orderbook stream is hybrid snapshot/delta via `reset`.
- Reconnect behaviour inside `_run_forever` (backoff bounds, ping settings, what happens when a handler raises, when it gives up). Quote the relevant code lines.
- `CryptoHistoricalDataClient` (no creds needed for crypto) + `CryptoLatestQuoteRequest` to fetch a latest quote via REST; response shape.

## Part B -- Trading REST (`alpaca.trading.client.TradingClient`)
- constructor (`api_key, secret_key, paper=True`), base URLs paper vs live.
- Methods with signatures: `submit_order(order_data)`, `get_order_by_id`, `get_order_by_client_id`, `get_orders(filter: GetOrdersRequest)` and ALL `GetOrdersRequest` fields (status, limit max, after, until, direction, nested, side, symbols), `cancel_order_by_id`, `cancel_orders`, `get_account`, `get_all_positions`, `get_asset(symbol_or_asset_id)`.
- Request models `MarketOrderRequest`/`LimitOrderRequest`: every field incl. `client_order_id`, `time_in_force`, `qty` vs `notional`; crypto-allowed TIF values (gtc/ioc only); enums `OrderSide`, `TimeInForce`, `OrderType`, `OrderStatus` (list every value), `QueryOrderStatus`.
- `Order` model: every field name and type (note numeric fields are strings), and that `model_dump(mode="json")` yields enum values as lowercase strings.
- `Asset` model fields relevant to crypto: `min_order_size`, `min_trade_increment`, `price_increment`, `tradable`, `fractionable`. Give the actual BTC/USD values if the docs state them, else `# TODO verify`.
- `APIError` (`alpaca.common.exceptions.APIError`): attributes (`status_code`, `code`, `message`/`response`), how to detect 404 vs 422 vs 429 vs 5xx; what a duplicate `client_order_id` returns `# TODO verify` if unsure; client_order_id max length `# TODO verify`.
- Rate limit: 200 requests/min per key.

## Part C -- Trading websocket (`alpaca.trading.stream.TradingStream`) and TradeUpdate
- constructor (`api_key, secret_key, paper=True, ...`), websocket URLs paper/live, `subscribe_trade_updates(handler)`, sync `run()` vs async `_run_forever()`, reconnect/backoff/ping details with line cites, behaviour when the handler raises, the one-connection-per-account limit (cite the SDK comment about 429/abandoned sessions if present).
- `TradeUpdate` model fields (event, execution_id, order, timestamp, position_qty, price, qty) and the `TradeEvent` enum values (all of them).

Finish with a short "Gotchas" list (10 bullets max) and a "Unverified" list collecting every `# TODO verify`. Target 250–400 lines. When the file is written, reply DONE.
