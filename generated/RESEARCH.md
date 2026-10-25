# Alpaca API research notes (alpaca-py 0.44.0)

Ground truth: installed package at `site-packages/alpaca/` (paths below are relative to it).
Docs consulted: `docs.alpaca.markets/docs/{real-time-crypto-pricing-data,crypto-trading,websocket-streaming}`.
Anything not confirmed in source or those three pages is tagged `# TODO verify`.

## Part A -- Crypto market data websocket (`alpaca.data.live.crypto.CryptoDataStream`)

### Constructor (`data/live/crypto.py:17-54`)
```python
CryptoDataStream(api_key: str, secret_key: str, raw_data: bool = False,
                 feed: CryptoFeed = CryptoFeed.US, url_override: Optional[str] = None,
                 websocket_params: Optional[Dict] = None, data_timeout: Optional[float] = None)
```
- Endpoint = `wss://stream.data.alpaca.markets` + `/v1beta3/crypto/` + feed → `wss://stream.data.alpaca.markets/v1beta3/crypto/us` (`crypto.py:47`, `common/enums.py:13`). `CryptoFeed` has only `US = "us"` (`data/enums.py:100`). The docs also list `us-1` and `eu-1` (Kraken). To use them, pass `url_override`.
- Keys are positional `str`. The SDK always sends `{"action":"auth","key":..,"secret":..}` over msgpack (`data/live/websocket.py:187-201`). It needs the server to reply `T == "success"` and `msg == "authenticated"`. Otherwise it raises `ValueError`.
- `data_timeout`: an optional staleness watchdog. If no market data arrives within N seconds, the socket is closed and reopened (`websocket.py:249-258`). It must be > 0 (`websocket.py:107-108`).
- `raw_data=True`: handlers get the raw msgpack dict (`websocket.py:293-294`) instead of models.

### Subscribing
```python
subscribe_trades(handler, *symbols: str)      # crypto.py:56
subscribe_quotes(handler, *symbols: str)      # crypto.py:68
subscribe_orderbooks(handler, *symbols)       # crypto.py:116
# also subscribe_bars / subscribe_updated_bars / subscribe_daily_bars, and unsubscribe_* (crypto.py:128-174)
```
- The handler must be an `async def` taking ONE arg (the model, or a dict if `raw_data`). `_ensure_coroutine` raises `ValueError("handler must be a coroutine function")` otherwise (`websocket.py:520-521`).
- Each channel has one handler per symbol. A later subscribe overwrites the earlier one (`websocket.py:381-382`). `"*"` is a wildcard fallback (`websocket.py:366`).
- Symbols use slash form, e.g. `"BTC/USD"` (docs: crypto-trading).
- You can subscribe before `run()`. `_run_forever` waits until at least one handler exists (`websocket.py:427-437`). Subscribing while running calls `asyncio.run_coroutine_threadsafe(...).result()` (`websocket.py:383-386`). That blocks, so call it from another thread and never from inside a handler on the stream's loop (deadlock).

### Lifecycle
- `run(self) -> None` is **sync**. It calls `asyncio.run(self._run_forever())`, catches `KeyboardInterrupt`, and calls `self.stop()` in `finally` (`websocket.py:494-502`).
- `async _run_forever(self)` is the coroutine. Await it yourself to share your own event loop (`websocket.py:421`).
- `async close(self)` closes the socket and sets `_running=False` (`websocket.py:211-220`). It does NOT stop the loop. `_run_forever` reconnects on its next pass.
- `async stop_ws(self)` sets `_should_run=False`, sets the stop event and enqueues a stop marker (`websocket.py:222-229`). This is the correct way to stop from inside the loop.
- `stop(self)` is **sync**: `asyncio.run_coroutine_threadsafe(self.stop_ws(), self._loop).result(timeout=5)` (`websocket.py:504-509`). Call it from another thread only. If it runs on the loop thread, it blocks the loop and times out. It raises `AttributeError` if `_loop` is still `None` (never started).

### Models (all `ValidateBaseModel` = pydantic `BaseModel` with `validate_assignment=True`, `common/models.py:6`)
Wire keys are mapped to model fields by `data/mappings.py`.

`Trade` (`data/models/trades.py:16-37`; mapping `mappings.py:26-34`):
| field | type | wire key |
|---|---|---|
| symbol | str | `S` |
| timestamp | datetime | `t` |
| exchange | Optional[Union[Exchange, str]] | `x` |
| price | float | `p` |
| size | float | `s` |
| id | Optional[int] | `i` |
| conditions | Optional[Union[List[str], str]] | `c` |
| tape | Optional[str] | `z` |

There is **no taker-side field**. The docs say crypto trades carry `tks` (`"B"`/`"S"`), but `tks` is absent from `TRADE_MAPPING`, so it is silently dropped in parsed mode. Use `raw_data=True` to read `msg["tks"]`. `exchange` is likely `None` for crypto because the docs' crypto trade schema has no `x` `# TODO verify`.

`Quote` (`data/models/quotes.py:11-36`; mapping `mappings.py:14-24`): `symbol: str`, `timestamp: datetime`, `bid_price: float` (`bp`), `bid_size: float` (`bs`), `bid_exchange: Optional[Union[str, Exchange]] = None` (`bx`), `ask_price: float` (`ap`), `ask_size: float` (`as`), `ask_exchange: Optional[...] = None` (`ax`), `conditions`, `tape`. The docs' crypto quote schema lists only `S, bp, bs, ap, as, t`, so expect the exchange fields to be `None`.

`Orderbook` (`data/models/orderbooks.py:19-36`; mapping `mappings.py:46-51`): `symbol: str`, `timestamp: datetime` (`t`), `bids: List[OrderbookQuote]` (`b`), `asks: List[OrderbookQuote]` (`a`), `reset: bool = False` (`r`).
Level item `OrderbookQuote` (`orderbooks.py:11-16`): `price: float = Field(alias="p")`, `size: float = Field(alias="s")`.

**The orderbook stream mixes snapshots and deltas, using `reset` to tell them apart.** When `reset=True`, the message is the full server book: discard the local book and rebuild it. This is "typically sent as the first message after subscription" (`orderbooks.py:27-29`, docs). When `reset=False`, only changed levels are sent. Upsert each price level's size, and **delete the level when `size == 0`** (docs: real-time-crypto-pricing-data). After any reconnect the SDK re-subscribes (`websocket.py:454-455`). Expect a fresh `reset=True` snapshot then `# TODO verify`.

Timestamps arrive as msgpack `Timestamp` and become `datetime` via `.to_datetime()` (`websocket.py:296-297`). They are tz-aware UTC `# TODO verify`.

### Reconnect behaviour inside `_run_forever` (`websocket.py:421-492`)
Defaults (`websocket.py:112-113, 121-125`):
```python
self._reconnect_min_backoff = 1.0
self._reconnect_max_backoff = 30.0
self._websocket_params = {"ping_interval": 10, "ping_timeout": 180, "max_queue": 1024}
```
Passing `websocket_params` **replaces** this whole dict and does not merge with it (`websocket.py:127-128`). Include the ping keys yourself.

Backoff (`common/utils.py:23-52`) is exponential with "equal jitter": `capped` doubles from 1.0 up to 30.0, and `return capped / 2 + random.uniform(0, capped / 2)`. Delays by retry: 1→[0.5,1]s, 2→[1,2], 3→[2,4], 4→[4,8], 5→[8,16], 6+→[15,30]. The wait can be interrupted early by `stop_ws()` (`websocket.py:143-152`).

Main loop:
```python
except websockets.WebSocketException as wse:        # websocket.py:468
    await self.close(); self._running = False
    if self._data_received: retries = 0
    retries += 1
    log.warning("data websocket error, restarting connection: " + str(wse))
    await self._wait_before_reconnect(retries)
except Exception as e:                               # websocket.py:476
    if isinstance(e, ValueError) and "insufficient subscription" in str(e):
        await self.close(); log.exception(...); return          # GIVES UP permanently
    log.exception(f"error during websocket communication: {str(e)}")
    if not self._running:                            # failed connect/auth
        await self.close(); retries += 1; await self._wait_before_reconnect(retries)
    elif self._data_received:
        retries = 0
```
- **Handler raises**: the exception escapes `_consume` (only `asyncio.TimeoutError` is caught there, `websocket.py:277`) and lands in `except Exception`. `_running` is still True, so the SDK just logs it with `log.exception`. It does not reconnect or back off, and loops back into `_consume` on the same socket. **That message is lost.** Wrap handler bodies in your own try/except.
- **When it gives up**: only on a `ValueError` whose text contains `"insufficient subscription"` (`websocket.py:477-480`). An auth error raises `ValueError(server msg)` (`websocket.py:198-199`), so a matching server message ends `_run_forever` silently and `run()` returns. Every other error is retried forever.
- Data staleness (only when `data_timeout` is set) also backs off (`websocket.py:458-467`). Control frames do not reset the staleness clock (`websocket.py:264-276`).
- `recv` polls with a 5 s timeout so stop requests are noticed (`websocket.py:248, 261`).

### REST latest quote: `CryptoHistoricalDataClient` (`data/historical/crypto.py:24-78`)
```python
CryptoHistoricalDataClient(api_key=None, secret_key=None, oauth_token=None, raw_data=False,
                           url_override=None, use_basic_auth=False, sandbox=False)
get_crypto_latest_quote(request_params: CryptoLatestQuoteRequest, feed: CryptoFeed = CryptoFeed.US)
    -> Union[Dict[str, Quote], RawData]                                   # crypto.py:179-200
```
- No credentials are needed. `_validate_credentials` is overridden because "crypto does not actually require authentication" (`crypto.py:275-278`). Supplying keys raises your rate limit (`crypto.py:31`).
- Base URL `https://data.alpaca.markets`, API version `v1beta3` (`crypto.py:66, 73`). The path is `/v1beta3/crypto/us/latest/quotes`.
- `CryptoLatestQuoteRequest(symbol_or_symbols: Union[str, List[str]])` (`data/requests.py:308-343`).
- The response is a **dict keyed by symbol**: `client.get_crypto_latest_quote(CryptoLatestQuoteRequest(symbol_or_symbols="BTC/USD"))["BTC/USD"].ask_price`. Values are the same `Quote` model as above. Raw JSON is `{"quotes": {"BTC/USD": {"t","bp","bs","ap","as"}}}` `# TODO verify`.

## Part B -- Trading REST (`alpaca.trading.client.TradingClient`)

### Constructor (`trading/client.py:53-86`)
```python
TradingClient(api_key: Optional[str] = None, secret_key: Optional[str] = None,
              oauth_token: Optional[str] = None, paper: bool = True,
              raw_data: bool = False, url_override: Optional[str] = None)
```
- Paper: `https://paper-api.alpaca.markets`. Live: `https://api.alpaca.markets` (`common/enums.py:9-10`). Requests go to `<base>/v2/...` (`common/rest.py:109`).
- Auth headers are `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` (`rest.py:171-172`). Requires both keys or an oauth token (`rest.py:353-364`).
- Built-in retry: up to `DEFAULT_RETRY_ATTEMPTS = 3` retries, a fixed `DEFAULT_RETRY_WAIT_SECONDS = 3` sleep between them, and only on `DEFAULT_RETRY_EXCEPTION_CODES = [429, 504]` (`common/constants.py:11-13`; loop `rest.py:127-135, 200-202`). `TradingClient` doesn't expose the retry kwargs (`client.py:74-86`). The retry is synchronous and blocks with `time.sleep`.

### Methods (`trading/client.py`)
| signature | returns | line |
|---|---|---|
| `submit_order(order_data: OrderRequest)` | `Order` (POST /orders) | 90 |
| `get_orders(filter: Optional[GetOrdersRequest] = None)` | `List[Order]` | 107 |
| `get_order_by_id(order_id: Union[UUID, str], filter: Optional[GetOrderByIdRequest] = None)` | `Order` | 132 |
| `get_order_by_client_id(client_id: str)` | `Order` (GET /orders:by_client_order_id) | 157 |
| `cancel_order_by_id(order_id: Union[UUID, str])` | `None` (DELETE /orders/{id}) | 217 |
| `cancel_orders()` | `List[CancelOrderResponse]` (`id`, `status: int`, `body`) | 203 |
| `get_account()` | `TradeAccount` | 465 |
| `get_all_positions()` | `List[Position]` | 235 |
| `get_asset(symbol_or_asset_id: Union[UUID, str])` | `Asset` | 399 |

- `order_id` strings must be valid UUIDs. Otherwise `UUID(...)` raises `ValueError` locally before any request is sent (`common/utils.py:76-77`).
- `get_asset` puts the symbol into the path unescaped: `self.get(f"/assets/{symbol_or_asset_id}")` (`client.py:413`). With `"BTC/USD"` the path becomes `/assets/BTC/USD`. Use `"BTCUSD"` or the asset UUID to be safe `# TODO verify` (whether the slash form works).
- `TradeAccount` money fields are `Optional[str]`, e.g. `cash`, `buying_power`, `non_marginable_buying_power`, `equity`, `portfolio_value`. `crypto_status: Optional[AccountStatus]` (`models.py:529-561`). Crypto buys are checked against `non_marginable_buying_power` (docs).
- `Position`: `symbol`, `qty: str`, `qty_available: Optional[str]`, `avg_entry_price: str`, `side: PositionSide`, `market_value`, etc. (`models.py:132-152`).

### `GetOrdersRequest` (`trading/requests.py:198-219`). All fields default to `None`.
| field | type | notes |
|---|---|---|
| status | Optional[QueryOrderStatus] | open/closed/all, server default open |
| limit | Optional[int] | default 50, **max 500** (docstring `requests.py:203`) |
| after | Optional[datetime] | sent as `isoformat()` (`common/requests.py:49-54`) |
| until | Optional[datetime] | |
| direction | Optional[Sort] | `Sort.ASC="asc"`, `Sort.DESC="desc"` (`common/enums.py:36-38`); default desc |
| nested | Optional[bool] | roll multi-leg orders into `legs` |
| side | Optional[OrderSide] | |
| symbols | Optional[List[str]] | client joins them with "," (`client.py:122-123`) |

`GetOrderByIdRequest(nested: bool)` (`requests.py:222-229`).

### Order requests (`trading/requests.py:333-552`)
`OrderRequest` fields (`requests.py:357-369`): `symbol: Optional[str]`, `qty: Optional[float]`, `notional: Optional[float]`, `side: Optional[OrderSide]`, `type: OrderType` (set automatically by the subclass), `time_in_force: TimeInForce` (**required**), `order_class: Optional[OrderClass]`, `extended_hours: Optional[bool]`, `client_order_id: Optional[str]`, `legs`, `take_profit`, `stop_loss`, `position_intent: Optional[PositionIntent]`.
- Validator (`requests.py:371-410`): exactly one of `qty` / `notional` ("At least one of qty or notional must be provided" / "Both qty and notional can not be set."). `symbol` and `side` are required for non-mleg orders. Violations raise pydantic `ValidationError` at construction.
- `MarketOrderRequest(**data)` forces `type=MARKET` and rejects `limit_price`/`stop_price`/`trail_*` (`requests.py:436-468`).
- `LimitOrderRequest` adds `limit_price: Optional[float] = None`, which is required unless `order_class == OCO` (`requests.py:509-552`).
- `qty`/`notional`/`limit_price` are **floats** and go into the JSON body as floats. Round them to `min_trade_increment`/`price_increment` yourself before building the request.
- Crypto: TIF **`gtc` or `ioc` only** (`trading/enums.py:235`, docs). Types market, limit, stop_limit (`enums.py:129`, docs). Order class simple only (`enums.py:111`). No shorting or margin. Max **$200k notional per order** (docs).

Example:
```python
MarketOrderRequest(symbol="BTC/USD", notional=25, side=OrderSide.BUY,
                   time_in_force=TimeInForce.GTC, client_order_id="bot-...")
LimitOrderRequest(symbol="BTC/USD", qty=0.001, limit_price=60000, side=OrderSide.SELL,
                  time_in_force=TimeInForce.IOC)
```

### Enums (`trading/enums.py`, all `(str, Enum)`)
- `OrderSide` (139): `BUY="buy"`, `SELL="sell"`.
- `TimeInForce` (228): `DAY="day"`, `GTC="gtc"`, `OPG="opg"`, `CLS="cls"`, `IOC="ioc"`, `FOK="fok"`.
- `OrderType` (121): `MARKET="market"`, `LIMIT="limit"`, `STOP="stop"`, `STOP_LIMIT="stop_limit"`, `TRAILING_STOP="trailing_stop"`.
- `OrderStatus` (148): `new, partially_filled, filled, done_for_day, canceled, expired, replaced, pending_cancel, pending_replace, pending_review, accepted, pending_new, accepted_for_bidding, stopped, rejected, suspended, calculated, held` (18 values; member name = upper-case value).
- `QueryOrderStatus` (336): `OPEN="open"`, `CLOSED="closed"`, `ALL="all"`.
- `OrderClass` (103): `simple, mleg, bracket, oco, oto`. `AssetClass` (175): `us_equity, us_option, crypto, crypto_perp`.

### `Order` model (`trading/models.py:168-266`, extends `ModelWithID` → `id: UUID`)
| field | type |
|---|---|
| id | UUID |
| client_order_id | str |
| created_at, updated_at, submitted_at | datetime |
| filled_at, expired_at, expires_at, canceled_at, failed_at, replaced_at | Optional[datetime] |
| replaced_by, replaces, asset_id | Optional[UUID] |
| symbol | Optional[str] |
| asset_class | Optional[AssetClass] |
| notional | Optional[str] |
| qty, filled_qty, filled_avg_price, limit_price, stop_price, ratio_qty | Optional[Union[str, float]] |
| order_class | OrderClass (empty or missing → SIMPLE, line 249-250) |
| order_type, type | Optional[OrderType] |
| side | Optional[OrderSide] |
| time_in_force | TimeInForce |
| status | OrderStatus |
| extended_hours | bool |
| legs | Optional[List[Order]] |
| trail_percent, trail_price, hwm | Optional[str] |
| position_intent | Optional[PositionIntent] |

Numeric fields come from the API as **JSON strings** (e.g. `"filled_qty": "1790.86"`, docs example). Pydantic keeps them as `str` under `Union[str, float]`, so convert with `Decimal(order.filled_qty)`. `filled_avg_price` may be `None` before any fill.
In Python mode, enum fields are enum members (`str` subclasses). `order.model_dump(mode="json")` serialises them to their **lowercase string values** (`"buy"`, `"filled"`, `"gtc"`), with UUIDs → str and datetimes → ISO strings (standard pydantic v2 behaviour on these `str` Enums).

### `Asset` (`trading/models.py:32-70`)
`id: UUID`, `asset_class: AssetClass` (alias `"class"`), `exchange: AssetExchange`, `symbol: str`, `name: Optional[str]`, `status: AssetStatus`, `tradable: bool`, `marginable: bool`, `shortable: bool`, `easy_to_borrow: bool`, `fractionable: bool`, `min_order_size: Optional[float]`, `min_trade_increment: Optional[float]`, `price_increment: Optional[float]`, `maintenance_margin_requirement: Optional[float]`, `attributes: Optional[List[str]]`.
BTC/USD per docs (sample Assets object, crypto-trading page): `min_order_size = 0.0001`, `min_trade_increment = 0.0001`, `price_increment = 1`. These can change, so read them at startup with `get_asset`. `tradable`/`fractionable` for BTC/USD are expected to be `True` `# TODO verify`.

### Errors: `alpaca.common.exceptions.APIError` (`common/exceptions.py:4-39`)
Raised for any non-2xx response that is not being retried: `raise APIError(response.text, http_error)` (`rest.py:205-207`).
- `.status_code` → `http_error.response.status_code` (int) (`exceptions.py:25-29`).
- `.code` → `json.loads(body)["code"]` (Alpaca's numeric error code). `.message` → `json.loads(body)["message"]` (`exceptions.py:15-23`). **Both raise `JSONDecodeError`/`KeyError` if the body isn't Alpaca JSON** (e.g. an HTML 502). Guard them, or use `str(e)` for the raw body.
- `.response` / `.request` give the `requests` objects (`exceptions.py:31-39`).
- Classify by `e.status_code`: 404 = order or asset not found. 422 = validation (bad qty, TIF, unprocessable). 403 = forbidden, e.g. insufficient buying power `# TODO verify`. 429 = rate limited, seen only after the SDK's 3 retries × 3 s are exhausted. 5xx = server error (504 is retried by the SDK first).
- Client-side validation failures raise pydantic `ValidationError` / `ValueError` and never `APIError`.
- Duplicate `client_order_id`: expected HTTP 422 with a "client_order_id must be unique" message `# TODO verify`. Max length 128 chars `# TODO verify`.

### Rate limit
200 requests/min per API key (per task brief; not on the fetched doc pages) `# TODO verify`. The SDK's only throttling is the 429 retry described above.

## Part C -- Trading websocket (`alpaca.trading.stream.TradingStream`) and TradeUpdate

### Constructor (`trading/stream.py:28-62`)
```python
TradingStream(api_key: str, secret_key: str, paper: bool = True, raw_data: bool = False,
              url_override: str = None, websocket_params: Optional[Dict] = None)
```
- URLs: paper `wss://paper-api.alpaca.markets/stream`, live `wss://api.alpaca.markets/stream` (`common/enums.py:15-16`, matches docs).
- The protocol is JSON (`json.dumps`/`json.loads`, `stream.py:100, 179`). The docs say the paper trade stream uses **binary** frames, and `json.loads` accepts bytes.
- Auth sent by the SDK is `{"action":"authenticate","data":{"key_id":..,"secret_key":..}}`. It requires `msg["data"]["status"] == "authorized"`, else `ValueError("failed to authenticate")` (`stream.py:98-113`). The docs show the newer `{"action":"auth","key","secret"}` form. The server accepts both `# TODO verify`.
- Listen: `{"action":"listen","data":{"streams":["trade_updates"]}}` (`stream.py:144`). This is sent on every (re)connect (`stream.py:168`).

### API
- `subscribe_trade_updates(handler: Callable)` (`stream.py:147-162`) takes an async handler with one arg: a `TradeUpdate`, or the full raw dict `{"stream":..,"data":..}` if `raw_data`. There is a single handler slot, and a later call replaces it. While running it does `run_coroutine_threadsafe(...).result()`, so it must not be called from the loop thread.
- `run()` is **sync**: `asyncio.run(self._run_forever())`, catching only `KeyboardInterrupt` (`stream.py:267-273`). Unlike DataStream it does **not** call `stop()` in `finally`.
- `async _run_forever()` (`stream.py:189`). Await it inside your own loop. It waits (polling every 0.1 s) until a handler is set (`stream.py:192-196`).
- `async close()` (`stream.py:243`). `async stop_ws()` (`stream.py:254`) is the in-loop way to stop. `stop()` is sync and cross-thread only: `run_coroutine_threadsafe(self.stop_ws(), self._loop).result()` **with no timeout** (`stream.py:262-265`), so calling it on the loop thread deadlocks forever.

### Reconnect / ping (`stream.py:189-241`)
- Same defaults as the data stream: backoff 1.0–30.0 s (`stream.py:53-54`), and `{"ping_interval": 10, "ping_timeout": 180, "max_queue": 1024}` (`stream.py:55-59`), replaced wholesale by `websocket_params` (`stream.py:61-62`). Same jittered `reconnect_delay` (`stream.py:64-75`).
- `websockets.WebSocketException` → close, increment `retries` (reset to 0 first if a `trade_updates` message was received on that connection), log a warning, back off (`stream.py:215-226`).
- Any other `Exception` → `log.exception`. If not yet running (connect/auth failed), close the half-open socket, increment `retries` and back off. The SDK comment explains why:
  ```python
  # Close any half-open socket from a failed connect/auth so
  # abandoned sessions do not keep consuming the
  # single-connection slot and cause HTTP 429s.          (stream.py:232-234; same in data/live/websocket.py:483-485)
  ```
- **Handler raises**: the exception propagates out of `_consume` (`stream.py:170-187`) into `except Exception` with `_running=True`. It is logged and the SDK resumes reading on the same socket with no backoff. **The update is dropped.** No re-delivery is known `# TODO verify`. Reconcile via REST `get_orders` after reconnects/errors.
- **It never gives up.** Unlike the data stream there is no terminal condition, and a bad key retries forever with backoff up to 30 s.
- One connection per account: the SDK comments refer to a "single-connection slot" and HTTP 429s from abandoned sessions (`stream.py:232-234`, `common/utils.py:30-32`). A second concurrent `TradingStream` with the same keys will be rejected or will kick the first one `# TODO verify` (exact server behaviour). Run exactly one per process/account.

### `TradeUpdate` (`trading/models.py:633-646`)
| field | type |
|---|---|
| event | Union[TradeEvent, str] |
| execution_id | Optional[UUID] |
| order | Order (full model above) |
| timestamp | datetime |
| position_qty | Optional[float] |
| price | Optional[float] |
| qty | Optional[float] |

`price`, `qty` and `position_qty` arrive as JSON strings (docs example `"price": "105.8988475"`) and are coerced to **float** here, unlike `Order`. Use `order.filled_qty`/`filled_avg_price` (strings) for exact accounting. They are present on `fill`/`partial_fill` (docs).

`TradeEvent` (`trading/enums.py:321-333`), all 12 values: `ACCEPTED="accepted"`, `CANCELED="canceled"`, `EXPIRED="expired"`, `FILL="fill"`, `NEW="new"`, `PARTIAL_FILL="partial_fill"`, `PENDING_CANCEL="pending_cancel"`, `PENDING_NEW="pending_new"`, `PENDING_REPLACE="pending_replace"`, `REJECTED="rejected"`, `REPLACED="replaced"`, `RESTATED="restated"`.
The docs list more events that are **not** in the enum: `done_for_day`, `stopped`, `calculated`, `suspended`, `order_replace_rejected`, `order_cancel_rejected`. These arrive as plain `str` because of the `Union`. Compare by value (`update.event == "fill"` works for both, since the enum subclasses str).

## Gotchas
1. The SDK auto-retries **POST /orders** on 429/504 (`rest.py:201`). A 504 retry can double-submit, so always set a unique `client_order_id` and look it up with `get_order_by_client_id` before resubmitting.
2. Handler exceptions in both streams are logged and swallowed, and the message is lost (`websocket.py:476-490`, `stream.py:227-239`). Wrap handlers in try/except.
3. `stop()` is sync and cross-thread only. Inside the loop, use `await stream.stop_ws()`. `TradingStream.stop()` has no timeout (`stream.py:265`).
4. `websocket_params` replaces the ping/queue defaults wholesale (`websocket.py:127-128`, `stream.py:61-62`).
5. Orderbook: on `reset=True`, rebuild the book; on deltas, upsert levels and delete them when `size == 0`.
6. `Order` numerics are strings and `TradeUpdate.price/qty` are floats. Request `qty`/`notional` are floats, so pre-round to `min_trade_increment` (BTC 0.0001) and `price_increment` (BTC 1).
7. `APIError.code`/`.message` call `json.loads` and can raise on non-JSON bodies. Branch on `.status_code` first.
8. Crypto TIF is `gtc`/`ioc` only. There is no `day`, no shorting/margin, and a $200k notional cap per order.
9. `TradeEvent` lacks several documented events. Treat `event` as a string.
10. One trading-stream connection per account. The data stream gives up permanently on an "insufficient subscription" error (`websocket.py:477-480`).

## Unverified
- `Trade.exchange`, `Quote.bid_exchange`/`ask_exchange` are `None` for crypto.
- A fresh `reset=True` orderbook snapshot arrives after every SDK reconnect.
- Stream timestamps are tz-aware UTC datetimes.
- Raw JSON shape of `/v1beta3/crypto/us/latest/quotes` (`{"quotes": {...}}`).
- `get_asset("BTC/USD")` works despite the unescaped slash in the path (vs `"BTCUSD"`).
- BTC/USD `tradable`/`fractionable` are `True`.
- HTTP 403 for insufficient buying power.
- Duplicate `client_order_id` → 422 "client_order_id must be unique".
- `client_order_id` max length 128.
- Rate limit 200 requests/min per key.
- The server accepts the SDK's legacy `"authenticate"`/`key_id` trading-stream auth form.
- Trade updates dropped by a failing handler are not re-delivered.
- Exact server behaviour on a second concurrent trading-stream connection.
