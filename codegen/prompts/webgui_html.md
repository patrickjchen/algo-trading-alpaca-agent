You are a senior frontend engineer generating: web/index.html — ONE self-contained page (inline CSS + vanilla JS; no CDN, no `<script src>`, no frameworks). First line `<!DOCTYPE html>`. Dark theme. Must work when Redis is empty, SQLite missing, or workers are down (placeholders, never JS exceptions on null/[]). ≤ 900 lines. No emojis.
If web/index.html already exists it is shown to you as EXISTING: keep it and apply the smallest Edit(s) needed to satisfy this brief (today that is the dynamic health strip and the wording above); otherwise write the file with ONE Write call. Then call `mcp__verify__verify_stage(stage="webgui_html")`.

API CONTRACT (webgui.py implements exactly this):
- `GET /api/health` → `{ok: bool, workers: {<worker name>: {alive: bool, age_s: number|null}, ...}, redis: bool, db: bool, last_reconcile: str|null}` — worker names currently `subscriber, trade_stream, oms, ems, algo_engine, persister, webgui`, but RENDER WHATEVER KEYS THE RESPONSE CONTAINS (do not hardcode the list).
- `GET /api/symbols` → `string[]`
- `GET /api/orderbook/<sym>` → `{symbol, timestamp?, bids:[{price,size}], asks:[{price,size}], fallback?}` (bids best-first, asks best-first, ≤10 each)
- `GET /api/latest/<sym>` → `{price?, bid_price?, ask_price?, ...}`
- `GET /api/orders?limit=N&status=all|open|closed&account=paper|live` → rows: `order_id, client_order_id, account, symbol, side, type, qty, limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx, submitted_at`
- `GET /api/fills?limit=N` → rows: `ts, order_id, symbol, side, price, qty, event, algo_id`
- `GET /api/summary?live=0|1` → `{account, positions, realized_pnl_by_symbol: {SYM: number}, degraded?: bool}`
- `POST /api/order` body `{symbol, qty, side, type:"market"|"limit", limit_price?, tif?:"gtc"|"ioc", live:bool}` → OMS reply `{ok, status, order?, error?}` (HTTP 200 if ok, 400 bad input, 503 if OMS down, 502 if rejected)
- `DELETE /api/order/<order_id>?live=0|1` → `{ok, status, error?}`
- `GET /api/algos/strategies` → `[{id, name, available, note, params_schema}]`
- `POST /api/algo` body `{strategy, symbol, side, total_qty, duration_s, n_slices, limit_cap?, params?, live}` → `{ok, algo_id, status, error?}` (503 if OMS down; 502 with `error` if the algo engine rejected or is down)
- `GET /api/algos?status=running|all` → `[{algo_id, strategy, account, symbol, side, total_qty, n_slices, start_ts, end_ts, limit_cap, status, filled_qty, avg_px, error, created_at, slices_done, slices_total}]`
- `GET /api/algo/<algo_id>` → `{algo: {...}, slices: [{slice_idx, scheduled_ts, qty, client_order_id, order_id, limit_price, status, filled_qty}], orders: [...child order rows...]}`
- `DELETE /api/algo/<algo_id>` → `{ok, status, error?}`
Symbols contain `/` → always `encodeURIComponent(sym)` in URLs.

LAYOUT
- Header: title "Trading With Claude", a Symbol `<select id="global-symbol">` (from /api/symbols; placeholder option "(no symbols streaming)"), a Refresh button, and a HEALTH STRIP: one pill per worker key returned by /api/health (iterate `Object.keys(workers)` in order; show the name, green when alive else red with "down"), plus redis/db dots; poll /api/health every 5 s.
- Tabs (buttons `id="btn-<name>"` → panels `id="tab-<name>"`), names exactly: `escalator`, `activity`, `summary`, `algos`. `switchTab(name)` toggles `.active`, clears the single poll interval and starts the tab's own polling.

Tab escalator (polls /api/orderbook/<sym> every 1000 ms): 21-row ladder — 10 ask rows (highest at top, best ask just above the divider), a spread row ("spread <px> (<bps> bps)"), 10 bid rows (best bid under the divider). One price per row; pad missing levels with empty non-clickable rows; never render undefined/null/NaN. Asks red-tinted, bids green-tinted. Click ask row → order modal as BUY at that price; click bid → SELL. Meta line under the ladder: symbol, timestamp, fallback tag if present.

Tab activity: Refresh button, status filter (all/open/closed), account filter (paper/live). Orders table (poll every 3 s while the tab is active): submitted_at, order_id (8 chars, full in title), account, symbol, side, type, Limit ($), qty, filled_qty, Avg Px ($), Notional ($), status, source, algo (algo_id 8 chars + slice_idx, blank otherwise), action (Cancel button for active statuses `new, accepted, pending_new, partially_filled, pending_cancel, pending_replace`; DELETE /api/order/<id>?live= from the ROW's account). Fills table below: ts, order_id, symbol, side, qty, Price ($), Notional ($), event, algo. 2-decimal formatting; blanks for null.

Tab summary: Live toggle (default off), Refresh. Account grid (equity, cash, buying_power, portfolio_value), positions table (symbol, qty, avg_entry_price, current_price, market_value, unrealized_pl, unrealized_plpc), realized PnL per symbol with total row. If `degraded` show a muted note "account data unavailable (no credentials or Alpaca unreachable)".

Tab algos (poll /api/algos every 2 s): a "New algo" form: strategy `<select id="algo-strategy">` filled from /api/algos/strategies (unavailable ones rendered `disabled` with their note), symbol (defaults to the header symbol), side (buy/sell), total qty, duration (seconds, default 300), slices (default 5), limit cap (optional), live checkbox (confirm() before submitting live). Submit → POST /api/algo; toast the reply. Table of algos: created_at, algo_id (8), strategy, account, symbol, side, progress bar `filled_qty/total_qty` with percentage text, slices_done/slices_total, avg_px, status (colour-coded), Cancel button (DELETE /api/algo/<id>) for pending/running. Clicking a row expands (fetch /api/algo/<id>) into a slices table (idx, scheduled_ts, qty, limit_price, status, filled_qty, order_id 8) under the row.

ORDER MODAL (absolutely positioned div, not `<dialog>`): fields Symbol (readonly), Side (readonly), Type radios `market | limit | algo` (default limit; limit shows a Limit price prefilled from the clicked row; **algo** switches the modal to the TWAP mini-form: duration s, slices, limit cap prefilled with the clicked price), Qty (number, step any, > 0), TIF select (gtc/ioc; hidden for algo), Live checkbox (confirm() when checked). Submit → POST /api/order (market/limit) or POST /api/algo (strategy "twap"); on success close + toast + refresh Activity/Algos. Esc closes, Enter submits. Toasts bottom-right, success/error colours. All DOM text set via textContent (no innerHTML with data).
