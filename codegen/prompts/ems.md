You are generating: ems.py — the Execution Management System worker: the ONLY process that calls Alpaca order
endpoints (through trader.py), and the algo engine (TWAP). It serves requests that OMS forwards on
`alpaca:ems:requests` and reports every execution back to OMS by XADDing an EXECUTION REPORT to `alpaca:trade_updates`.
It never writes the OMS tables (`orders`, `fills`, `trade_events`, `commands`); it may READ `orders`/`fills`.

SCHEMA — `SCHEMA: str`, `setup_db(conn)` (idempotent):
```sql
CREATE TABLE IF NOT EXISTS algo_orders(
  algo_id TEXT PRIMARY KEY, strategy TEXT NOT NULL, account TEXT NOT NULL DEFAULT 'paper',
  symbol TEXT NOT NULL, side TEXT NOT NULL, total_qty REAL NOT NULL, n_slices INTEGER NOT NULL,
  start_ts TEXT NOT NULL, end_ts TEXT NOT NULL, limit_cap REAL, params_json TEXT, source TEXT,
  status TEXT NOT NULL, filled_qty REAL DEFAULT 0, avg_px REAL, error TEXT,
  created_at TEXT, updated_at TEXT);
CREATE INDEX IF NOT EXISTS idx_algo_status ON algo_orders(status);
CREATE TABLE IF NOT EXISTS algo_slices(
  algo_id TEXT NOT NULL, slice_idx INTEGER NOT NULL, scheduled_ts TEXT NOT NULL, qty REAL NOT NULL,
  client_order_id TEXT, order_id TEXT, limit_price REAL, status TEXT NOT NULL,
  filled_qty REAL DEFAULT 0, updated_at TEXT, PRIMARY KEY(algo_id, slice_idx));
```
Statuses: algo `pending|running|completed|partial|cancelled|failed`; slice `scheduled|submitted|working|filled|rolled|cancelled|failed`.

EXECUTION GATEWAY (exact names)
- `publish_report(r, event: str, order: dict | None, *, account, client_order_id=None, order_id=None, symbol=None, side=None, qty=None, source=None, algo_id=None, slice_idx=None) -> None`: XADD to `runtime.STREAM_TRADE_UPDATES` `{"json": json.dumps(payload, default=str)}` (maxlen=100000, approximate) via `runtime.redis_call`, where payload = `{event, execution_id: None, order_id, client_order_id, symbol, side, price: None, qty, position_qty: None, ts: runtime.now_iso(), account, order, source, algo_id, slice_idx}` (fill order_id/client_order_id/symbol/side/qty from `order` when not given). Never raises (log on failure).
- `execute_submit(conn, r, p: dict) -> dict`: p = `{client_order_id, account, symbol, side, type, qty, limit_price, tif, source, algo_id?, slice_idx?}`. Validate (side, type, qty>0, limit needs limit_price>0, tif gtc|ioc, client_order_id `^[A-Za-z0-9_-]{1,48}$`) → `rejected` on failure. `live = account == "live"`. Call `trader.submit_limit(symbol, qty, side, limit_price, client_order_id, live=live, tif=tif)` or `trader.submit_market(...)`. On success → `publish_report(r, "ems_submitted", order, account=…, source=…, algo_id=…, slice_idx=…)` and return `{ok True, status:"acked", order_id, client_order_id, order}`. On `APIError`: if status 422 and 'client_order_id' in the message → `trader.get_order_by_client_id(client_order_id, live)`; found → publish report + `already_submitted` (ok True); else `rejected` with the error text. If `trader.is_transient(exc)`: retry up to 3× (0.5/1/2 s); after that look up by client_order_id; found → report + acked; else `{ok False, status:"unknown", error}`. Any other exception → `{ok False, status:"error", error}`.
- `execute_cancel(conn, r, p: dict) -> dict`: resolve `order_id` from `client_order_id` via the `orders` table (read-only) or `trader.get_order_by_client_id` when the row is missing; no order → `not_found`. `trader.cancel(order_id, live)` → `publish_report(r, "ems_cancel_requested", order_row_or_None, account=…, order_id=…, client_order_id=…, source=…)` and `{ok True, status:"cancel_requested", order_id}`; APIError 404/422 "not found"/already terminal → `not_found`; other → `error`.
- `handle_request(conn, r, payload: dict) -> dict` (sends the reply with `runtime.send_reply(r, payload["cmd_id"], reply)` when r is given; also accept legacy `req_id`; returns the reply):
  * `submit` → `execute_submit`; `cancel_order` → `execute_cancel` (reply shapes above plus `cmd_id`).
  * `create`: validate (`strategy` in `algos.REGISTRY` and available → else `ok False, error`), side, total_qty>0, duration_s ≥ 10*n_slices (default duration 300, n_slices 5), account. `rules = algos.rules_from_dict(trader.get_asset_rules(symbol, live, r=r))` (catch errors → rules with Nones). `start = utc_ts()+2`, `end = start+duration_s`; `slices = algos.build_schedule(...)`; `algo_id = runtime.new_id()`; INSERT algo_orders(status 'pending', source) + algo_slices(status 'scheduled', `client_order_id = f"algo-{algo_id[:12]}-{idx:03d}"`), commit; reply `{ok True, status, algo_id, algo: row dict}`.
  * `cancel`: mark algo `cancelled`; for slices with status submitted/working → `execute_cancel` by client_order_id (mark `cancelled` on cancel_requested/not_found); slices scheduled → `cancelled`; commit; reply `{ok, status, algo_id, algo}`.
  * unknown kind → ok False.

ALGO ENGINE (exact names; keep I/O injectable for tests)
- `refresh_from_oms(conn, algo_id) -> None`: read `orders` rows WHERE `client_order_id IN (slice ids)` (fall back to `algo_id LIKE '<algo_id[:12]>%'`); update slice `order_id, filled_qty, status` (filled when order status filled; working when active; failed when rejected/expired; `canceled` after OUR roll/cancel is `rolled`/`cancelled` — keep the slice's existing rolled/cancelled status); recompute algo `filled_qty = sum(slice filled_qty)` and `avg_px` from `fills` rows for those order ids (qty-weighted); commit.
- `tick(conn, r, now_ts: float | None = None, allow_submit: bool = True) -> int`: for each algo with status pending/running:
  1. `refresh_from_oms`.
  2. If status pending and now ≥ first scheduled_ts → running.
  3. For each slice with status `scheduled` and `scheduled_ts ≤ now` (only when `allow_submit`): `remaining_roll` = sum over earlier slices of (qty - filled_qty) that are `working`/`submitted`: `execute_cancel` each (mark `rolled`), add their unfilled qty to this slice (persist new qty); `quote = trader.latest_quote(symbol, r=r)`, `px = strategy.child_price(side, quote, limit_cap, final=(idx == last), rules)`, persist `limit_price`; then `execute_submit(conn, r, {client_order_id: slice id, account, symbol, side, type:"limit", qty, limit_price: px, tif: "ioc" if final else "gtc", source:"ems", algo_id, slice_idx})`: acked/already_submitted → slice `submitted` (+order_id); rejected/error → slice `failed` and algo `failed` with error (stop); `unknown` → leave `submitted`; the next refresh / `trader.get_order_by_client_id` (only if the orders row is still missing 10 s later) resolves it.
  4. When all slices terminal: `completed` if filled_qty ≥ total_qty*(1-1e-9) else `partial` (if any fill) else `failed`; if the final IOC slice left unfilled qty and now > end_ts + 5 → finalise as `partial`.
  Commit per algo. Return number of algos touched. Never let one algo's exception kill the loop: catch, log, mark that algo `failed` with the error text.
- `body()`: `r = runtime.get_redis()`, `conn = runtime.sqlite_connect()`, `setup_db`, `GroupReader(r, STREAM_EMS_REQUESTS, "ems", env EMS_CONSUMER default "ems-1", block_ms=500)`; loop: read requests (try/commit/ack, except rollback/dead_letter + error reply), then `tick(conn, r, allow_submit=oms_up)` where `oms_up = runtime.worker_alive(r, "oms") is not None` (child state is tracked through the OMS-written `orders` table, so do not start new children while OMS is down; warn once per 30 s); sleep so the loop runs about once per second.
- `main()`: `runtime.run_forever(body, "ems")`; `if __name__ == "__main__": main()`.
Restart safety: everything the engine needs is in algo_orders/algo_slices; on restart, `tick` simply resumes. Never send anything to `runtime.STREAM_OMS_COMMANDS` from this file. Call `mcp__verify__verify_stage(stage="ems")`.
