You are generating: ems.py — the Execution Management System worker (algo engine).
It never calls Alpaca directly: children are submitted/cancelled THROUGH OMS commands.

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

FUNCTIONS (exact names; keep I/O injectable for tests)
- `handle_request(conn, r, payload: dict) -> dict` (sends reply via `runtime.send_reply(r, payload["req_id" or "cmd_id"], reply)` when r is given; returns reply):
  * create: validate (`strategy` in `algos.REGISTRY` and available → else `ok False, error`), side, total_qty>0, duration_s ≥ 10*n_slices (default duration 300, n_slices 5), account. `rules = algos.rules_from_dict(trader.get_asset_rules(symbol, live, r=r))` (catch errors → rules with Nones). `start = utc_ts()+2`, `end = start+duration_s`; `slices = algos.build_schedule(...)`; `algo_id = runtime.new_id()`; INSERT algo_orders(status 'pending') + algo_slices(status 'scheduled', `client_order_id = f"algo-{algo_id[:12]}-{idx:03d}"`), commit; reply `{ok True, algo_id, status, algo: row dict}`.
  * cancel: mark algo `cancelled`; for slices with status submitted/working → send OMS cancel command (by client_order_id); slices scheduled → `cancelled`; commit; reply.
- `refresh_from_oms(conn, algo_id) -> None`: read `orders` rows WHERE `client_order_id IN (slice ids)` (fall back to `algo_id LIKE '<algo_id[:12]>%'`); update slice `order_id, filled_qty, status` (filled when order status filled; working when active; failed when rejected/expired/canceled-by-exchange… careful: `canceled` after OUR roll is `rolled`); recompute algo `filled_qty = sum(slice filled_qty)` and `avg_px` from `fills` rows for those order ids (qty-weighted); commit.
- `tick(conn, r, now_ts: float | None = None) -> int`: for each algo with status pending/running:
  1. `refresh_from_oms`.
  2. If status pending and now ≥ first scheduled_ts → running.
  3. For each slice with status `scheduled` and `scheduled_ts ≤ now`: compute `remaining_roll` = sum over earlier slices of (qty - filled_qty) that are `working`: send OMS cancel for those (mark `rolled` after cancel ack or on next refresh when canceled), add their unfilled qty to this slice (persist new qty), then `quote = trader.latest_quote(symbol, r=r)`, `px = strategy.child_price(side, quote, limit_cap, final=(idx == last), rules)`, `limit_price` persisted; send OMS submit command `{kind:"submit", account, client_order_id, symbol, side, type:"limit", qty, limit_price: px, tif: "ioc" if final else "gtc", source:"ems", algo_id, slice_idx}` with timeout 10; on reply acked/already_submitted → slice `submitted` (+order_id); on rejected/error → slice `failed` and algo `failed` with error (stop); on timeout/unknown → leave `submitted` with a note; the next refresh/`trader.get_order_by_client_id` (only if the row is still missing 10 s later) resolves it.
  4. When all slices terminal: `completed` if filled_qty ≥ total_qty*(1-1e-9) else `partial` (if any fill) else `failed`; if the final IOC slice left unfilled qty and now > end_ts + 5 → finalise as `partial`.
  Commit per algo. Return number of algos touched. Never let one algo's exception kill the loop: catch, log, mark that algo `failed` with the error text.
- `body()`: `r = runtime.get_redis()`, `conn = runtime.sqlite_connect()`, `setup_db`, `GroupReader(r, STREAM_EMS_REQUESTS, "ems", env EMS_CONSUMER default "ems-1", block_ms=500)`; loop: read requests (try/commit/ack, except rollback/dead_letter + error reply), `tick(conn, r)`, sleep so the loop runs about once per second. Also OMS must be alive to submit: if `runtime.worker_alive(r,"oms")` is None, skip submits this tick and log a warning (once per 30 s).
- `main()`: `runtime.run_forever(body, "ems")`; `if __name__ == "__main__": main()`.
Restart safety: everything the engine needs is in algo_orders/algo_slices; on restart, `tick` simply resumes. Call `mcp__verify__verify_stage(stage="ems")`.
