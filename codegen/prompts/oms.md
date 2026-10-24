You are generating: oms.py — the Order Management System worker (replaces the old persister).
It is the single source of truth for order state and the ONLY process that calls Alpaca order endpoints.

RESPONSIBILITIES
1. Consume `alpaca:trade_updates` (group `oms`, consumer env `OMS_CONSUMER` default `oms-1`) → SQLite (`orders`, `fills`, `trade_events`), idempotently.
2. Consume `alpaca:oms:commands` (group `oms`, same consumer name) → submit/cancel via trader.py; reply via `runtime.send_reply`; audit in `commands`.
3. Reconcile with Alpaca REST on startup and every `OMS_RECONCILE_S` (default 30) seconds; write `alpaca:oms:last_reconcile` (ISO) to Redis.
4. Heartbeat `alpaca:heartbeat:oms` (done by `run_forever`).

SCHEMA — define `SCHEMA: str` (executescript) and `setup_db(conn)` that runs it (idempotent, CREATE IF NOT EXISTS):
```sql
CREATE TABLE IF NOT EXISTS orders(
  order_id TEXT PRIMARY KEY, client_order_id TEXT UNIQUE, account TEXT NOT NULL DEFAULT 'paper',
  symbol TEXT, side TEXT, type TEXT, time_in_force TEXT, qty REAL, limit_price REAL,
  filled_qty REAL DEFAULT 0, filled_avg_price REAL, status TEXT, source TEXT,
  algo_id TEXT, slice_idx INTEGER, submitted_at TEXT, created_at TEXT, updated_at TEXT,
  last_event_ts TEXT, raw_json TEXT);
CREATE INDEX IF NOT EXISTS idx_orders_algo ON orders(algo_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_submitted ON orders(submitted_at);
CREATE TABLE IF NOT EXISTS fills(
  id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT UNIQUE, order_id TEXT, account TEXT,
  symbol TEXT, side TEXT, price REAL, qty REAL, event TEXT, ts TEXT, algo_id TEXT, raw_json TEXT);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_ts ON fills(ts);
CREATE TABLE IF NOT EXISTS trade_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id TEXT UNIQUE, event TEXT, order_id TEXT,
  account TEXT, ts TEXT, raw_json TEXT);
CREATE TABLE IF NOT EXISTS commands(
  cmd_id TEXT PRIMARY KEY, stream_id TEXT, kind TEXT NOT NULL, account TEXT NOT NULL,
  client_order_id TEXT, order_id TEXT, source TEXT, algo_id TEXT, slice_idx INTEGER,
  payload_json TEXT NOT NULL, status TEXT NOT NULL, error TEXT, received_at TEXT, completed_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cmd_coid ON commands(client_order_id) WHERE kind='submit';
```

FUNCTIONS (exact names; pure where possible so tests can drive them with fakeredis + a temp DB)
- `upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None, event_ts=None)`: maps an Alpaca order dict (`id, client_order_id, symbol, side, type/order_type, time_in_force, qty, limit_price, filled_qty, filled_avg_price, status, submitted_at, created_at, updated_at`) → INSERT … ON CONFLICT(order_id) DO UPDATE with: `filled_qty = max(excluded, orders.filled_qty)`, `filled_avg_price = COALESCE(excluded, orders.)`, `limit_price = COALESCE(excluded, orders.)`, `status = CASE WHEN orders.status IN (terminal set) AND excluded.status NOT IN (terminal set) THEN orders.status ELSE excluded.status END`, `source/algo_id/slice_idx = COALESCE(orders.x, excluded.x)`, `updated_at`, `last_event_ts`, `raw_json`. Numeric strings → float. If `client_order_id` is an EMS id (`algo-<id12>-<idx>`), derive `algo_id`/`slice_idx` when not given (match EMS rows by prefix on `algo_orders.algo_id LIKE '<id12>%'` is NOT needed; just store the 12-char prefix as `algo_id` if unknown — EMS tolerates prefix match).
- `handle_payload(conn, stream_id: str, payload: dict) -> None`: INSERT OR IGNORE `trade_events(stream_id, …)`; if an order dict present → `upsert_order(...)`; if `event in ("fill","partial_fill")` and `execution_id` → INSERT OR IGNORE `fills`. No commit inside (caller commits).
- `handle_command(conn, r, payload: dict, stream_id: str | None = None) -> dict` (returns the reply, also sends it with `runtime.send_reply` when `r` is not None):
  * validate kind/account/side/type/qty>0 (limit requires limit_price>0); `tif` default "gtc".
  * submit: if `commands` already has this `client_order_id` (kind submit) or `orders` has it → status `already_submitted`, reply with the existing order row (dict). Else INSERT commands(status='received'); call `trader.submit_limit/submit_market(..., live=(account=="live"))`; on success `upsert_order(conn, order, account=..., source=..., algo_id=..., slice_idx=...)`, update commands status='acked', order_id; reply `{ok: True, status: "acked", order: …}`. On `APIError`: if 422 and 'client_order_id' in message → look up `trader.get_order_by_client_id`; if found treat as acked/already_submitted; else status `rejected` with error text. If `trader.is_transient(exc)`: retry up to 3× with 0.5/1/2 s; after that, try `get_order_by_client_id`; if found → acked, else status `unknown` (ok False) with error. Any other exception → `error`.
  * cancel: by `order_id` or `client_order_id` (resolve via SQLite); call `trader.cancel`; status `cancel_requested` (ok True) or `not_found`/`error`.
  * Always `conn.commit()` at the end of handle_command (it is its own unit of work) and record `completed_at`.
- `reconcile(conn, r, accounts=("paper",), since_iso: str | None = None) -> int`: for each account, `trader.open_orders(live)` + `trader.list_orders("closed", after=since_iso or 24h ago, live)` → `upsert_order(...)` each; commit; `SET alpaca:oms:last_reconcile now_iso()`; return count. Catch `APIError`/transient errors, log, return 0 (reconcile must never crash the worker). Accounts to reconcile: `paper` always; `live` only if env `OMS_RECONCILE_LIVE=1`.
- `process_updates(conn, reader) -> int` and `process_commands(conn, r, reader) -> int`: read a batch; per entry try/commit/ack, except rollback + `reader.dead_letter` (and for commands, also send an `error` reply so the caller does not hang).
- `body()` (sync): `r = runtime.get_redis()`, `conn = runtime.sqlite_connect()`, `setup_db`, two `GroupReader`s (block_ms=500 each), `reconcile(...)` at start, then loop: process_updates, process_commands, reconcile when due; `time.sleep(0.05)` if both batches were empty.
- `main()`: `runtime.run_forever(body, "oms")`. `if __name__ == "__main__": main()`.
Call `mcp__verify__verify_stage(stage="oms")`.
