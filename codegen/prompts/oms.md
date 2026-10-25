You are generating: oms.py — the Order Management System worker: the FRONT DOOR for every order intent and the
single source of truth for order STATE. It makes NO Alpaca order calls (no submit_order / cancel_order_by_id, ever);
execution is delegated to EMS over `alpaca:ems:requests` and the EMS reply is relayed to the caller.
Read-only Alpaca REST (`trader.open_orders`, `trader.list_orders`, `trader.get_order_by_client_id`) is allowed for reconciliation.

RESPONSIBILITIES
1. Consume `alpaca:trade_updates` (group `oms`, consumer env `OMS_CONSUMER` default `oms-1`) → SQLite (`orders`, `fills`, `trade_events`), idempotently. This stream carries BOTH the exchange's TradingStream events (via trade_stream.py) and EMS execution reports (`ems_submitted`, `ems_cancel_requested`).
2. Consume `alpaca:oms:commands` (group `oms`, same consumer name): kinds `submit`, `cancel`, `algo_create`, `algo_cancel`. Validate → audit in `commands` → idempotency → forward to EMS → persist the outcome → reply via `runtime.send_reply`.
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
  client_order_id TEXT, order_id TEXT, algo_id TEXT, slice_idx INTEGER, source TEXT,
  payload_json TEXT NOT NULL, status TEXT NOT NULL, error TEXT, received_at TEXT, completed_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cmd_coid ON commands(client_order_id) WHERE kind='submit';
```
`commands.status`: `received | acked | already_submitted | rejected | unknown | error | cancel_requested | not_found`.

FUNCTIONS (exact names; pure where possible so tests can drive them with fakeredis + a temp DB)
- `upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None, event_ts=None)`: maps an Alpaca order dict (`id, client_order_id, symbol, side, type/order_type, time_in_force, qty, limit_price, filled_qty, filled_avg_price, status, submitted_at, created_at, updated_at`) → INSERT … ON CONFLICT(order_id) DO UPDATE with: `filled_qty = max(excluded, orders.filled_qty)`, `filled_avg_price = COALESCE(excluded, orders.)`, `limit_price = COALESCE(excluded, orders.)`, `status = CASE WHEN orders.status IN (terminal set) AND excluded.status NOT IN (terminal set) THEN orders.status ELSE excluded.status END`, `source/algo_id/slice_idx = COALESCE(orders.x, excluded.x)`, `updated_at`, `last_event_ts`, `raw_json`. Numeric strings → float. If `client_order_id` matches `algo-<id12>-<idx>` and algo_id/slice_idx were not given, store the 12-char prefix as `algo_id` and idx as `slice_idx` (EMS tolerates prefix match).
- `handle_payload(conn, stream_id: str, payload: dict) -> None`: INSERT OR IGNORE `trade_events(stream_id, …)`; if an `order` dict is present → `upsert_order(conn, order, account=…, source=payload.get("source"), algo_id=payload.get("algo_id"), slice_idx=payload.get("slice_idx"), event_ts=ts)`; if `event in ("fill","partial_fill")` and `execution_id` → INSERT OR IGNORE `fills`. Execution-report events (`ems_submitted`, `ems_cancel_requested`) therefore just create/refresh the orders row. No commit inside (caller commits).
- `forward_to_ems(r, payload: dict, timeout: float = 10.0) -> dict`: `runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, payload, timeout)` (a thin wrapper so tests can patch one name). Payload gets its own fresh `cmd_id` (`runtime.new_id()`), never the caller's.
- `handle_command(conn, r, payload: dict, stream_id: str | None = None) -> dict` (returns the reply, also sends it with `runtime.send_reply(r, cmd_id, reply)` when `r` is not None). Common prologue: validate `kind`; `account` in {paper, live} (default paper); INSERT commands(status='received', payload_json, received_at) — if the `cmd_id` already exists, return its recorded outcome (replay safety). Common epilogue: UPDATE commands(status, error, order_id/algo_id, completed_at); `conn.commit()` (handle_command is its own unit of work).
  * `submit`: validate side/type/qty>0 (limit requires limit_price>0), `tif` default "gtc" (gtc|ioc), `client_order_id` present and `^[A-Za-z0-9_-]{1,48}$`. Idempotency: if `orders` has this client_order_id, or `commands` has a submit with it in status acked/already_submitted/unknown → reply `already_submitted` with the existing order row (dict) and do NOT forward. If `runtime.worker_alive(r, "ems")` is None → status `error`, `error:"EMS not running"`, ok False, do not forward. Else forward `{kind:"submit", account, client_order_id, symbol, side, type, qty, limit_price, tif, source}` to EMS. Map the EMS reply: acked / already_submitted → `upsert_order(conn, reply["order"], account=…, source=payload["source"])`, commands status = reply status, order_id; rejected / error → store error; `unknown` → status unknown (ok False; reconcile will pick the order up by client_order_id later); EMS reply `timeout` (no EMS answer within the timeout) → status `unknown`, error "EMS did not reply". Reply to the caller with `{ok, status, cmd_id, order_id, client_order_id, order, error}` (the original cmd_id, not the forwarded one).
  * `cancel`: resolve `order_id` from `client_order_id` via `orders` when needed; if the order is unknown to SQLite AND no order_id was given → `not_found` (no forward). Else (EMS alive) forward `{kind:"cancel_order", account, order_id, client_order_id, source}`; relay status (`cancel_requested` / `not_found` / `error`); on `cancel_requested` set the local `orders.status = 'pending_cancel'` only if the current status is non-terminal.
  * `algo_create`: validate `strategy` is a str, side, total_qty>0 (detailed validation is EMS's job); forward `{kind:"create", strategy, account, symbol, side, total_qty, duration_s, n_slices, limit_cap, params, source}`; record `algo_id` from the reply in `commands.algo_id`; relay `{ok, status, cmd_id, algo_id, algo, error}`.
  * `algo_cancel`: require `algo_id`; forward `{kind:"cancel", algo_id, source}`; relay.
  * Any exception while forwarding → status `error` with the text; the caller must always get a reply.
- `reconcile(conn, r, accounts=("paper",), since_iso: str | None = None) -> int`: for each account, `trader.open_orders(live)` + `trader.list_orders("closed", after=since_iso or 24h ago, live)` → `upsert_order(...)` each; commit; `SET alpaca:oms:last_reconcile now_iso()`; return count. Catch `APIError`/transient errors, log, return 0 (reconcile must never crash the worker). Accounts: `paper` always; `live` only if env `OMS_RECONCILE_LIVE=1`.
- `process_updates(conn, reader) -> int` and `process_commands(conn, r, reader) -> int`: read a batch; per entry try/commit/ack, except rollback + `reader.dead_letter` (and for commands, also send an `error` reply so the caller does not hang).
- `body()` (sync): `r = runtime.get_redis()`, `conn = runtime.sqlite_connect()`, `setup_db`, two `GroupReader`s (block_ms=500 each), `reconcile(...)` at start, then loop: process_updates, process_commands, reconcile when due; `time.sleep(0.05)` if both batches were empty.
- `main()`: `runtime.run_forever(body, "oms")`. `if __name__ == "__main__": main()`.
Do NOT import or call `trader.submit_limit`, `trader.submit_market`, `trader.cancel`, `trader.cancel_all` anywhere in this file. Call `mcp__verify__verify_stage(stage="oms")`.
