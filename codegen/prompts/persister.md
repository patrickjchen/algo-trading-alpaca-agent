You are generating: persister.py — the ONLY process that writes SQLite. It drains three Redis Streams with consumer
group `persister` (consumer env `PERSISTER_CONSUMER` default `persister-1`) into `runtime.DB_PATH`, in batches, so no
hot-path worker ever blocks on disk. Everything is idempotent so replays and crashes are harmless.

SCHEMA — define `SCHEMA: str` (executescript) and `setup_db(conn)` (idempotent):
```sql
CREATE TABLE IF NOT EXISTS orders(
  order_id TEXT PRIMARY KEY, client_order_id TEXT UNIQUE, account TEXT NOT NULL DEFAULT 'paper',
  symbol TEXT, side TEXT, type TEXT, time_in_force TEXT, qty REAL, limit_price REAL,
  filled_qty REAL DEFAULT 0, filled_avg_price REAL, status TEXT, source TEXT,
  algo_id TEXT, slice_idx INTEGER, book TEXT, venue TEXT DEFAULT 'alpaca', routed_order_id TEXT, error TEXT,
  submitted_at TEXT, created_at TEXT, updated_at TEXT,
  last_event_ts TEXT, raw_json TEXT);
CREATE INDEX IF NOT EXISTS idx_orders_algo ON orders(algo_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_submitted ON orders(submitted_at);
CREATE TABLE IF NOT EXISTS fills(
  id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT UNIQUE, order_id TEXT, account TEXT,
  symbol TEXT, side TEXT, price REAL, qty REAL, event TEXT, ts TEXT, algo_id TEXT,
  book TEXT, venue TEXT DEFAULT 'alpaca', cross_id TEXT, counter_client_order_id TEXT, raw_json TEXT);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_venue ON fills(venue);
CREATE INDEX IF NOT EXISTS idx_fills_ts ON fills(ts);
CREATE TABLE IF NOT EXISTS trade_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id TEXT UNIQUE, event TEXT, order_id TEXT,
  account TEXT, ts TEXT, raw_json TEXT);
CREATE TABLE IF NOT EXISTS commands(
  cmd_id TEXT PRIMARY KEY, stream_id TEXT, kind TEXT NOT NULL, account TEXT NOT NULL,
  client_order_id TEXT, order_id TEXT, algo_id TEXT, slice_idx INTEGER, source TEXT,
  payload_json TEXT NOT NULL, status TEXT NOT NULL, error TEXT, received_at TEXT, completed_at TEXT);
CREATE TABLE IF NOT EXISTS algo_orders(
  algo_id TEXT PRIMARY KEY, strategy TEXT NOT NULL, account TEXT NOT NULL DEFAULT 'paper', book TEXT,
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

WRITE FUNCTIONS (exact names; no commit inside — the batch loop commits)
- `upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None, book=None, event_ts=None, raw=None)`: `o = runtime.normalize_order(order)`; extras from the order dict: `venue` (default 'alpaca'), `routed_order_id`, `error`, `book` (argument wins, then order dict, then DEFAULT_BOOK); INSERT … ON CONFLICT(order_id) DO UPDATE: `filled_qty = max(excluded.filled_qty, orders.filled_qty)`, `filled_avg_price = COALESCE(excluded., orders.)`, `limit_price = COALESCE(excluded., orders.)`, `status = CASE WHEN orders.status IN (<TERMINAL_STATUSES>) AND excluded.status NOT IN (…) THEN orders.status ELSE COALESCE(excluded.status, orders.status) END`, `source/algo_id/slice_idx/book = COALESCE(orders.x, excluded.x)`, `venue = COALESCE(excluded.venue, orders.venue)`, `routed_order_id/error = COALESCE(excluded.x, orders.x)`, `updated_at`, `last_event_ts`, `raw_json`. Derive algo_id/slice_idx from an `algo-<id12>-<idx>` client_order_id when not given. Skip silently when there is no order_id.
- `handle_trade_update(conn, stream_id, payload: dict)`: INSERT OR IGNORE `trade_events(stream_id, event, order_id, account, ts, raw_json)`; if `payload["order"]` is a dict → `upsert_order(conn, order, account, source=payload.get("source"), algo_id=…, slice_idx=…, event_ts=ts)`; if `event in ("fill","partial_fill","internal_fill")` and `execution_id` → INSERT OR IGNORE `fills` with `venue = payload.get("venue") or "alpaca"`, `book`, `cross_id`, `counter_client_order_id` (algo_id from the payload, the orders row or the client_order_id pattern).
- `handle_oms_event(conn, stream_id, payload: dict)`: `type == "order"` → `upsert_order(conn, payload["order"], account=payload.get("account") or order.account, source=…, algo_id=…, slice_idx=…, book=payload.get("book"), event_ts=payload.get("ts"))`; `type == "command"` → INSERT … ON CONFLICT(cmd_id) DO UPDATE SET status/error/order_id/algo_id/completed_at (payload stored as `payload_json` = json.dumps(command["payload"])); unknown type → log and ignore.
- `handle_algo_event(conn, stream_id, payload: dict)`: `type == "algo"` → upsert `algo_orders` from `payload["algo"]` (all columns; `params_json` as given), then upsert each dict in `payload.get("slices") or []`; `type == "slice"` → upsert one `algo_slices` row (ON CONFLICT(algo_id, slice_idx) DO UPDATE every column). Floats from strings with `float()`; ints with `int()`.
- Dispatch table `HANDLERS = {STREAM_TRADE_UPDATES: handle_trade_update, STREAM_OMS_EVENTS: handle_oms_event, STREAM_ALGO_EVENTS: handle_algo_event}`.
- `process_batch(conn, reader, handler) -> int`: `entries = reader.read()`; decode each `fields["json"]`; first try the WHOLE batch in one transaction (handler per entry, then one `conn.commit()`, then ack all); if anything raises → `conn.rollback()` and redo ONE BY ONE: per entry try → commit → ack; except → rollback → `reader.dead_letter(entry_id, fields, error)`. Return the number of entries acked. This is the only place `commit()` is called.
- `body()`: `r = runtime.get_redis()`; `conn = runtime.sqlite_connect()`; `setup_db(conn)`; three `GroupReader`s (group `persister`, block_ms=200) for the three streams; loop over them calling `process_batch`; `time.sleep(0.05)` if all three batches were empty; `finally: conn.close()`. Log a throttled (every 30 s) summary line: rows written per stream.
- `main()`: `runtime.run_forever(body, "persister")`; `if __name__ == "__main__": main()`.
Import only `runtime` (and stdlib). Never read or write Redis hashes; never send commands. Call `mcp__verify__verify_stage(stage="persister")`.
