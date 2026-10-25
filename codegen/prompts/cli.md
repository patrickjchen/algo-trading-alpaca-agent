You are generating: cli.py — argparse CLI, FLAT subcommands (one level), `import runtime`, `import trader`.

Subcommands (all top-level):
- Read-only Redis: `symbols`, `latest <SYM>`, `recent <SYM> [--kind trade|quote] [-n N]`, `health` (prints every `alpaca:heartbeat:*` worker with age in seconds and OK/DOWN, plus `alpaca:oms:last_reconcile`; exit 1 if any of subscriber, trade_stream, oms, ems is DOWN).
- Trading (via OMS by default; `--direct` calls trader.py directly, bypassing OMS — print a warning when used). Each accepts `--live`:
  * `account`, `positions` → trader (read-only REST, fine to call directly).
  * `orders [--status open|closed|all] [--limit N]` → SQLite `orders` (via `runtime.sqlite_connect()`, read-only) when no `--direct`; else trader.list_orders.
  * `buy <SYM> <QTY> [--limit PRICE] [--tif gtc|ioc]`, `sell …` → build a submit command `{kind:"submit", account, client_order_id: runtime.new_client_order_id("tc"), symbol, side, type, qty, limit_price, tif, source:"cli"}` and `runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload)`; print the reply as JSON; exit 1 if `ok` is false. If `runtime.worker_alive(r,"oms")` is None → print "OMS is not running (no heartbeat); start generated/oms.py or use --direct" and exit 2.
  * `cancel <ORDER_ID>`, `cancel-all` → cancel command(s) via OMS (cancel-all: iterate open orders from SQLite and send one cancel each; `--direct` → trader.cancel_all).
- Algos (via EMS): `twap <SYM> <buy|sell> <QTY> [--duration S] [--slices N] [--limit-cap PX] [--live]` → `runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, {kind:"create", strategy:"twap", account, symbol, side, total_qty, duration_s, n_slices, limit_cap, params:{}, source:"cli"})`; `algos [--status running|all]` → read `algo_orders` (+ slice counts) from SQLite; `algo <ALGO_ID>` → algo row + its `algo_slices` + child orders; `algo-cancel <ALGO_ID>` → EMS cancel request. If EMS heartbeat missing → clear message, exit 2.
- `--live` prints `WARNING: --live mode -- this places real orders` to stderr first. Output JSON via `json.dumps(indent=2, default=str)`; tables via `tabulate` are optional (stdlib fallback fine). Exit non-zero with a clear message when Redis has no data.
- `main()` only; no top-level work. Call `mcp__verify__verify_stage(stage="cli")`.
