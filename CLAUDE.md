# CLAUDE.md

## What this repo is

`claude_codegen.py` (package `codegen/`) is a **Claude Agent SDK** multi-agent generator that
produces the Alpaca crypto paper-trading pipeline under `generated/`. The generated files are
committed but are LLM output; regenerate with the codegen. It is the successor of
`../TradingWithAlpaca` (AG2/Gemini); the pipeline there had no fault tolerance, no OMS and no algos.

Roles are separate `query()` calls chosen deterministically by `codegen/driver.py`:
Researcher → `generated/RESEARCH.md` (reads the installed alpaca-py source); Coder per stage
(Write/Edit fenced to the stage's files by a PreToolUse hook; must call the in-process MCP tool
`mcp__verify__verify_stage`); the harness re-runs the same verifier and retries ≤5× via
`resume=`; Tester triages pytest failures to stages (JSON schema output). Pass/fail is always
the harness's own `codegen/verify.py` result, never an agent's claim.

Prompts live in `codegen/prompts/*.md`; `coder_preamble.md` carries the **durable rules** and the
**cross-module contract** (public names, Redis stream payloads, reply shapes). When the LLM
regresses on something, add a rule there rather than hand-fixing output twice.
`codegen/stages.py` is the registry: targets (write-allowed globs), priors, `must_define`
names, Flask routes / HTML substrings the verifier checks.

## Running

```bash
pip install -r requirements.txt -r requirements-codegen.txt
export ANTHROPIC_API_KEY=sk-ant-...        # REQUIRED: Console key; subscription login is not allowed for SDK apps
python claude_codegen.py                   # generate missing stages, then run the pytest gate
python claude_codegen.py --stage all --skip-tests
python claude_codegen.py --stage oms ems   # specific stages
python claude_codegen.py --fix err.txt --stage ems      # feed a runtime traceback back (also `--fix -` for stdin)
python claude_codegen.py --only-tests      # just the gate (unit; plus live when creds+Redis present)
python -m pytest -q tests_codegen          # offline tests of the harness itself (no LLM)
```
Costs: Opus 5.5, ≤$6 per agent call by default (`--budget`), ~12 stages per full run.
Logs: `.codegen_logs/run-*.jsonl`; last-good backups `.codegen_backup/`; failed attempts `.codegen_failed/`.

Runtime (paper by default): `cp run.bash.example run.bash`, fill keys, `source run.bash`,
start Redis, `scripts/run_all.sh` (7 processes: subscriber, trade_stream, oms_ems, crossing, algo_engine, persister, webgui;
UI on http://127.0.0.1:5000). `cd generated && python cli.py health|symbols|buy|twap ...`.
Only one TradingStream per Alpaca account: do not run `../TradingWithAlpaca` workers concurrently.

## Architecture (what the generated code must satisfy)

- **Hot path = Redis only; SQLite is written by one worker.** OMS, EMS and the algo engine keep
  their operational state in Redis hashes/sets and never open SQLite. `persister.py` is the only SQLite
  writer: consumer groups on `alpaca:trade_updates`, `alpaca:oms:events` and `alpaca:algo:events`
  feed `orders, fills, trade_events, commands, algo_orders, algo_slices` with batched, idempotent
  upserts. It owns the schema (`persister.SCHEMA/setup_db`). webgui and cli read SQLite read-only
  for history and Redis for live state.
- **OMS → crossing → EMS → exchange.** OMS (`oms.py`) is the front door and owns the order *lifecycle*:
  every intent (`submit`, `cancel`, `algo_create`, `algo_cancel`) arrives on `alpaca:oms:commands` and
  is answered on `alpaca:reply:<cmd_id>` (`runtime.send_command`). OMS validates, replays by `cmd_id`
  (`alpaca:cmd:<id>`), dedupes by `client_order_id` (`alpaca:coid:<id>`), forwards execution to the liquidity book on
  `alpaca:cross:requests` or algos to the algo engine on `alpaca:algo:requests`, relays the reply, and
  maintains the live order hashes `alpaca:order:<id>` + `alpaca:orders:open` from EMS replies,
  `alpaca:trade_updates` and REST reconciliation (every 30 s). It emits order/command snapshots to
  `alpaca:oms:events` for the persister. OMS makes no Alpaca order calls.
- **Crossing / central liquidity book** (`crossing.py`) nets offsetting internal interest before
  anything is routed: every intent carries a `book` (portfolio id); opposite-side orders on the same
  symbol and account whose prices cross are matched internally at the midpoint (both legs avoid the
  exchange fee and half the spread). Limit orders rest for `CROSS_HOLD_S` (default 1 s, per-order
  `hold_s`) waiting for a counterparty; market/IOC/hold-0 orders cross what they can and route at once.
  Each intent gets an internal record `int-<client_order_id>` (venue `internal`) with events
  `internal_accepted/internal_fill/internal_routed/internal_canceled/internal_rejected` on
  `alpaca:trade_updates`; the residual is routed to EMS with the SAME client_order_id, so one intent
  can have two order records. Resting book in `alpaca:book:*`; savings tally in `alpaca:cross:stats`
  (`FEE_BPS`, default 25). crossing.py is the only writer of `alpaca:ems:requests`.
- **EMS** (`ems.py`) is the exchange gateway and the only caller of Alpaca order endpoints
  (`execute_submit` / `execute_cancel` via trader.py). After every submit or cancel it publishes an
  execution report (`ems_submitted` / `ems_cancel_requested`) onto `alpaca:trade_updates`. No algos.
- **Algo engine** (`algo_engine.py`) is the strategy service: it takes `create`/`cancel` from OMS,
  builds the schedule with `algos.py` (TWAP real; VWAP/IS stubs), keeps `alpaca:algo:<id>`,
  `alpaca:algo:<id>:slices`, `alpaca:algos:active` in Redis, submits every child THROUGH OMS
  (`source: "algo"`), tracks children via the OMS order hashes, emits snapshots to `alpaca:algo:events`,
  and reloads running algos from SQLite once at cold start. TWAP children are limit-at-touch GTC,
  rolled forward at the next slice, final slice IOC crossing by `cross_bps`, clamped to `limit_cap`.
- **runtime.py** is the shared resilience layer: `run_forever` supervisor + heartbeat
  (`alpaca:heartbeat:<worker>`, EX 15), `redis_call` with `SpillBuffer`, `GroupReader`,
  `sqlite_connect` (WAL). Stream handlers never raise (alpaca-py drops the event otherwise).
- Import-time purity: no network/Redis/SQLite/env at import; the verifier imports every module
  with no creds and an unreachable `REDIS_URL`.
- Tests: `generated/tests/test_unit_*` (fakeredis, tmp SQLite) always; `test_live_*` (marker
  `live`) auto-skip without creds+Redis, reuse running workers by heartbeat, place only
  far-from-market paper limits and cancel in `finally`; TWAP end-to-end needs `--run-twap`.
