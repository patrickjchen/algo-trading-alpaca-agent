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
start Redis, `scripts/run_all.sh` (4 processes: subscriber, trade_stream, oms_ems, webgui;
UI on http://127.0.0.1:5000). `cd generated && python cli.py health|symbols|buy|twap ...`.
Only one TradingStream per Alpaca account: do not run `../TradingWithAlpaca` workers concurrently.

## Architecture (what the generated code must satisfy)

- **OMS** (`oms.py`) is the only caller of Alpaca order endpoints and owns `orders, fills,
  trade_events, commands`. Everyone submits via the Redis Stream `alpaca:oms:commands` and waits
  on `alpaca:reply:<cmd_id>` (`runtime.send_command`). Idempotent on `client_order_id`.
  Consumer-group reads drain the pending list first and XAUTOCLAIM stale entries; failures go to
  `alpaca:trade_updates:dlq`. REST reconciliation at start and every 30 s.
- **oms_ems** (`oms_ems.py`) hosts `oms.body` and `ems.body` as two supervised daemon threads in one
  process (each via `runtime.run_forever`, so both `alpaca:heartbeat:oms` and `:ems` are still written).
  `oms.py` / `ems.py` remain runnable standalone; `run_all.sh` launches `oms_ems` instead of both.
- **EMS** (`ems.py`) owns `algo_orders, algo_slices`; schedules are persisted so a restart resumes.
  TWAP children are limit-at-touch GTC, rolled forward at the next slice, final slice IOC crossing
  by `cross_bps`, always clamped to `limit_cap`. VWAP/IS are registered stubs (GUI shows them disabled).
- **runtime.py** is the shared resilience layer: `run_forever` supervisor + heartbeat
  (`alpaca:heartbeat:<worker>`, EX 15), `redis_call` with `SpillBuffer`, `GroupReader`,
  `sqlite_connect` (WAL). Stream handlers never raise (alpaca-py drops the event otherwise).
- Import-time purity: no network/Redis/SQLite/env at import; the verifier imports every module
  with no creds and an unreachable `REDIS_URL`.
- Tests: `generated/tests/test_unit_*` (fakeredis, tmp SQLite) always; `test_live_*` (marker
  `live`) auto-skip without creds+Redis, reuse running workers by heartbeat, place only
  far-from-market paper limits and cancel in `finally`; TWAP end-to-end needs `--run-twap`.
