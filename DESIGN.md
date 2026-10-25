# Design

## 1. Codegen (Claude Agent SDK)

```
claude_codegen.py ─► codegen/driver.py
   │  check_auth()  (ANTHROPIC_API_KEY)
   │  research?  ──► query(Researcher)  tools: Read/Glob/Grep(site-packages alpaca), WebFetch(3 URLs), Write(RESEARCH.md)
   │  for stage in STAGES:
   │      backup(stage)
   │      attempt 1..5:  query(Coder, system=coder_preamble+stage.md, prompt=brief+priors+task[+feedback])
   │                      tools: Read/Glob/Grep/Write/Edit (hook: writes fenced to stage.targets)
   │                             mcp__verify__verify_stage  (same code as the harness check)
   │                      harness: verify_stage()  → OK ? next stage : feedback, resume session (fresh after 3)
   │      on exhaustion: restore last-good, dump attempt to .codegen_failed/
   │  test gate (≤3 rounds): pytest -m "not live" (sandbox copy) [+ pytest -m live if creds+Redis]
   │                          fail → query(Tester, output_format=json_schema) → {failures:[{stage,traceback}]}
   │                                 → run_stage(stage, feedback) for ≤2 stages → repeat
```

Verifier (`codegen/verify.py`): copies `generated/` into a temp dir, strips creds from the env,
points `REDIS_URL` at a closed port, then `py_compile` → `import` → `must_define` names →
Flask `url_map` rules (webgui) / required substrings (HTML) / `pytest` (test stages).

## 2. Runtime components

```
 Alpaca crypto WS ──► subscriber.py ──► Redis: alpaca:latest:*, alpaca:ob:{bids,asks}:*, alpaca:recent:*, alpaca:symbols
 Alpaca trading WS ─► trade_stream.py ► Redis Stream alpaca:trade_updates ──► oms.py ──► SQLite orders/fills/trade_events/commands
                                              ▲                                  ▲   └──► alpaca:trade_updates:dlq
 ems.py execution reports (ems_submitted, ems_cancel_requested) ┘                  │
 webgui.py / cli.py ── XADD alpaca:oms:commands (submit/cancel/algo_create/algo_cancel) ┘   reply alpaca:reply:<cmd_id>
 oms.py ── XADD alpaca:ems:requests (submit/cancel_order/create/cancel) ──► ems.py ──► Alpaca REST (the ONLY order writer)
                                                                              └──► SQLite algo_orders/algo_slices (TWAP engine;
                                                                                   children submitted by ems.py itself)
 every worker ──► alpaca:heartbeat:<name> (EX 15) ──► webgui /api/health, cli health, live tests
 oms_ems.py ──► runs oms.body + ems.body as two supervised threads in one process (both heartbeats kept)
```

### Submit path (sequence)
1. UI POST /api/order → webgui validates, checks `worker_alive(oms)` (503 otherwise), builds
   `{kind:submit, client_order_id: tc-xxxx, …}` → XADD `alpaca:oms:commands` → BLPOP `alpaca:reply:<cmd_id>` (≤12 s).
2. OMS reads the command; if `client_order_id` already known → reply `already_submitted`.
   Else INSERT commands(received) → `trader.submit_*` → upsert orders → commands(acked) → RPUSH reply.
   Transient error → retry ×3 → `get_order_by_client_id` → acked or `unknown`. 422 duplicate → lookup → acked.
3. Alpaca pushes `new/fill/...` on the trading WS → trade_stream XADDs → OMS upserts (status never regresses
   from terminal; `trade_events.stream_id` unique so replays are no-ops) → UI Activity tab polls SQLite.

### TWAP slice lifecycle
`create` → EMS persists algo_orders(pending) + N algo_slices(scheduled, client_order_id algo-<id12>-<idx>) →
`tick` each second: due slice → cancel still-working earlier children (their unfilled qty rolls into this slice)
→ price = touch (final: cross by cross_bps), clamp to limit_cap → submit through OMS (GTC; final IOC) →
`refresh_from_oms` reads child rows from `orders`/`fills` → algo filled_qty/avg_px → all slices terminal →
completed | partial | failed. Restart: nothing is in memory; `tick` resumes from the tables.

### OMS recovery
Group `oms` on both streams. On start: XREADGROUP id `0` drains this consumer's own pending entries; every
30 s XAUTOCLAIM steals entries idle >60 s from dead consumers; then `>`. Per entry: commit → XACK;
exception → rollback → XADD `<stream>:dlq` → XACK. Reconcile: `open_orders` + `list_orders(closed, after=last)`
upserted at start and every 30 s; `alpaca:oms:last_reconcile` exposes freshness.

## 3. SQLite schema
See `codegen/prompts/oms.md` (orders, fills, trade_events, commands) and `ems.md` (algo_orders, algo_slices).
One file (`ALPACA_DB_PATH`, default `<repo>/alpaca.db`), WAL, two writer processes (OMS, EMS), readers everywhere.

## 4. Redis keys
| Key | Type | Writer → Reader |
|---|---|---|
| `alpaca:latest:<SYM>`, `alpaca:ob:bids/asks:<SYM>`, `alpaca:orderbook:<SYM>`, `alpaca:recent:<SYM>:trade/quote`, `alpaca:symbols` | Hash/List/Set | subscriber → webgui, cli, ems (quotes) |
| `alpaca:trade_updates` (+`:dlq`) | Stream | trade_stream → oms |
| `alpaca:oms:commands` | Stream (group oms) | webgui/cli/ems → oms |
| `alpaca:ems:requests` | Stream (group ems) | webgui/cli → ems |
| `alpaca:reply:<id>` | List, EX 60 | oms/ems → requester |
| `alpaca:heartbeat:<worker>` | String, EX 15 | each worker → health |
| `alpaca:asset:<SYM>` | Hash, EX 3600 | trader.get_asset_rules → ems |
| `alpaca:oms:last_reconcile` | String | oms → health |
