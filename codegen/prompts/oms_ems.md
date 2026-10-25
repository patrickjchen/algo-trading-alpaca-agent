You are generating: oms_ems.py — ONE process that runs the OMS and the EMS together, plus its unit test tests/test_unit_oms_ems.py.

WHY
oms.py and ems.py stay as they are (modules + standalone entry points). Operators want a single
worker process for the trading side, so this module hosts both bodies in one process. Nothing
downstream may notice: both heartbeats `alpaca:heartbeat:oms` and `alpaca:heartbeat:ems` must keep
being written, because webgui `/api/health`, cli `health`, ems' own OMS-alive check and the live tests
all look for those two names.

REQUIREMENTS — oms_ems.py
- `import runtime`, `import oms`, `import ems` (flat imports; cwd is generated/). No network/Redis/SQLite at import; module level holds constants + defs only.
- `WORKER_NAMES = ("oms", "ems")`.
- `run_oms(stop_event)` → `runtime.run_forever(oms.body, "oms", stop_event=stop_event)`;
  `run_ems(stop_event)` → `runtime.run_forever(ems.body, "ems", stop_event=stop_event)`.
  Each therefore gets its own supervisor loop, backoff and Heartbeat thread exactly as when run standalone. `run_forever` only installs signal handlers on the main thread, so inside worker threads it installs none — that is intended.
- `start_workers(stop_event, *, targets=None) -> list[threading.Thread]`: start one **daemon** thread per worker (names `worker-oms`, `worker-ems`), running `run_oms`/`run_ems` (or the `targets` mapping `{name: callable(stop_event)}` when given — the test uses this to inject fakes). Return the threads.
- `main(argv=None)`:
  1. `stop = threading.Event()`; install SIGTERM/SIGINT handlers on the main thread that log and `stop.set()` (do not raise).
  2. `threads = start_workers(stop)`; log `oms_ems started pid=... workers=oms,ems`.
  3. Supervise: loop every 0.5 s while `stop` is not set; if any worker thread is no longer alive while `stop` is not set, log an error naming it, set `stop`, and remember `exit_code = 1` (an outer supervisor such as scripts/run_all.sh restarts the whole process — both bodies are restart-safe by design).
  4. On stop: join each thread with a total grace of ~5 s. The bodies are blocking loops that cannot be interrupted from another thread, so a thread may still be alive after the grace period; that is fine because the threads are daemons — just log it and return. Return `exit_code` (0 on signal, 1 on worker death); `if __name__ == "__main__": raise SystemExit(main())`.
- Use `runtime.log("oms_ems")`. Honour nothing new from the environment: `OMS_CONSUMER`, `EMS_CONSUMER`, `OMS_RECONCILE_S` etc. keep working because the original bodies read them.

REQUIREMENTS — tests/test_unit_oms_ems.py (offline, no `live` marker, uses the existing conftest fixtures such as `fake_redis` / monkeypatching of `runtime.get_redis` as the other unit tests do — read tests/conftest.py and tests/test_unit_runtime.py first and follow their style)
- Test 1: `start_workers` with injected fake targets runs both callables in daemon threads and passes the same stop_event to each; threads are named `worker-oms` / `worker-ems`.
- Test 2: with `runtime.get_redis` patched to the fake redis, `start_workers(stop, targets={"oms": lambda ev: runtime.run_forever(<body that waits on ev>, "oms", stop_event=ev), "ems": ...})` makes BOTH `runtime.worker_alive(fake_redis, "oms")` and `runtime.worker_alive(fake_redis, "ems")` non-None within ~3 s; then `stop.set()` and the threads finish within ~5 s.
- Test 3: `main()` returns 1 when a worker thread dies: monkeypatch `oms_ems.start_workers` to start one thread that returns immediately and one that waits on the event (keep it short; the supervise loop polls at 0.5 s). Patch `signal.signal` with a no-op if the test runner is not on the main thread issue arises; prefer running `main` directly in the test's main thread.
- Keep tests fast (< 10 s total) and deterministic; no real Redis, no Alpaca.

Call `mcp__verify__verify_stage(stage="oms_ems")` and `mcp__verify__run_unit_tests()` and fix until both pass.
