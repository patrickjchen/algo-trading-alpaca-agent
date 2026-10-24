You are generating: runtime.py — the shared resilience library every worker imports.
Keep it ≤ 350 lines, stdlib + `redis` only (no alpaca import here). Pure
functions and small classes; nothing executes at import except constant
definitions. All names below are REQUIRED (the verifier checks them).

```python
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")           # read at import is OK (no I/O)
DB_PATH   = os.environ.get("ALPACA_DB_PATH") or str(Path(__file__).resolve().parent.parent / "alpaca.db")
STREAM_TRADE_UPDATES = "alpaca:trade_updates"; STREAM_TRADE_UPDATES_DLQ = "alpaca:trade_updates:dlq"
STREAM_OMS_COMMANDS = "alpaca:oms:commands";   STREAM_EMS_REQUESTS = "alpaca:ems:requests"
TERMINAL_STATUSES = frozenset({"filled", "canceled", "expired", "rejected", "replaced"})
ACTIVE_STATUSES   = frozenset({"new", "accepted", "pending_new", "partially_filled", "accepted_for_bidding",
                               "pending_cancel", "pending_replace", "calculated", "held"})
```

Functions / classes (exact signatures):
- `log(name: str) -> logging.Logger` — configures root logging once (idempotent) with format `%(asctime)s %(levelname)s %(name)s %(message)s`, level from `LOG_LEVEL` env (default INFO), stream=stderr; returns `logging.getLogger(name)`.
- `now_iso() -> str` (UTC, isoformat, timespec="milliseconds"), `utc_ts() -> float` (epoch seconds).
- `get_redis(url: str | None = None) -> redis.Redis` — `redis.Redis.from_url(url or REDIS_URL, decode_responses=True, socket_timeout=5, socket_connect_timeout=5, health_check_interval=30, retry_on_timeout=True)`. Lazy; no ping.
- `class SpillBuffer` — `__init__(self, maxlen=10000)`, `add(self, fn, *args, **kwargs)`, `flush(self) -> int` (replays FIFO, stops at the first failure keeping the rest, returns count flushed), `__len__`, `dropped` counter (incremented when maxlen evicts).
- `redis_call(fn, *args, retries: int = 3, backoff: float = 0.2, buffer: SpillBuffer | None = None, **kwargs)` — calls `fn(*args, **kwargs)`; on `redis.exceptions.ConnectionError`, `TimeoutError`, `BusyLoadingError` retries with exponential backoff; after the retries, if `buffer` is given → `buffer.add(fn, *args, **kwargs)` and return None, else re-raise. Any other exception propagates. On success, if `buffer` is non-empty, call `buffer.flush()` first (best effort) so queued writes land in order.
- `heartbeat_key(name) -> f"alpaca:heartbeat:{name}"`.
- `class Heartbeat` — `__init__(self, r, name, interval: float = 5.0, ttl: int = 15)`; `beat(self) -> bool` writes `SET key json({"ts": now_iso(), "pid": os.getpid(), "host": socket.gethostname()}) EX ttl`, swallowing redis errors (returns False); `start(self) -> threading.Thread` starts a daemon thread beating every `interval` until `stop()` is called; `stop(self)`.
- `worker_alive(r, name) -> dict | None` — GET + json.loads, None if missing/unparseable/redis error.
- `run_forever(body, name: str, *, min_backoff: float = 1.0, max_backoff: float = 60.0, heartbeat: bool = True, stop_event: threading.Event | None = None) -> None` — the supervisor. Installs SIGTERM/SIGINT handlers that set `stop_event`. Loop: start a Heartbeat (fresh `get_redis()`), call `body()` (if `inspect.iscoroutinefunction(body)`: `asyncio.run(body())`), on normal return or exception log (`exception` level with traceback) and sleep `min(max_backoff, backoff) * uniform(0.8, 1.2)`, doubling backoff; reset backoff to `min_backoff` when the previous incarnation lived > 60s. Exit cleanly on stop_event / KeyboardInterrupt / SystemExit. Stop the heartbeat thread on each restart.
- `sqlite_connect(path: str | None = None) -> sqlite3.Connection` — mkdir parent; `sqlite3.connect(path or DB_PATH, timeout=5, check_same_thread=False)`; PRAGMA `journal_mode=WAL`, `busy_timeout=5000`, `synchronous=NORMAL`, `foreign_keys=ON`; `row_factory=sqlite3.Row`. Default isolation level (so `commit()`/`rollback()` work per entry).
- `ensure_group(r, stream, group, start_id: str = "0") -> None` — `XGROUP CREATE … MKSTREAM`, ignore `BUSYGROUP`.
- `class GroupReader` — `__init__(self, r, stream, group, consumer, *, count=64, block_ms=1000, claim_idle_ms=60000, claim_every_s=30.0)`; calls `ensure_group`. `read(self) -> list[tuple[str, dict]]`: phase 1 drains this consumer's own pending entries (`XREADGROUP … id="0"`, no block) until empty; then every `claim_every_s` runs `XAUTOCLAIM(stream, group, consumer, min_idle_time=claim_idle_ms, start_id="0-0", count=count)` and returns claimed entries (handle both 2- and 3-element replies, skip deleted ids); otherwise `XREADGROUP … id=">" block=block_ms`. `ack(self, entry_id)`. `dead_letter(self, entry_id, fields: dict, error: str)`: `XADD f"{stream}:dlq" {**fields, "error": error[:500], "src_id": entry_id, "consumer": consumer}` then ack. Redis errors propagate (the supervisor restarts the worker).
- `new_id() -> str` (uuid4 hex), `new_client_order_id(prefix="tc") -> f"{prefix}-{secrets.token_hex(6)}"`.
- `reply_key(cmd_id) -> f"alpaca:reply:{cmd_id}"`.
- `send_command(r, stream, payload: dict, timeout: float = 10.0) -> dict` — ensures `payload["cmd_id"]` (reuse `req_id` if present, else `new_id()`), `XADD stream {"json": json.dumps(payload, default=str)} maxlen=100000 approximate=True`, then `return wait_reply(r, payload["cmd_id"], timeout)`.
- `wait_reply(r, cmd_id, timeout: float = 10.0) -> dict` — `BLPOP reply_key(cmd_id) timeout`; returns parsed dict, or `{"ok": False, "status": "timeout", "cmd_id": cmd_id, "error": f"no reply within {timeout}s"}`.
- `send_reply(r, cmd_id, reply: dict) -> None` — `RPUSH` json + `EXPIRE 60` (pipeline).

Docstring at top explaining the resilience model in ~10 lines. Then call `mcp__verify__verify_stage(stage="runtime")`.
