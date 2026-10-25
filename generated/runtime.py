"""Shared resilience library for the Alpaca crypto pipeline workers.

Resilience model:
- Every worker runs under ``run_forever`` (a supervisor): its body is restarted with
  jittered exponential backoff (1s -> 60s) on any exit or exception; clients are
  re-created inside the body on every incarnation. SIGTERM/SIGINT exit cleanly.
- A heartbeat thread writes ``alpaca:heartbeat:<name>`` (SET .. EX 15) every 5s so
  other processes can check liveness via ``worker_alive``.
- Redis writes go through ``redis_call`` which retries transient errors and can spill
  to an in-memory FIFO ``SpillBuffer`` (replayed in order once Redis is back).
- Redis Streams + consumer groups (``GroupReader``) give at-least-once delivery:
  own pending entries are drained first, stale ones XAUTOCLAIMed, failures dead-lettered.
- SQLite runs in WAL mode with a busy timeout; all writes are idempotent so replay is safe.
- Nothing here performs I/O at import time.
"""
from __future__ import annotations

import asyncio
import collections
import inspect
import json
import logging
import os
import random
import secrets
import signal
import socket
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
DB_PATH = os.environ.get("ALPACA_DB_PATH") or str(Path(__file__).resolve().parent.parent / "alpaca.db")
STREAM_TRADE_UPDATES = "alpaca:trade_updates"
STREAM_TRADE_UPDATES_DLQ = "alpaca:trade_updates:dlq"
STREAM_OMS_COMMANDS = "alpaca:oms:commands"
STREAM_EMS_REQUESTS = "alpaca:ems:requests"
TERMINAL_STATUSES = frozenset({"filled", "canceled", "expired", "rejected", "replaced"})
ACTIVE_STATUSES = frozenset({"new", "accepted", "pending_new", "partially_filled", "accepted_for_bidding",
                             "pending_cancel", "pending_replace", "calculated", "held"})

_TRANSIENT = (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
              redis.exceptions.BusyLoadingError)
_log_lock = threading.Lock()
_log_configured = False


def log(name: str) -> logging.Logger:
    global _log_configured
    with _log_lock:
        if not _log_configured:
            level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
            logging.basicConfig(level=level, stream=sys.stderr,
                                format="%(asctime)s %(levelname)s %(name)s %(message)s")
            _log_configured = True
    return logging.getLogger(name)


_rlog = logging.getLogger("runtime")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def utc_ts() -> float:
    return time.time()


def get_redis(url: str | None = None) -> redis.Redis:
    return redis.Redis.from_url(url or REDIS_URL, decode_responses=True, socket_timeout=5,
                                socket_connect_timeout=5, health_check_interval=30,
                                retry_on_timeout=True)


class SpillBuffer:
    """Bounded FIFO of deferred Redis calls, replayed once Redis is reachable."""

    def __init__(self, maxlen: int = 10000):
        self.maxlen = maxlen
        self._q: collections.deque = collections.deque()
        self._lock = threading.Lock()
        self.dropped = 0

    def add(self, fn, *args, **kwargs) -> None:
        with self._lock:
            if len(self._q) >= self.maxlen:
                self._q.popleft()
                self.dropped += 1
            self._q.append((fn, args, kwargs))

    def flush(self) -> int:
        n = 0
        while True:
            with self._lock:
                if not self._q:
                    return n
                fn, args, kwargs = self._q[0]
            try:
                fn(*args, **kwargs)
            except Exception:
                return n
            with self._lock:
                if self._q and self._q[0][0] is fn:
                    self._q.popleft()
            n += 1

    def __len__(self) -> int:
        return len(self._q)


def redis_call(fn, *args, retries: int = 3, backoff: float = 0.2,
               buffer: SpillBuffer | None = None, **kwargs):
    attempt = 0
    while True:
        try:
            if buffer is not None and len(buffer):
                try:
                    buffer.flush()
                except Exception:
                    pass
                if len(buffer):
                    # still failing to flush: preserve ordering by queueing behind
                    raise redis.exceptions.ConnectionError("spill buffer not drained")
            return fn(*args, **kwargs)
        except _TRANSIENT:
            attempt += 1
            if attempt > retries:
                if buffer is not None:
                    buffer.add(fn, *args, **kwargs)
                    return None
                raise
            time.sleep(backoff * (2 ** (attempt - 1)))


def heartbeat_key(name: str) -> str:
    return f"alpaca:heartbeat:{name}"


class Heartbeat:
    def __init__(self, r, name: str, interval: float = 5.0, ttl: int = 15):
        self.r, self.name, self.interval, self.ttl = r, name, interval, ttl
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def beat(self) -> bool:
        try:
            payload = json.dumps({"ts": now_iso(), "pid": os.getpid(), "host": socket.gethostname()})
            self.r.set(heartbeat_key(self.name), payload, ex=self.ttl)
            return True
        except Exception:
            return False

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.beat()
            self._stop.wait(self.interval)

    def start(self) -> threading.Thread:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name=f"heartbeat-{self.name}", daemon=True)
        self._thread.start()
        return self._thread

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)


def worker_alive(r, name: str) -> dict | None:
    try:
        raw = r.get(heartbeat_key(name))
        if not raw:
            return None
        d = json.loads(raw)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def run_forever(body, name: str, *, min_backoff: float = 1.0, max_backoff: float = 60.0,
                heartbeat: bool = True, stop_event: threading.Event | None = None) -> None:
    """Supervisor: run ``body`` forever, restarting with jittered exponential backoff."""
    lg = log(name)
    stop_event = stop_event or threading.Event()
    prev_handlers = {}

    def _handler(signum, frame):
        lg.info("signal %s received; stopping", signum)
        stop_event.set()
        raise KeyboardInterrupt

    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                prev_handlers[sig] = signal.signal(sig, _handler)
            except (ValueError, OSError):
                pass

    backoff = min_backoff
    try:
        while not stop_event.is_set():
            hb = None
            if heartbeat:
                try:
                    hb = Heartbeat(get_redis(), name)
                    hb.start()
                except Exception:
                    lg.exception("heartbeat start failed")
                    hb = None
            started = time.monotonic()
            try:
                if inspect.iscoroutinefunction(body):
                    asyncio.run(body())
                else:
                    res = body()
                    if inspect.iscoroutine(res):
                        asyncio.run(res)
                lg.warning("worker body returned; restarting")
            except (KeyboardInterrupt, SystemExit):
                lg.info("worker %s exiting", name)
                stop_event.set()
                return
            except Exception:
                lg.exception("worker body crashed")
            finally:
                if hb is not None:
                    hb.stop()
            if stop_event.is_set():
                break
            if time.monotonic() - started > 60:
                backoff = min_backoff
            delay = min(max_backoff, backoff) * random.uniform(0.8, 1.2)
            lg.info("restarting %s in %.1fs", name, delay)
            backoff = min(max_backoff, backoff * 2)
            try:
                stop_event.wait(delay)
            except (KeyboardInterrupt, SystemExit):
                stop_event.set()
                return
    except (KeyboardInterrupt, SystemExit):
        stop_event.set()
    finally:
        for sig, h in prev_handlers.items():
            try:
                signal.signal(sig, h)
            except Exception:
                pass


def sqlite_connect(path: str | None = None) -> sqlite3.Connection:
    p = path or DB_PATH
    if p != ":memory:":
        Path(p).resolve().parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p, timeout=5, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_group(r, stream: str, group: str, start_id: str = "0") -> None:
    try:
        r.xgroup_create(stream, group, id=start_id, mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _flatten_xread(resp) -> list:
    out = []
    if not resp:
        return out
    items = list(resp.items()) if isinstance(resp, dict) else resp
    for item in items:
        entries = item[1]
        # RESP3 wraps the entry list in an extra list
        if isinstance(resp, dict) and len(entries) == 1 and entries[0] \
                and isinstance(entries[0][0], (list, tuple)):
            entries = entries[0]
        for e in entries or []:
            eid, fields = e[0], e[1]
            if fields is None:
                continue
            out.append((eid, dict(fields)))
    return out


class GroupReader:
    def __init__(self, r, stream: str, group: str, consumer: str, *, count: int = 64,
                 block_ms: int = 1000, claim_idle_ms: int = 60000, claim_every_s: float = 30.0):
        self.r, self.stream, self.group, self.consumer = r, stream, group, consumer
        self.count, self.block_ms = count, block_ms
        self.claim_idle_ms, self.claim_every_s = claim_idle_ms, claim_every_s
        self._draining = True
        self._last_claim = time.monotonic()
        ensure_group(r, stream, group)

    def read(self) -> list:
        if self._draining:
            entries = _flatten_xread(self.r.xreadgroup(self.group, self.consumer, {self.stream: "0"},
                                                       count=self.count))
            if entries:
                return entries
            self._draining = False
        if time.monotonic() - self._last_claim >= self.claim_every_s:
            self._last_claim = time.monotonic()
            resp = self.r.xautoclaim(self.stream, self.group, self.consumer,
                                     min_idle_time=self.claim_idle_ms, start_id="0-0", count=self.count)
            msgs = resp[1] if resp and len(resp) >= 2 else []
            claimed = [(e[0], dict(e[1])) for e in msgs or [] if e and len(e) > 1 and e[1] is not None]
            if claimed:
                return claimed
        return _flatten_xread(self.r.xreadgroup(self.group, self.consumer, {self.stream: ">"},
                                                count=self.count, block=self.block_ms))

    def ack(self, entry_id: str) -> None:
        self.r.xack(self.stream, self.group, entry_id)

    def dead_letter(self, entry_id: str, fields: dict, error: str) -> None:
        payload = {**(fields or {}), "error": str(error)[:500], "src_id": entry_id,
                   "consumer": self.consumer}
        self.r.xadd(f"{self.stream}:dlq", {k: str(v) for k, v in payload.items() if v is not None})
        self.ack(entry_id)


def new_id() -> str:
    return uuid.uuid4().hex


def new_client_order_id(prefix: str = "tc") -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


def reply_key(cmd_id: str) -> str:
    return f"alpaca:reply:{cmd_id}"


def send_command(r, stream: str, payload: dict, timeout: float = 10.0) -> dict:
    if not payload.get("cmd_id"):
        payload["cmd_id"] = payload.get("req_id") or new_id()
    r.xadd(stream, {"json": json.dumps(payload, default=str)}, maxlen=100000, approximate=True)
    return wait_reply(r, payload["cmd_id"], timeout)


def wait_reply(r, cmd_id: str, timeout: float = 10.0) -> dict:
    timeout_reply = {"ok": False, "status": "timeout", "cmd_id": cmd_id,
                     "error": f"no reply within {timeout}s"}
    res = r.blpop(reply_key(cmd_id), timeout=timeout)
    if not res:
        return timeout_reply
    try:
        d = json.loads(res[1])
        return d if isinstance(d, dict) else timeout_reply
    except (ValueError, TypeError):
        return {"ok": False, "status": "error", "cmd_id": cmd_id, "error": "unparseable reply"}


def send_reply(r, cmd_id: str, reply: dict) -> None:
    key = reply_key(cmd_id)
    pipe = r.pipeline()
    pipe.rpush(key, json.dumps(reply, default=str))
    pipe.expire(key, 60)
    pipe.execute()
