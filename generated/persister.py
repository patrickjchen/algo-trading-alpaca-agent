"""Persister worker: the ONLY SQLite writer.

Drains ``alpaca:trade_updates``, ``alpaca:oms:events`` and ``alpaca:algo:events`` with consumer group
``persister`` into ``runtime.DB_PATH`` in batches. Every write is idempotent (INSERT OR IGNORE / upsert)
so replays and crashes are harmless. Owns the schema (``SCHEMA`` / ``setup_db``).
No I/O at import time.
"""
from __future__ import annotations

import json
import os
import re
import time

import runtime

log = runtime.log("persister")

GROUP = "persister"
ALGO_COID_RE = re.compile(r"^algo-([A-Za-z0-9]+)-(\d+)$")

SCHEMA = """
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
CREATE TABLE IF NOT EXISTS algo_orders(
  algo_id TEXT PRIMARY KEY, strategy TEXT NOT NULL, account TEXT NOT NULL DEFAULT 'paper',
  symbol TEXT NOT NULL, side TEXT NOT NULL, total_qty REAL NOT NULL, n_slices INTEGER NOT NULL,
  start_ts TEXT NOT NULL, end_ts TEXT NOT NULL, limit_cap REAL, params_json TEXT, source TEXT,
  status TEXT NOT NULL, filled_qty REAL DEFAULT 0, avg_px REAL, error TEXT,
  created_at TEXT, updated_at TEXT);
CREATE INDEX IF NOT EXISTS idx_algo_status ON algo_orders(status);
CREATE TABLE IF NOT EXISTS algo_slices(
  algo_id TEXT NOT NULL, slice_idx INTEGER NOT NULL, scheduled_ts TEXT NOT NULL, qty REAL NOT NULL,
  client_order_id TEXT, order_id TEXT, limit_price REAL, status TEXT NOT NULL,
  filled_qty REAL DEFAULT 0, updated_at TEXT, PRIMARY KEY(algo_id, slice_idx));
"""


def setup_db(conn) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# --------------------------------------------------------------------------- helpers
def _empty(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() in ("", "None", "null"))


def _f(v):
    if _empty(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v):
    if _empty(v):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _s(v):
    if _empty(v):
        return None
    return str(v)


def _derive_algo(coid):
    if not coid:
        return None, None
    m = ALGO_COID_RE.match(str(coid))
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


def _account(v, default="paper"):
    if _empty(v):
        return default
    return "live" if str(v).lower() == "live" else "paper"


def _dumps(obj) -> str:
    return json.dumps(obj, default=str)


_TERM_SQL = ",".join(f"'{s}'" for s in sorted(runtime.TERMINAL_STATUSES))

_UPSERT_ORDER_SQL = f"""
INSERT INTO orders(order_id, client_order_id, account, symbol, side, type, time_in_force, qty,
  limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx, submitted_at,
  created_at, updated_at, last_event_ts, raw_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(order_id) DO UPDATE SET
  client_order_id = COALESCE(orders.client_order_id, excluded.client_order_id),
  account = COALESCE(excluded.account, orders.account),
  symbol = COALESCE(excluded.symbol, orders.symbol),
  side = COALESCE(excluded.side, orders.side),
  type = COALESCE(excluded.type, orders.type),
  time_in_force = COALESCE(excluded.time_in_force, orders.time_in_force),
  qty = COALESCE(excluded.qty, orders.qty),
  limit_price = COALESCE(excluded.limit_price, orders.limit_price),
  filled_qty = MAX(COALESCE(excluded.filled_qty, 0), COALESCE(orders.filled_qty, 0)),
  filled_avg_price = COALESCE(excluded.filled_avg_price, orders.filled_avg_price),
  status = CASE WHEN orders.status IN ({_TERM_SQL})
                 AND (excluded.status IS NULL OR excluded.status NOT IN ({_TERM_SQL}))
                THEN orders.status ELSE COALESCE(excluded.status, orders.status) END,
  source = COALESCE(orders.source, excluded.source),
  algo_id = COALESCE(orders.algo_id, excluded.algo_id),
  slice_idx = COALESCE(orders.slice_idx, excluded.slice_idx),
  submitted_at = COALESCE(orders.submitted_at, excluded.submitted_at),
  created_at = COALESCE(orders.created_at, excluded.created_at),
  updated_at = COALESCE(excluded.updated_at, orders.updated_at),
  last_event_ts = COALESCE(excluded.last_event_ts, orders.last_event_ts),
  raw_json = COALESCE(excluded.raw_json, orders.raw_json)
"""


# --------------------------------------------------------------------------- writers (no commit)
def upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None,
                 event_ts=None, raw=None) -> str | None:
    if not isinstance(order, dict):
        return None
    o = runtime.normalize_order(order)
    oid = o.get("order_id")
    if not oid:
        return None
    d_algo, d_idx = _derive_algo(o.get("client_order_id"))
    algo_id = _s(algo_id) or d_algo
    sidx = _i(slice_idx)
    if sidx is None:
        sidx = d_idx
    source = _s(source) or ("algo" if d_algo else None)
    raw_json = raw if isinstance(raw, str) else _dumps(raw if raw is not None else order)
    conn.execute(_UPSERT_ORDER_SQL, (
        oid, o.get("client_order_id"), _account(account), o.get("symbol"), o.get("side"), o.get("type"),
        o.get("time_in_force"), o.get("qty"), o.get("limit_price"), o.get("filled_qty") or 0.0,
        o.get("filled_avg_price"), o.get("status"), source, algo_id, sidx, o.get("submitted_at"),
        o.get("created_at"), o.get("updated_at") or runtime.now_iso(),
        _s(event_ts) or runtime.now_iso(), raw_json))
    return oid


def handle_trade_update(conn, stream_id, payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("trade update payload is not an object")
    event = _s(payload.get("event"))
    order = payload.get("order") if isinstance(payload.get("order"), dict) else None
    order_id = _s(payload.get("order_id"))
    if not order_id and order:
        order_id = _s(order.get("id") or order.get("order_id"))
    account = _account(payload.get("account"))
    ts = _s(payload.get("ts")) or runtime.now_iso()
    raw = _dumps(payload)
    conn.execute("INSERT OR IGNORE INTO trade_events(stream_id, event, order_id, account, ts, raw_json) "
                 "VALUES(?,?,?,?,?,?)", (str(stream_id), event, order_id, account, ts, raw))
    if order:
        upsert_order(conn, order, account=account, source=payload.get("source"),
                     algo_id=payload.get("algo_id"), slice_idx=payload.get("slice_idx"), event_ts=ts)
    exec_id = _s(payload.get("execution_id"))
    if event in ("fill", "partial_fill") and exec_id:
        algo_id = _s(payload.get("algo_id"))
        if not algo_id and order_id:
            row = conn.execute("SELECT algo_id FROM orders WHERE order_id = ?", (order_id,)).fetchone()
            if row is not None and row[0]:
                algo_id = row[0]
        coid = _s(payload.get("client_order_id")) or (_s(order.get("client_order_id")) if order else None)
        if not algo_id:
            algo_id = _derive_algo(coid)[0]
        symbol = _s(payload.get("symbol")) or (_s(order.get("symbol")) if order else None)
        side = payload.get("side")
        if _empty(side) and order:
            side = order.get("side")
        side = runtime._enum_str(side) if not _empty(side) else None
        conn.execute("INSERT OR IGNORE INTO fills(execution_id, order_id, account, symbol, side, price, qty, "
                     "event, ts, algo_id, raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                     (exec_id, order_id, account, symbol, side, _f(payload.get("price")),
                      _f(payload.get("qty")), event, ts, algo_id, raw))


def handle_oms_event(conn, stream_id, payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("oms event payload is not an object")
    typ = payload.get("type")
    if typ == "order":
        order = payload.get("order")
        if not isinstance(order, dict):
            log.warning("oms event %s: order missing", stream_id)
            return
        upsert_order(conn, order, account=payload.get("account") or order.get("account"),
                     source=payload.get("source") or order.get("source"),
                     algo_id=payload.get("algo_id") or order.get("algo_id"),
                     slice_idx=payload.get("slice_idx") if not _empty(payload.get("slice_idx"))
                     else order.get("slice_idx"),
                     event_ts=payload.get("ts"), raw=order)
    elif typ == "command":
        c = payload.get("command")
        if not isinstance(c, dict) or _empty(c.get("cmd_id")):
            log.warning("oms event %s: command missing cmd_id", stream_id)
            return
        conn.execute(
            "INSERT INTO commands(cmd_id, stream_id, kind, account, client_order_id, order_id, algo_id, "
            "slice_idx, source, payload_json, status, error, received_at, completed_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(cmd_id) DO UPDATE SET status = excluded.status, error = excluded.error, "
            "order_id = COALESCE(excluded.order_id, commands.order_id), "
            "algo_id = COALESCE(excluded.algo_id, commands.algo_id), "
            "completed_at = COALESCE(excluded.completed_at, commands.completed_at)",
            (str(c["cmd_id"]), str(stream_id), _s(c.get("kind")) or "unknown", _account(c.get("account")),
             _s(c.get("client_order_id")), _s(c.get("order_id")), _s(c.get("algo_id")),
             _i(c.get("slice_idx")), _s(c.get("source")), _dumps(c.get("payload")),
             _s(c.get("status")) or "unknown", _s(c.get("error")), _s(c.get("received_at")),
             _s(c.get("completed_at"))))
    else:
        log.info("oms event %s: unknown type %r ignored", stream_id, typ)


_UPSERT_SLICE_SQL = """
INSERT INTO algo_slices(algo_id, slice_idx, scheduled_ts, qty, client_order_id, order_id, limit_price,
  status, filled_qty, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(algo_id, slice_idx) DO UPDATE SET
  scheduled_ts = excluded.scheduled_ts, qty = excluded.qty,
  client_order_id = COALESCE(excluded.client_order_id, algo_slices.client_order_id),
  order_id = COALESCE(excluded.order_id, algo_slices.order_id),
  limit_price = COALESCE(excluded.limit_price, algo_slices.limit_price),
  status = excluded.status,
  filled_qty = MAX(COALESCE(excluded.filled_qty, 0), COALESCE(algo_slices.filled_qty, 0)),
  updated_at = excluded.updated_at
"""


def _upsert_slice(conn, sl: dict, algo_id=None) -> None:
    if not isinstance(sl, dict):
        return
    aid = _s(sl.get("algo_id")) or _s(algo_id)
    idx = _i(sl.get("slice_idx"))
    if not aid or idx is None:
        log.warning("slice without algo_id/slice_idx ignored")
        return
    conn.execute(_UPSERT_SLICE_SQL, (
        aid, idx, _s(sl.get("scheduled_ts")) or "", _f(sl.get("qty")) or 0.0, _s(sl.get("client_order_id")),
        _s(sl.get("order_id")), _f(sl.get("limit_price")), _s(sl.get("status")) or "unknown",
        _f(sl.get("filled_qty")) or 0.0, _s(sl.get("updated_at")) or runtime.now_iso()))


_UPSERT_ALGO_SQL = """
INSERT INTO algo_orders(algo_id, strategy, account, symbol, side, total_qty, n_slices, start_ts, end_ts,
  limit_cap, params_json, source, status, filled_qty, avg_px, error, created_at, updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(algo_id) DO UPDATE SET
  strategy = excluded.strategy, account = excluded.account, symbol = excluded.symbol,
  side = excluded.side, total_qty = excluded.total_qty, n_slices = excluded.n_slices,
  start_ts = excluded.start_ts, end_ts = excluded.end_ts, limit_cap = excluded.limit_cap,
  params_json = COALESCE(excluded.params_json, algo_orders.params_json),
  source = COALESCE(excluded.source, algo_orders.source),
  status = excluded.status, filled_qty = excluded.filled_qty,
  avg_px = COALESCE(excluded.avg_px, algo_orders.avg_px), error = excluded.error,
  created_at = COALESCE(algo_orders.created_at, excluded.created_at),
  updated_at = excluded.updated_at
"""


def handle_algo_event(conn, stream_id, payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("algo event payload is not an object")
    typ = payload.get("type")
    if typ == "algo":
        a = payload.get("algo")
        if not isinstance(a, dict) or _empty(a.get("algo_id")):
            log.warning("algo event %s: algo missing", stream_id)
            return
        params = a.get("params_json")
        if params is not None and not isinstance(params, str):
            params = _dumps(params)
        conn.execute(_UPSERT_ALGO_SQL, (
            str(a["algo_id"]), _s(a.get("strategy")) or "unknown", _account(a.get("account")),
            _s(a.get("symbol")) or "", _s(a.get("side")) or "", _f(a.get("total_qty")) or 0.0,
            _i(a.get("n_slices")) or 0, _s(a.get("start_ts")) or "", _s(a.get("end_ts")) or "",
            _f(a.get("limit_cap")), params, _s(a.get("source")), _s(a.get("status")) or "unknown",
            _f(a.get("filled_qty")) or 0.0, _f(a.get("avg_px")), _s(a.get("error")),
            _s(a.get("created_at")), _s(a.get("updated_at")) or runtime.now_iso()))
        for sl in payload.get("slices") or []:
            _upsert_slice(conn, sl, a["algo_id"])
    elif typ == "slice":
        _upsert_slice(conn, payload.get("slice"), payload.get("algo_id"))
    else:
        log.info("algo event %s: unknown type %r ignored", stream_id, typ)


HANDLERS = {
    runtime.STREAM_TRADE_UPDATES: handle_trade_update,
    runtime.STREAM_OMS_EVENTS: handle_oms_event,
    runtime.STREAM_ALGO_EVENTS: handle_algo_event,
}


# --------------------------------------------------------------------------- batch loop
def _decode(fields: dict) -> dict:
    payload = json.loads((fields or {}).get("json") or "")
    if not isinstance(payload, dict):
        raise ValueError("payload is not an object")
    return payload


def process_batch(conn, reader, handler) -> int:
    entries = reader.read()
    if not entries:
        return 0
    acked = 0
    try:
        for entry_id, fields in entries:
            handler(conn, entry_id, _decode(fields))
        conn.commit()
    except Exception as e:
        log.warning("batch of %d on %s failed (%s: %s); retrying one by one", len(entries),
                    reader.stream, type(e).__name__, e)
        try:
            conn.rollback()
        except Exception:
            pass
    else:
        for entry_id, _fields in entries:
            try:
                reader.ack(entry_id)
                acked += 1
            except Exception as e:
                log.error("ack %s failed: %s", entry_id, e)
        return acked

    for entry_id, fields in entries:
        try:
            handler(conn, entry_id, _decode(fields))
            conn.commit()
        except Exception as e:
            log.exception("entry %s on %s failed; dead-lettering", entry_id, reader.stream)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                reader.dead_letter(entry_id, fields, f"{type(e).__name__}: {e}")
                acked += 1
            except Exception as e2:
                log.error("dead_letter %s failed: %s", entry_id, e2)
            continue
        try:
            reader.ack(entry_id)
            acked += 1
        except Exception as e:
            log.error("ack %s failed: %s", entry_id, e)
    return acked


def body():
    r = runtime.get_redis()
    conn = runtime.sqlite_connect()
    try:
        setup_db(conn)
        consumer = os.environ.get("PERSISTER_CONSUMER", "persister-1")
        readers = [(runtime.GroupReader(r, stream, GROUP, consumer, block_ms=200), handler)
                   for stream, handler in HANDLERS.items()]
        log.info("persister started consumer=%s db=%s", consumer, runtime.DB_PATH)
        counts = {stream: 0 for stream in HANDLERS}
        last_summary = time.monotonic()
        while True:
            total = 0
            for reader, handler in readers:
                n = process_batch(conn, reader, handler)
                counts[reader.stream] += n
                total += n
            if time.monotonic() - last_summary >= 30:
                if any(counts.values()):
                    log.info("persisted last 30s: %s",
                             " ".join(f"{k}={v}" for k, v in counts.items()))
                counts = {stream: 0 for stream in HANDLERS}
                last_summary = time.monotonic()
            if not total:
                time.sleep(0.05)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    runtime.run_forever(body, "persister")


if __name__ == "__main__":
    main()
