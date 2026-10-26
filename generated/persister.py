"""Persister worker: the ONLY process that writes SQLite.

Drains alpaca:trade_updates, alpaca:oms:events and alpaca:algo:events with consumer group
``persister`` into ``runtime.DB_PATH`` in batches. Every write is idempotent (INSERT OR IGNORE /
upsert keyed by stream_id / execution_id / cmd_id / order_id / (algo_id, slice_idx)), so replays
after crashes are harmless. Poison entries are dead-lettered. No I/O at import time.
"""
from __future__ import annotations

import json
import os
import re
import time

import runtime

log = runtime.log("persister")

GROUP = "persister"
ALGO_COID_RE = re.compile(r"^algo-([A-Za-z0-9]{1,12})-(\d+)$")
FILL_EVENTS = ("fill", "partial_fill", "internal_fill")

SCHEMA = """
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
"""


def setup_db(conn) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# --------------------------------------------------------------------------- helpers
def _f(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _int(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def _s(x):
    if x is None or x == "" or x == "None":
        return None
    return str(x)


def _dumps(x) -> str:
    return json.dumps(x, default=str)


def _derive_algo(coid):
    if not coid:
        return None, None
    m = ALGO_COID_RE.match(str(coid))
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


_TERM_SQL = ",".join(f"'{s}'" for s in sorted(runtime.TERMINAL_STATUSES))


# --------------------------------------------------------------------------- orders
def upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None,
                 book=None, event_ts=None, raw=None) -> None:
    order = order or {}
    o = runtime.normalize_order(order)
    oid = o.get("order_id")
    if not oid:
        return
    coid = o.get("client_order_id")
    is_internal = str(oid).startswith("int-")
    lookup_coid = coid or (str(oid)[4:] if is_internal else None)
    d_algo, d_idx = _derive_algo(lookup_coid)
    algo_id = _s(algo_id) or _s(order.get("algo_id")) or d_algo
    slice_idx = _int(slice_idx)
    if slice_idx is None:
        slice_idx = _int(order.get("slice_idx"))
    if slice_idx is None:
        slice_idx = d_idx
    source = _s(source) or _s(order.get("source"))
    book = _s(book) or _s(order.get("book")) or runtime.DEFAULT_BOOK
    venue = _s(order.get("venue")) or ("internal" if is_internal else "alpaca")
    routed = _s(order.get("routed_order_id"))
    error = _s(order.get("error"))
    acct = _s(account) or _s(order.get("account")) or "paper"

    # One client_order_id can belong to int-<coid> AND the exchange order; the column is
    # UNIQUE, so the exchange order owns it (the internal one is reachable by its id).
    if coid:
        row = conn.execute("SELECT order_id FROM orders WHERE client_order_id = ? AND order_id != ?",
                           (coid, oid)).fetchone()
        if row is not None:
            other = row[0]
            if not is_internal and str(other).startswith("int-"):
                conn.execute("UPDATE orders SET client_order_id = NULL WHERE order_id = ?", (other,))
            else:
                coid = None

    raw_json = raw if isinstance(raw, str) else _dumps(raw if raw is not None else order)
    now = runtime.now_iso()
    conn.execute(f"""
        INSERT INTO orders(order_id, client_order_id, account, symbol, side, type, time_in_force,
            qty, limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx,
            book, venue, routed_order_id, error, submitted_at, created_at, updated_at,
            last_event_ts, raw_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(order_id) DO UPDATE SET
            client_order_id = COALESCE(orders.client_order_id, excluded.client_order_id),
            account = COALESCE(excluded.account, orders.account),
            symbol = COALESCE(excluded.symbol, orders.symbol),
            side = COALESCE(excluded.side, orders.side),
            type = COALESCE(excluded.type, orders.type),
            time_in_force = COALESCE(excluded.time_in_force, orders.time_in_force),
            qty = COALESCE(excluded.qty, orders.qty),
            limit_price = COALESCE(excluded.limit_price, orders.limit_price),
            filled_qty = max(COALESCE(excluded.filled_qty, 0), COALESCE(orders.filled_qty, 0)),
            filled_avg_price = COALESCE(excluded.filled_avg_price, orders.filled_avg_price),
            status = CASE WHEN orders.status IN ({_TERM_SQL})
                           AND (excluded.status IS NULL OR excluded.status NOT IN ({_TERM_SQL}))
                          THEN orders.status
                          ELSE COALESCE(excluded.status, orders.status) END,
            source = COALESCE(orders.source, excluded.source),
            algo_id = COALESCE(orders.algo_id, excluded.algo_id),
            slice_idx = COALESCE(orders.slice_idx, excluded.slice_idx),
            book = COALESCE(orders.book, excluded.book),
            venue = COALESCE(excluded.venue, orders.venue),
            routed_order_id = COALESCE(excluded.routed_order_id, orders.routed_order_id),
            error = COALESCE(excluded.error, orders.error),
            submitted_at = COALESCE(orders.submitted_at, excluded.submitted_at),
            created_at = COALESCE(orders.created_at, excluded.created_at),
            updated_at = COALESCE(excluded.updated_at, orders.updated_at),
            last_event_ts = COALESCE(excluded.last_event_ts, orders.last_event_ts),
            raw_json = excluded.raw_json
    """, (oid, coid, acct, o.get("symbol"), o.get("side"), o.get("type"), o.get("time_in_force"),
          o.get("qty"), o.get("limit_price"), o.get("filled_qty") or 0.0, o.get("filled_avg_price"),
          o.get("status"), source, algo_id, slice_idx, book, venue, routed, error,
          o.get("submitted_at"), o.get("created_at"), o.get("updated_at") or now,
          _s(event_ts) or now, raw_json))


# --------------------------------------------------------------------------- handlers
def handle_trade_update(conn, stream_id, payload: dict) -> None:
    payload = payload if isinstance(payload, dict) else {}
    event = _s(payload.get("event"))
    if event and "." in event and event.split(".", 1)[0][:1].isupper():
        event = event.rsplit(".", 1)[1].lower()
    order = payload.get("order") if isinstance(payload.get("order"), dict) else None
    order_id = _s(payload.get("order_id")) or (
        _s(order.get("id") or order.get("order_id")) if order else None)
    account = _s(payload.get("account")) or "paper"
    ts = _s(payload.get("ts")) or runtime.now_iso()
    raw = _dumps(payload)
    conn.execute("INSERT OR IGNORE INTO trade_events(stream_id, event, order_id, account, ts, raw_json) "
                 "VALUES (?,?,?,?,?,?)", (str(stream_id), event, order_id, account, ts, raw))
    if order is not None:
        upsert_order(conn, order, account=account, source=payload.get("source"),
                     algo_id=payload.get("algo_id"), slice_idx=payload.get("slice_idx"),
                     book=payload.get("book"), event_ts=ts)
    exec_id = _s(payload.get("execution_id"))
    if event in FILL_EVENTS and exec_id:
        algo_id = _s(payload.get("algo_id"))
        if not algo_id and order_id:
            row = conn.execute("SELECT algo_id FROM orders WHERE order_id = ?", (order_id,)).fetchone()
            if row is not None and row[0]:
                algo_id = row[0]
        if not algo_id:
            coid = _s(payload.get("client_order_id")) or (_s(order.get("client_order_id")) if order else None)
            algo_id = _derive_algo(coid)[0]
        venue = _s(payload.get("venue")) or "alpaca"
        symbol = _s(payload.get("symbol")) or (_s(order.get("symbol")) if order else None)
        side = _s(payload.get("side")) or (runtime._enum_str(order.get("side")) if order else None)
        side = runtime._enum_str(side) if side else None
        conn.execute("""INSERT OR IGNORE INTO fills(execution_id, order_id, account, symbol, side, price,
                        qty, event, ts, algo_id, book, venue, cross_id, counter_client_order_id, raw_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                     (exec_id, order_id, account, symbol, side, _f(payload.get("price")),
                      _f(payload.get("qty")), event, ts, algo_id,
                      _s(payload.get("book")) or (_s(order.get("book")) if order else None),
                      venue, _s(payload.get("cross_id")), _s(payload.get("counter_client_order_id")),
                      raw))


def handle_oms_event(conn, stream_id, payload: dict) -> None:
    payload = payload if isinstance(payload, dict) else {}
    typ = payload.get("type")
    if typ == "order":
        order = payload.get("order") if isinstance(payload.get("order"), dict) else {}
        upsert_order(conn, order, account=payload.get("account") or order.get("account") or "paper",
                     source=payload.get("source") or order.get("source"),
                     algo_id=payload.get("algo_id") or order.get("algo_id"),
                     slice_idx=payload.get("slice_idx") if payload.get("slice_idx") is not None
                     else order.get("slice_idx"),
                     book=payload.get("book"), event_ts=payload.get("ts"))
    elif typ == "command":
        c = payload.get("command") if isinstance(payload.get("command"), dict) else {}
        cmd_id = _s(c.get("cmd_id"))
        if not cmd_id:
            log.warning("command event %s without cmd_id ignored", stream_id)
            return
        conn.execute("""
            INSERT INTO commands(cmd_id, stream_id, kind, account, client_order_id, order_id, algo_id,
                slice_idx, source, payload_json, status, error, received_at, completed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cmd_id) DO UPDATE SET
                status = excluded.status, error = excluded.error,
                order_id = COALESCE(excluded.order_id, commands.order_id),
                algo_id = COALESCE(excluded.algo_id, commands.algo_id),
                completed_at = COALESCE(excluded.completed_at, commands.completed_at)
        """, (cmd_id, str(stream_id), _s(c.get("kind")) or "unknown", _s(c.get("account")) or "paper",
              _s(c.get("client_order_id")), _s(c.get("order_id")), _s(c.get("algo_id")),
              _int(c.get("slice_idx")), _s(c.get("source")), _dumps(c.get("payload")),
              _s(c.get("status")) or "unknown", _s(c.get("error")), _s(c.get("received_at")),
              _s(c.get("completed_at"))))
    else:
        log.info("unknown oms event type %r (%s) ignored", typ, stream_id)


def _upsert_slice(conn, sl: dict, algo_id=None) -> None:
    sl = sl if isinstance(sl, dict) else {}
    aid = _s(sl.get("algo_id")) or _s(algo_id)
    idx = _int(sl.get("slice_idx"))
    if not aid or idx is None:
        return
    sched = sl.get("scheduled_ts")
    conn.execute("""
        INSERT INTO algo_slices(algo_id, slice_idx, scheduled_ts, qty, client_order_id, order_id,
            limit_price, status, filled_qty, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(algo_id, slice_idx) DO UPDATE SET
            scheduled_ts = excluded.scheduled_ts, qty = excluded.qty,
            client_order_id = excluded.client_order_id, order_id = excluded.order_id,
            limit_price = excluded.limit_price, status = excluded.status,
            filled_qty = excluded.filled_qty, updated_at = excluded.updated_at
    """, (aid, idx, str(sched) if sched not in (None, "") else "", _f(sl.get("qty")) or 0.0,
          _s(sl.get("client_order_id")), _s(sl.get("order_id")), _f(sl.get("limit_price")),
          _s(sl.get("status")) or "scheduled", _f(sl.get("filled_qty")) or 0.0,
          _s(sl.get("updated_at")) or runtime.now_iso()))


def handle_algo_event(conn, stream_id, payload: dict) -> None:
    payload = payload if isinstance(payload, dict) else {}
    typ = payload.get("type")
    if typ == "algo":
        a = payload.get("algo") if isinstance(payload.get("algo"), dict) else {}
        aid = _s(a.get("algo_id"))
        if not aid:
            log.warning("algo event %s without algo_id ignored", stream_id)
            return
        params_json = a.get("params_json")
        if params_json is None and a.get("params") is not None:
            params_json = a.get("params") if isinstance(a.get("params"), str) else _dumps(a.get("params"))
        elif params_json is not None and not isinstance(params_json, str):
            params_json = _dumps(params_json)
        st, et = a.get("start_ts"), a.get("end_ts")
        conn.execute("""
            INSERT INTO algo_orders(algo_id, strategy, account, book, symbol, side, total_qty, n_slices,
                start_ts, end_ts, limit_cap, params_json, source, status, filled_qty, avg_px, error,
                created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(algo_id) DO UPDATE SET
                strategy = excluded.strategy, account = excluded.account,
                book = COALESCE(excluded.book, algo_orders.book), symbol = excluded.symbol,
                side = excluded.side, total_qty = excluded.total_qty, n_slices = excluded.n_slices,
                start_ts = excluded.start_ts, end_ts = excluded.end_ts, limit_cap = excluded.limit_cap,
                params_json = COALESCE(excluded.params_json, algo_orders.params_json),
                source = COALESCE(excluded.source, algo_orders.source), status = excluded.status,
                filled_qty = excluded.filled_qty, avg_px = excluded.avg_px, error = excluded.error,
                created_at = COALESCE(algo_orders.created_at, excluded.created_at),
                updated_at = excluded.updated_at
        """, (aid, _s(a.get("strategy")) or "unknown", _s(a.get("account")) or "paper",
              _s(a.get("book")) or runtime.DEFAULT_BOOK, _s(a.get("symbol")) or "",
              _s(a.get("side")) or "", _f(a.get("total_qty")) or 0.0, _int(a.get("n_slices")) or 0,
              str(st) if st not in (None, "") else "", str(et) if et not in (None, "") else "",
              _f(a.get("limit_cap")), params_json, _s(a.get("source")),
              _s(a.get("status")) or "unknown", _f(a.get("filled_qty")) or 0.0, _f(a.get("avg_px")),
              _s(a.get("error")), _s(a.get("created_at")),
              _s(a.get("updated_at")) or runtime.now_iso()))
        for sl in payload.get("slices") or []:
            _upsert_slice(conn, sl, aid)
    elif typ == "slice":
        _upsert_slice(conn, payload.get("slice"), payload.get("algo_id"))
    else:
        log.info("unknown algo event type %r (%s) ignored", typ, stream_id)


HANDLERS = {
    runtime.STREAM_TRADE_UPDATES: handle_trade_update,
    runtime.STREAM_OMS_EVENTS: handle_oms_event,
    runtime.STREAM_ALGO_EVENTS: handle_algo_event,
}


# --------------------------------------------------------------------------- batch loop
def _decode(fields: dict) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        return dict(fields or {})
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("payload is not an object")
    return d


def process_batch(conn, reader, handler) -> int:
    entries = reader.read()
    if not entries:
        return 0
    try:
        for eid, fields in entries:
            handler(conn, eid, _decode(fields))
        conn.commit()
    except Exception as e:
        log.warning("batch of %d on %s failed (%s: %s); retrying one by one", len(entries),
                    getattr(reader, "stream", "?"), type(e).__name__, e)
        try:
            conn.rollback()
        except Exception:
            pass
    else:
        for eid, _ in entries:
            reader.ack(eid)
        return len(entries)

    acked = 0
    for eid, fields in entries:
        try:
            handler(conn, eid, _decode(fields))
            conn.commit()
        except Exception as e:
            log.exception("entry %s on %s failed; dead-lettering", eid, getattr(reader, "stream", "?"))
            try:
                conn.rollback()
            except Exception:
                pass
            reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
            acked += 1
            continue
        reader.ack(eid)
        acked += 1
    return acked


def body() -> None:
    r = runtime.get_redis()
    conn = runtime.sqlite_connect()
    try:
        setup_db(conn)
        consumer = os.environ.get("PERSISTER_CONSUMER", "persister-1")
        readers = [(stream, runtime.GroupReader(r, stream, GROUP, consumer, block_ms=200), handler)
                   for stream, handler in HANDLERS.items()]
        log.info("persister started consumer=%s db=%s", consumer, runtime.DB_PATH)
        counts = {stream: 0 for stream in HANDLERS}
        last_log = time.monotonic()
        while True:
            total = 0
            for stream, reader, handler in readers:
                n = process_batch(conn, reader, handler)
                counts[stream] += n
                total += n
            if time.monotonic() - last_log >= 30:
                last_log = time.monotonic()
                if any(counts.values()):
                    log.info("persisted last 30s: %s",
                             " ".join(f"{k}={v}" for k, v in counts.items()))
                counts = {stream: 0 for stream in HANDLERS}
            if not total:
                time.sleep(0.05)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    runtime.run_forever(body, "persister")


if __name__ == "__main__":
    main()
