"""Order Management System worker.

The single source of truth for order state and the ONLY process that calls Alpaca
order endpoints (via trader.py).

- Consumes ``alpaca:trade_updates`` (group ``oms``) -> SQLite orders/fills/trade_events.
- Consumes ``alpaca:oms:commands`` (group ``oms``) -> submit/cancel via trader, reply on
  ``alpaca:reply:<cmd_id>``, audit in ``commands``.
- Reconciles with Alpaca REST at startup and every OMS_RECONCILE_S seconds.

All writes are idempotent (keyed by stream_id / execution_id / order_id / client_order_id),
so at-least-once replay is harmless. Nothing performs I/O at import time.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

from alpaca.common.exceptions import APIError

import runtime
import trader

NAME = "oms"
GROUP = "oms"
LAST_RECONCILE_KEY = "alpaca:oms:last_reconcile"
RETRY_DELAYS = (0.5, 1.0, 2.0)

log = runtime.log(NAME)

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
  client_order_id TEXT, order_id TEXT, source TEXT, algo_id TEXT, slice_idx INTEGER,
  payload_json TEXT NOT NULL, status TEXT NOT NULL, error TEXT, received_at TEXT, completed_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cmd_coid ON commands(client_order_id) WHERE kind='submit';
"""

_TERMINAL_SQL = ", ".join(f"'{s}'" for s in sorted(runtime.TERMINAL_STATUSES))
_EMS_COID_RE = re.compile(r"^algo-([A-Za-z0-9]+)-(\d+)$")
_COID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")

_UPSERT_SQL = f"""
INSERT INTO orders(order_id, client_order_id, account, symbol, side, type, time_in_force, qty,
  limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx, submitted_at,
  created_at, updated_at, last_event_ts, raw_json)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(order_id) DO UPDATE SET
  client_order_id = COALESCE(orders.client_order_id, excluded.client_order_id),
  symbol = COALESCE(excluded.symbol, orders.symbol),
  side = COALESCE(excluded.side, orders.side),
  type = COALESCE(excluded.type, orders.type),
  time_in_force = COALESCE(excluded.time_in_force, orders.time_in_force),
  qty = COALESCE(excluded.qty, orders.qty),
  limit_price = COALESCE(excluded.limit_price, orders.limit_price),
  filled_qty = MAX(COALESCE(excluded.filled_qty, 0), COALESCE(orders.filled_qty, 0)),
  filled_avg_price = COALESCE(excluded.filled_avg_price, orders.filled_avg_price),
  status = CASE WHEN orders.status IN ({_TERMINAL_SQL})
                 AND COALESCE(excluded.status, '') NOT IN ({_TERMINAL_SQL})
            THEN orders.status ELSE COALESCE(excluded.status, orders.status) END,
  source = COALESCE(orders.source, excluded.source),
  algo_id = COALESCE(orders.algo_id, excluded.algo_id),
  slice_idx = COALESCE(orders.slice_idx, excluded.slice_idx),
  submitted_at = COALESCE(orders.submitted_at, excluded.submitted_at),
  created_at = COALESCE(orders.created_at, excluded.created_at),
  updated_at = COALESCE(excluded.updated_at, orders.updated_at),
  last_event_ts = COALESCE(excluded.last_event_ts, orders.last_event_ts),
  raw_json = CASE WHEN orders.status IN ({_TERMINAL_SQL})
                   AND COALESCE(excluded.status, '') NOT IN ({_TERMINAL_SQL})
              THEN orders.raw_json ELSE excluded.raw_json END
"""


# --------------------------------------------------------------------------- helpers
def setup_db(conn) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _s(x):
    if x is None:
        return None
    v = getattr(x, "value", x)
    v = str(v)
    return v if v != "" else None


def _lower(x):
    v = _s(x)
    return v.lower() if v is not None else None


def _ts(x):
    if x is None or x == "":
        return None
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)


def parse_ems_coid(client_order_id):
    """``algo-<id12>-<idx>`` -> (algo_id_prefix, slice_idx) or (None, None)."""
    if not client_order_id:
        return None, None
    m = _EMS_COID_RE.match(str(client_order_id))
    if not m:
        return None, None
    try:
        return m.group(1), int(m.group(2))
    except ValueError:
        return m.group(1), None


def _row_to_dict(row):
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def get_order_row(conn, order_id=None, client_order_id=None):
    row = None
    if order_id:
        row = conn.execute("SELECT * FROM orders WHERE order_id=?", (str(order_id),)).fetchone()
    if row is None and client_order_id:
        row = conn.execute("SELECT * FROM orders WHERE client_order_id=?",
                           (str(client_order_id),)).fetchone()
    return _row_to_dict(row)


def upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None,
                 event_ts=None):
    """Idempotently upsert an Alpaca order dict into ``orders``. Returns order_id or None."""
    if not isinstance(order, dict):
        order = trader._to_dict(order)
        if not isinstance(order, dict):
            return None
    oid = order.get("id") or order.get("order_id")
    if not oid:
        return None
    oid = str(oid)
    coid = order.get("client_order_id")
    coid = str(coid) if coid else None
    if algo_id is None or slice_idx is None:
        a, i = parse_ems_coid(coid)
        if algo_id is None:
            algo_id = a
        if slice_idx is None:
            slice_idx = i
    otype = order.get("type") or order.get("order_type")
    now = runtime.now_iso()
    params = (
        oid, coid, account or "paper", _s(order.get("symbol")), _lower(order.get("side")),
        _lower(otype), _lower(order.get("time_in_force")), _f(order.get("qty")),
        _f(order.get("limit_price")), _f(order.get("filled_qty")) or 0.0,
        _f(order.get("filled_avg_price")), _lower(order.get("status")), source, algo_id,
        int(slice_idx) if slice_idx is not None else None,
        _ts(order.get("submitted_at")), _ts(order.get("created_at")),
        _ts(order.get("updated_at")) or now, _ts(event_ts),
        json.dumps(order, default=str),
    )
    conn.execute(_UPSERT_SQL, params)
    return oid


# --------------------------------------------------------------------------- trade updates
def handle_payload(conn, stream_id: str, payload: dict) -> None:
    """Persist one trade_updates payload. Caller commits."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    event = _lower(payload.get("event"))
    account = payload.get("account") or "paper"
    ts = _ts(payload.get("ts")) or runtime.now_iso()
    order = payload.get("order")
    order = order if isinstance(order, dict) else {}
    order_id = payload.get("order_id") or order.get("id")
    order_id = str(order_id) if order_id else None
    conn.execute(
        "INSERT OR IGNORE INTO trade_events(stream_id, event, order_id, account, ts, raw_json) "
        "VALUES (?,?,?,?,?,?)",
        (str(stream_id), event, order_id, account, ts, json.dumps(payload, default=str)))
    if order and (order.get("id") or order.get("order_id")):
        upsert_order(conn, order, account=account, event_ts=ts)
    exec_id = payload.get("execution_id")
    if event in ("fill", "partial_fill") and exec_id:
        algo_id = None
        if order_id:
            row = conn.execute("SELECT algo_id FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if row is not None:
                algo_id = row["algo_id"]
        if algo_id is None:
            algo_id, _ = parse_ems_coid(payload.get("client_order_id") or order.get("client_order_id"))
        conn.execute(
            "INSERT OR IGNORE INTO fills(execution_id, order_id, account, symbol, side, price, qty, "
            "event, ts, algo_id, raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (str(exec_id), order_id, account, payload.get("symbol") or order.get("symbol"),
             _lower(payload.get("side") or order.get("side")), _f(payload.get("price")),
             _f(payload.get("qty")), event, ts, algo_id, json.dumps(payload, default=str)))


# --------------------------------------------------------------------------- commands
def _reply(cmd_id, *, ok, status, order_id=None, client_order_id=None, order=None, error=None):
    return {"ok": bool(ok), "status": status, "cmd_id": cmd_id, "order_id": order_id,
            "client_order_id": client_order_id, "order": order, "error": error}


def _err_text(exc) -> str:
    try:
        info = trader.error_info(exc)
        sc = info.get("status_code")
        msg = info.get("message") or str(exc)
        return f"{sc} {msg}" if sc else f"{type(exc).__name__}: {msg}"
    except Exception:
        return f"{type(exc).__name__}: {str(exc)[:500]}"


def _status_code(exc):
    try:
        return exc.status_code
    except Exception:
        return None


def _validate_submit(p: dict):
    symbol = str(p.get("symbol") or "").strip().upper()
    if not symbol:
        return None, "symbol required"
    side = _lower(p.get("side"))
    if side not in ("buy", "sell"):
        return None, "side must be buy or sell"
    otype = _lower(p.get("type") or "market")
    if otype not in ("market", "limit"):
        return None, "type must be market or limit"
    qty = _f(p.get("qty"))
    if qty is None or qty <= 0:
        return None, "qty must be > 0"
    limit_price = _f(p.get("limit_price"))
    if otype == "limit":
        if limit_price is None or limit_price <= 0:
            return None, "limit order requires limit_price > 0"
    else:
        limit_price = None
    tif = _lower(p.get("tif") or p.get("time_in_force") or "gtc")
    if tif not in ("gtc", "ioc"):
        return None, "tif must be gtc or ioc"
    coid = p.get("client_order_id")
    coid = str(coid).strip() if coid else runtime.new_client_order_id("tc")
    if not _COID_RE.match(coid):
        return None, "client_order_id must be <=48 chars of [A-Za-z0-9_-]"
    slice_idx = p.get("slice_idx")
    try:
        slice_idx = int(slice_idx) if slice_idx is not None and slice_idx != "" else None
    except (TypeError, ValueError):
        slice_idx = None
    return {"symbol": symbol, "side": side, "type": otype, "qty": qty, "limit_price": limit_price,
            "tif": tif, "client_order_id": coid, "algo_id": p.get("algo_id") or None,
            "slice_idx": slice_idx}, None


def _finish_command(conn, cmd_id, status, order_id=None, error=None):
    conn.execute(
        "UPDATE commands SET status=?, order_id=COALESCE(?, order_id), error=?, completed_at=? "
        "WHERE cmd_id=?", (status, order_id, error, runtime.now_iso(), cmd_id))


def _lookup_existing(live, coid):
    try:
        return trader.get_order_by_client_id(coid, live=live)
    except Exception as e:
        log.warning("lookup by client_order_id %s failed: %s", coid, e)
        return None


def _do_submit(conn, cmd_id, p, account, stream_id, source):
    v, err = _validate_submit(p)
    if err:
        conn.execute(
            "INSERT OR IGNORE INTO commands(cmd_id, stream_id, kind, account, client_order_id, source, "
            "algo_id, slice_idx, payload_json, status, error, received_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (cmd_id, stream_id, "submit", account, None, source, p.get("algo_id"), None,
             json.dumps(p, default=str), "rejected", err, runtime.now_iso()))
        _finish_command(conn, cmd_id, "rejected", error=err)
        return _reply(cmd_id, ok=False, status="rejected", client_order_id=p.get("client_order_id"),
                      error=err)
    coid = v["client_order_id"]
    live = account == "live"

    existing_order = get_order_row(conn, client_order_id=coid)
    existing_cmd = conn.execute(
        "SELECT * FROM commands WHERE client_order_id=? AND kind='submit'", (coid,)).fetchone()
    if existing_order is not None or existing_cmd is not None:
        if existing_order is None and existing_cmd is not None:
            # previous attempt may have reached Alpaca; try to learn its state (harmless lookup)
            found = _lookup_existing(live, coid)
            if found:
                upsert_order(conn, found, account=existing_cmd["account"] or account,
                             source=existing_cmd["source"] or source,
                             algo_id=existing_cmd["algo_id"] or v["algo_id"],
                             slice_idx=existing_cmd["slice_idx"] if existing_cmd["slice_idx"] is not None
                             else v["slice_idx"])
                existing_order = get_order_row(conn, client_order_id=coid)
        oid = existing_order["order_id"] if existing_order else (
            existing_cmd["order_id"] if existing_cmd is not None else None)
        ok = existing_order is not None or (
            existing_cmd is not None and existing_cmd["status"] in ("acked", "received",
                                                                    "already_submitted"))
        log.info("submit %s already_submitted order=%s", coid, oid)
        return _reply(cmd_id, ok=ok, status="already_submitted", order_id=oid, client_order_id=coid,
                      order=existing_order,
                      error=None if ok else (existing_cmd["error"] if existing_cmd is not None else None))

    conn.execute(
        "INSERT INTO commands(cmd_id, stream_id, kind, account, client_order_id, source, algo_id, "
        "slice_idx, payload_json, status, received_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(cmd_id) DO UPDATE SET status='received', client_order_id=excluded.client_order_id",
        (cmd_id, stream_id, "submit", account, coid, source, v["algo_id"], v["slice_idx"],
         json.dumps(p, default=str), "received", runtime.now_iso()))
    conn.commit()  # durable audit before touching Alpaca

    def _submit():
        if v["type"] == "limit":
            return trader.submit_limit(v["symbol"], v["qty"], v["side"], v["limit_price"], coid,
                                       live=live, tif=v["tif"])
        return trader.submit_market(v["symbol"], v["qty"], v["side"], coid, live=live, tif=v["tif"])

    def _ack(order, status):
        oid = upsert_order(conn, order, account=account, source=source, algo_id=v["algo_id"],
                           slice_idx=v["slice_idx"])
        _finish_command(conn, cmd_id, "acked", order_id=oid)
        row = get_order_row(conn, order_id=oid) or order
        log.info("submit %s %s %s %s qty=%s px=%s -> %s order=%s", account, v["symbol"], v["side"],
                 v["type"], v["qty"], v["limit_price"], status, oid)
        return _reply(cmd_id, ok=True, status=status, order_id=oid, client_order_id=coid, order=row)

    attempt = 0
    while True:
        try:
            order = _submit()
            return _ack(order, "acked")
        except APIError as e:
            sc = _status_code(e)
            text = _err_text(e)
            if sc == 422 and "client_order_id" in (text + str(e)).lower():
                found = _lookup_existing(live, coid)
                if found:
                    return _ack(found, "already_submitted")
                _finish_command(conn, cmd_id, "rejected", error=text)
                log.warning("submit %s rejected: %s", coid, text)
                return _reply(cmd_id, ok=False, status="rejected", client_order_id=coid, error=text)
            if trader.is_transient(e):
                if attempt < len(RETRY_DELAYS):
                    log.warning("submit %s transient error (attempt %d): %s", coid, attempt + 1, text)
                    time.sleep(RETRY_DELAYS[attempt])
                    attempt += 1
                    continue
                return _unknown(conn, cmd_id, coid, live, text, _ack)
            _finish_command(conn, cmd_id, "rejected", error=text)
            log.warning("submit %s rejected: %s", coid, text)
            return _reply(cmd_id, ok=False, status="rejected", client_order_id=coid, error=text)
        except Exception as e:
            text = _err_text(e)
            if trader.is_transient(e):
                if attempt < len(RETRY_DELAYS):
                    log.warning("submit %s transient error (attempt %d): %s", coid, attempt + 1, text)
                    time.sleep(RETRY_DELAYS[attempt])
                    attempt += 1
                    continue
                return _unknown(conn, cmd_id, coid, live, text, _ack)
            _finish_command(conn, cmd_id, "error", error=text)
            log.error("submit %s error: %s", coid, text)
            return _reply(cmd_id, ok=False, status="error", client_order_id=coid, error=text)


def _unknown(conn, cmd_id, coid, live, text, ack):
    found = _lookup_existing(live, coid)
    if found:
        return ack(found, "acked")
    _finish_command(conn, cmd_id, "unknown", error=text)
    log.error("submit %s status unknown after retries: %s", coid, text)
    return _reply(cmd_id, ok=False, status="unknown", client_order_id=coid, error=text)


def _do_cancel(conn, cmd_id, p, account, stream_id, source):
    order_id = p.get("order_id") or None
    coid = p.get("client_order_id") or None
    conn.execute(
        "INSERT INTO commands(cmd_id, stream_id, kind, account, client_order_id, order_id, source, "
        "payload_json, status, received_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(cmd_id) DO UPDATE SET status='received'",
        (cmd_id, stream_id, "cancel", account, coid, order_id, source, json.dumps(p, default=str),
         "received", runtime.now_iso()))
    if not order_id and not coid:
        err = "order_id or client_order_id required"
        _finish_command(conn, cmd_id, "rejected", error=err)
        return _reply(cmd_id, ok=False, status="rejected", error=err)
    row = get_order_row(conn, order_id=order_id, client_order_id=coid)
    if row is not None:
        order_id = order_id or row["order_id"]
        coid = coid or row["client_order_id"]
        account = row["account"] or account
    live = account == "live"
    if not order_id and coid:
        try:
            found = trader.get_order_by_client_id(coid, live=live)
        except Exception as e:
            text = _err_text(e)
            _finish_command(conn, cmd_id, "error", error=text)
            return _reply(cmd_id, ok=False, status="error", client_order_id=coid, error=text)
        if found:
            order_id = upsert_order(conn, found, account=account)
    if not order_id:
        _finish_command(conn, cmd_id, "not_found", error="order not found")
        return _reply(cmd_id, ok=False, status="not_found", client_order_id=coid,
                      error="order not found")
    try:
        trader.cancel(order_id, live=live)
    except APIError as e:
        text = _err_text(e)
        if _status_code(e) == 404:
            _finish_command(conn, cmd_id, "not_found", order_id=order_id, error=text)
            return _reply(cmd_id, ok=False, status="not_found", order_id=order_id,
                          client_order_id=coid, error=text)
        _finish_command(conn, cmd_id, "error", order_id=order_id, error=text)
        log.warning("cancel %s failed: %s", order_id, text)
        return _reply(cmd_id, ok=False, status="error", order_id=order_id, client_order_id=coid,
                      error=text)
    except Exception as e:
        text = _err_text(e)
        _finish_command(conn, cmd_id, "error", order_id=order_id, error=text)
        log.warning("cancel %s failed: %s", order_id, text)
        return _reply(cmd_id, ok=False, status="error", order_id=order_id, client_order_id=coid,
                      error=text)
    _finish_command(conn, cmd_id, "cancel_requested", order_id=order_id)
    log.info("cancel %s order=%s cancel_requested", account, order_id)
    return _reply(cmd_id, ok=True, status="cancel_requested", order_id=order_id,
                  client_order_id=coid, order=get_order_row(conn, order_id=order_id))


def handle_command(conn, r, payload: dict, stream_id: str | None = None) -> dict:
    """Execute one OMS command; commit; send and return the reply."""
    if not isinstance(payload, dict):
        payload = {}
    cmd_id = str(payload.get("cmd_id") or payload.get("req_id") or runtime.new_id())
    kind = _lower(payload.get("kind"))
    account = _lower(payload.get("account") or "paper")
    source = payload.get("source") or None
    if kind not in ("submit", "cancel"):
        reply = _reply(cmd_id, ok=False, status="rejected", error=f"unknown kind {kind!r}")
    elif account not in ("paper", "live"):
        reply = _reply(cmd_id, ok=False, status="rejected",
                       client_order_id=payload.get("client_order_id"),
                       error="account must be paper or live")
    else:
        if account == "live":
            log.warning("LIVE account command %s kind=%s", cmd_id, kind)
        if kind == "submit":
            reply = _do_submit(conn, cmd_id, payload, account, stream_id, source)
        else:
            reply = _do_cancel(conn, cmd_id, payload, account, stream_id, source)
    conn.commit()
    if r is not None:
        try:
            runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
        except Exception as e:
            log.error("reply send failed for %s: %s", cmd_id, e)
    return reply


# --------------------------------------------------------------------------- reconcile
def reconcile_accounts() -> tuple:
    accts = ["paper"]
    if os.environ.get("OMS_RECONCILE_LIVE", "") == "1":
        accts.append("live")
    return tuple(accts)


def reconcile(conn, r, accounts=("paper",), since_iso: str | None = None) -> int:
    total = 0
    after = since_iso or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    for account in accounts:
        live = account == "live"
        try:
            orders = list(trader.open_orders(live=live) or [])
            orders += list(trader.list_orders("closed", after=after, live=live) or [])
            n = 0
            for o in orders:
                if upsert_order(conn, o, account=account):
                    n += 1
            conn.commit()
            total += n
            log.info("reconcile account=%s upserted=%d", account, n)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.warning("reconcile account=%s failed: %s", account, _err_text(e))
            return 0
    if r is not None:
        try:
            runtime.redis_call(r.set, LAST_RECONCILE_KEY, runtime.now_iso())
        except Exception as e:
            log.warning("reconcile timestamp write failed: %s", e)
    return total


# --------------------------------------------------------------------------- consumers
def _parse_fields(fields) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        raise ValueError("missing json field")
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("json payload is not an object")
    return d


def process_updates(conn, reader) -> int:
    entries = reader.read()
    n = 0
    for eid, fields in entries:
        try:
            payload = _parse_fields(fields)
            handle_payload(conn, eid, payload)
            conn.commit()
            reader.ack(eid)
            n += 1
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.exception("trade update %s failed; dead-lettering", eid)
            try:
                reader.dead_letter(eid, fields, repr(e))
            except Exception:
                log.exception("dead_letter failed for %s", eid)
    return len(entries)


def process_commands(conn, r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        cmd_id = None
        try:
            payload = _parse_fields(fields)
            cmd_id = payload.get("cmd_id") or payload.get("req_id")
            handle_command(conn, r, payload, stream_id=eid)
            reader.ack(eid)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.exception("command %s (%s) failed; dead-lettering", eid, cmd_id)
            if cmd_id and r is not None:
                try:
                    runtime.send_reply(r, str(cmd_id), _reply(str(cmd_id), ok=False, status="error",
                                                              error=f"oms error: {e!r}"[:500]))
                except Exception:
                    log.exception("error reply failed for %s", cmd_id)
            try:
                reader.dead_letter(eid, fields, repr(e))
            except Exception:
                log.exception("dead_letter failed for %s", eid)
    return len(entries)


# --------------------------------------------------------------------------- worker
def body() -> None:
    r = runtime.get_redis()
    conn = runtime.sqlite_connect()
    try:
        setup_db(conn)
        consumer = os.environ.get("OMS_CONSUMER", "oms-1")
        try:
            interval = float(os.environ.get("OMS_RECONCILE_S", "30"))
        except ValueError:
            interval = 30.0
        accounts = reconcile_accounts()
        upd_reader = runtime.GroupReader(r, runtime.STREAM_TRADE_UPDATES, GROUP, consumer, block_ms=500)
        cmd_reader = runtime.GroupReader(r, runtime.STREAM_OMS_COMMANDS, GROUP, consumer, block_ms=500)
        log.info("oms started consumer=%s accounts=%s reconcile_s=%s", consumer, accounts, interval)
        reconcile(conn, r, accounts)
        last_rec = time.monotonic()
        while True:
            n_cmd = process_commands(conn, r, cmd_reader)
            n_upd = process_updates(conn, upd_reader)
            if time.monotonic() - last_rec >= interval:
                reconcile(conn, r, accounts)
                last_rec = time.monotonic()
            if not n_upd and not n_cmd:
                time.sleep(0.05)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    runtime.run_forever(body, NAME)


if __name__ == "__main__":
    main()
