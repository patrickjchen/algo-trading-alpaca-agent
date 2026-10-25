"""OMS -- Order Management System worker.

Front door for every order intent (alpaca:oms:commands) and the single source of truth
for order STATE (SQLite orders / fills / trade_events / commands). Makes NO Alpaca order
calls: execution is delegated to EMS over alpaca:ems:requests and the reply relayed.
Read-only Alpaca REST is used for reconciliation only.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import runtime
import trader

NAME = "oms"
GROUP = "oms"
LAST_RECONCILE_KEY = "alpaca:oms:last_reconcile"

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
  client_order_id TEXT, order_id TEXT, algo_id TEXT, slice_idx INTEGER, source TEXT,
  payload_json TEXT NOT NULL, status TEXT NOT NULL, error TEXT, received_at TEXT, completed_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cmd_coid ON commands(client_order_id) WHERE kind='submit';
"""

KINDS = ("submit", "cancel", "algo_create", "algo_cancel")
ACCOUNTS = ("paper", "live")
COID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
ALGO_COID_RE = re.compile(r"^algo-(.{12})-(\d+)$")
SUBMITTED_CMD_STATUSES = ("acked", "already_submitted", "unknown")
_TERMINAL_SQL = ",".join(f"'{s}'" for s in sorted(runtime.TERMINAL_STATUSES))


def setup_db(conn) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# --------------------------------------------------------------------------- helpers
def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _int(x):
    if x is None or x == "":
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _s(x):
    """Enum-ish value -> lowercase plain string (or None)."""
    if x is None:
        return None
    v = getattr(x, "value", x)
    v = str(v).strip()
    if not v:
        return None
    if "." in v and v.split(".", 1)[0][:1].isupper() and not v.replace(".", "").isdigit():
        v = v.rsplit(".", 1)[-1]
    return v.lower()


def _str(x):
    if x is None:
        return None
    v = getattr(x, "value", x)
    return str(v)


def _row_dict(cur, row):
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _fetchone(conn, sql, params=()):
    cur = conn.execute(sql, params)
    return _row_dict(cur, cur.fetchone())


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _ems_timeout() -> float:
    return _env_float("OMS_EMS_TIMEOUT", 10.0)


# --------------------------------------------------------------------------- order state
def upsert_order(conn, order: dict, *, account, source=None, algo_id=None, slice_idx=None,
                 event_ts=None) -> None:
    if not isinstance(order, dict):
        return
    oid = order.get("id") or order.get("order_id")
    if not oid:
        return
    oid = str(oid)
    coid = order.get("client_order_id")
    coid = str(coid) if coid else None
    if coid and (algo_id is None or slice_idx is None):
        m = ALGO_COID_RE.match(coid)
        if m:
            if algo_id is None:
                algo_id = m.group(1)
            if slice_idx is None:
                slice_idx = int(m.group(2))
    now = runtime.now_iso()
    otype = order.get("type") or order.get("order_type")
    row = {
        "order_id": oid,
        "client_order_id": coid,
        "account": account or "paper",
        "symbol": order.get("symbol"),
        "side": _s(order.get("side")),
        "type": _s(otype),
        "time_in_force": _s(order.get("time_in_force")),
        "qty": _f(order.get("qty")),
        "limit_price": _f(order.get("limit_price")),
        "filled_qty": _f(order.get("filled_qty")),
        "filled_avg_price": _f(order.get("filled_avg_price")),
        "status": _s(order.get("status")),
        "source": source,
        "algo_id": algo_id,
        "slice_idx": _int(slice_idx),
        "submitted_at": _str(order.get("submitted_at")),
        "created_at": _str(order.get("created_at")),
        "updated_at": _str(order.get("updated_at")) or now,
        "last_event_ts": _str(event_ts) if event_ts else None,
        "raw_json": json.dumps(order, default=str),
    }
    cols = list(row.keys())
    sql = f"""
    INSERT INTO orders({",".join(cols)}) VALUES({",".join("?" for _ in cols)})
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
      status = CASE
        WHEN excluded.status IS NULL THEN orders.status
        WHEN orders.status IN ({_TERMINAL_SQL}) AND excluded.status NOT IN ({_TERMINAL_SQL})
          THEN orders.status
        ELSE excluded.status END,
      source = COALESCE(orders.source, excluded.source),
      algo_id = COALESCE(orders.algo_id, excluded.algo_id),
      slice_idx = COALESCE(orders.slice_idx, excluded.slice_idx),
      submitted_at = COALESCE(excluded.submitted_at, orders.submitted_at),
      created_at = COALESCE(orders.created_at, excluded.created_at),
      updated_at = excluded.updated_at,
      last_event_ts = COALESCE(excluded.last_event_ts, orders.last_event_ts),
      raw_json = excluded.raw_json
    """
    try:
        conn.execute(sql, [row[c] for c in cols])
    except sqlite3.IntegrityError as e:
        log.warning("upsert_order conflict order=%s coid=%s: %s", oid, coid, e)


def handle_payload(conn, stream_id: str, payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    event = _s(payload.get("event"))
    account = payload.get("account") or "paper"
    ts = payload.get("ts") or runtime.now_iso()
    order = payload.get("order")
    order_id = payload.get("order_id")
    if not order_id and isinstance(order, dict):
        order_id = order.get("id")
    order_id = str(order_id) if order_id else None
    raw = json.dumps(payload, default=str)
    conn.execute("INSERT OR IGNORE INTO trade_events(stream_id, event, order_id, account, ts, raw_json)"
                 " VALUES(?,?,?,?,?,?)", (str(stream_id), event, order_id, account, ts, raw))
    if isinstance(order, dict) and order:
        upsert_order(conn, order, account=account, source=payload.get("source"),
                     algo_id=payload.get("algo_id"), slice_idx=payload.get("slice_idx"),
                     event_ts=ts)
    exec_id = payload.get("execution_id")
    if event in ("fill", "partial_fill") and exec_id:
        algo_id = payload.get("algo_id")
        if not algo_id and order_id:
            row = _fetchone(conn, "SELECT algo_id FROM orders WHERE order_id=?", (order_id,))
            algo_id = row.get("algo_id") if row else None
        symbol = payload.get("symbol") or (order.get("symbol") if isinstance(order, dict) else None)
        side = _s(payload.get("side") or (order.get("side") if isinstance(order, dict) else None))
        conn.execute(
            "INSERT OR IGNORE INTO fills(execution_id, order_id, account, symbol, side, price, qty,"
            " event, ts, algo_id, raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (str(exec_id), order_id, account, symbol, side, _f(payload.get("price")),
             _f(payload.get("qty")), event, ts, algo_id, raw))


# --------------------------------------------------------------------------- commands
def forward_to_ems(r, payload: dict, timeout: float = 10.0) -> dict:
    req = dict(payload)
    req["cmd_id"] = runtime.new_id()
    return runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, req, timeout)


def _order_row_by_coid(conn, coid):
    if not coid:
        return None
    return _fetchone(conn, "SELECT * FROM orders WHERE client_order_id=?", (coid,))


def _order_row_by_id(conn, oid):
    if not oid:
        return None
    return _fetchone(conn, "SELECT * FROM orders WHERE order_id=?", (str(oid),))


def _reply_from_cmd_row(conn, row: dict) -> dict:
    kind = row.get("kind")
    status = row.get("status")
    if kind in ("algo_create", "algo_cancel"):
        algo = None
        try:
            algo = _fetchone(conn, "SELECT * FROM algo_orders WHERE algo_id=?", (row.get("algo_id"),)) \
                if row.get("algo_id") else None
        except sqlite3.Error:
            algo = None
        return {"ok": status not in ("rejected", "error", "unknown", "not_found", "received"),
                "status": status, "cmd_id": row.get("cmd_id"), "algo_id": row.get("algo_id"),
                "algo": algo, "error": row.get("error")}
    order = _order_row_by_id(conn, row.get("order_id")) or _order_row_by_coid(
        conn, row.get("client_order_id"))
    ok = status in ("acked", "already_submitted", "cancel_requested")
    return {"ok": ok, "status": status, "cmd_id": row.get("cmd_id"),
            "order_id": row.get("order_id") or (order or {}).get("order_id"),
            "client_order_id": row.get("client_order_id") or (order or {}).get("client_order_id"),
            "order": order, "error": row.get("error")}


def _finish(conn, cmd_id, status, error=None, order_id=None, algo_id=None):
    conn.execute("UPDATE commands SET status=?, error=?, order_id=COALESCE(?, order_id),"
                 " algo_id=COALESCE(?, algo_id), completed_at=? WHERE cmd_id=?",
                 (status, str(error)[:1000] if error else None, order_id, algo_id,
                  runtime.now_iso(), cmd_id))
    conn.commit()


def _order_reply(cmd_id, status, ok=False, order_id=None, coid=None, order=None, error=None):
    return {"ok": bool(ok), "status": status, "cmd_id": cmd_id, "order_id": order_id,
            "client_order_id": coid, "order": order, "error": error}


def _algo_reply(cmd_id, status, ok=False, algo_id=None, algo=None, error=None):
    return {"ok": bool(ok), "status": status, "cmd_id": cmd_id, "algo_id": algo_id,
            "algo": algo, "error": error}


def _err_reply(kind, cmd_id, status, error):
    if kind in ("algo_create", "algo_cancel"):
        return _algo_reply(cmd_id, status, False, error=error)
    return _order_reply(cmd_id, status, False, error=error)


def _insert_command(conn, cmd_id, stream_id, kind, account, payload) -> bool:
    """Insert the audit row. Returns True if this submit duplicates an already-submitted coid."""
    coid = payload.get("client_order_id")
    coid = str(coid) if coid else None
    dup = False
    if kind == "submit" and coid:
        prior = _fetchone(conn, "SELECT cmd_id, status FROM commands WHERE kind='submit'"
                                " AND client_order_id=? AND cmd_id<>?", (coid, cmd_id))
        if prior:
            if prior["status"] in SUBMITTED_CMD_STATUSES:
                dup = True
            else:
                # failed / stale earlier attempt: release the coid so this attempt owns it
                conn.execute("UPDATE commands SET client_order_id=NULL WHERE cmd_id=?",
                             (prior["cmd_id"],))
    conn.execute(
        "INSERT OR IGNORE INTO commands(cmd_id, stream_id, kind, account, client_order_id, order_id,"
        " algo_id, slice_idx, source, payload_json, status, received_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (cmd_id, stream_id, kind, account, None if dup else coid,
         _str(payload.get("order_id")), _str(payload.get("algo_id")), _int(payload.get("slice_idx")),
         _str(payload.get("source")), json.dumps(payload, default=str), "received",
         runtime.now_iso()))
    return dup


def handle_command(conn, r, payload: dict, stream_id: str | None = None) -> dict:
    payload = dict(payload or {})
    cmd_id = str(payload.get("cmd_id") or runtime.new_id())
    payload["cmd_id"] = cmd_id
    kind = payload.get("kind")
    try:
        reply = _handle_command(conn, r, payload, cmd_id, stream_id)
    except Exception as e:
        log.exception("command %s (%s) failed", cmd_id, kind)
        try:
            conn.rollback()
        except Exception:
            pass
        reply = _err_reply(kind, cmd_id, "error", f"{type(e).__name__}: {e}")
        try:
            _finish(conn, cmd_id, "error", reply["error"])
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    if r is not None:
        try:
            runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
        except Exception as e:
            log.error("send_reply failed cmd=%s: %s", cmd_id, e)
    log.info("cmd %s kind=%s -> %s %s", cmd_id[:12], kind, reply.get("status"),
             reply.get("error") or "")
    return reply


def _handle_command(conn, r, payload, cmd_id, stream_id) -> dict:
    existing = _fetchone(conn, "SELECT * FROM commands WHERE cmd_id=?", (cmd_id,))
    if existing and existing.get("status") != "received":
        return _reply_from_cmd_row(conn, existing)

    kind = payload.get("kind")
    kind_s = str(kind) if kind else "invalid"
    account = str(payload.get("account") or "paper").lower()
    dup = False
    if not existing:
        dup = _insert_command(conn, cmd_id, stream_id, kind_s, account, payload)
        conn.commit()

    if kind not in KINDS:
        reply = _err_reply(None, cmd_id, "rejected", f"invalid kind {kind!r}")
        _finish(conn, cmd_id, "rejected", reply["error"])
        return reply
    if account not in ACCOUNTS:
        reply = _err_reply(kind, cmd_id, "rejected", f"invalid account {account!r}")
        _finish(conn, cmd_id, "rejected", reply["error"])
        return reply
    payload["account"] = account

    if kind == "submit":
        return _cmd_submit(conn, r, payload, cmd_id, dup)
    if kind == "cancel":
        return _cmd_cancel(conn, r, payload, cmd_id)
    if kind == "algo_create":
        return _cmd_algo_create(conn, r, payload, cmd_id)
    return _cmd_algo_cancel(conn, r, payload, cmd_id)


def _ems_down(r) -> bool:
    if r is None:
        return True
    return runtime.worker_alive(r, "ems") is None


def _cmd_submit(conn, r, p, cmd_id, dup) -> dict:
    account = p["account"]
    coid = p.get("client_order_id")
    coid = str(coid) if coid else None
    side = str(p.get("side") or "").lower()
    otype = str(p.get("type") or "market").lower()
    tif = str(p.get("tif") or "gtc").lower()
    qty = _f(p.get("qty"))
    limit_price = _f(p.get("limit_price"))
    symbol = p.get("symbol")
    source = p.get("source")

    def reject(msg):
        _finish(conn, cmd_id, "rejected", msg)
        return _order_reply(cmd_id, "rejected", False, coid=coid, error=msg)

    if not coid or not COID_RE.match(coid):
        return reject("client_order_id missing or invalid (^[A-Za-z0-9_-]{1,48}$)")
    if not symbol or not isinstance(symbol, str):
        return reject("symbol required")
    if side not in ("buy", "sell"):
        return reject("side must be buy or sell")
    if otype not in ("market", "limit"):
        return reject("type must be market or limit")
    if qty is None or qty <= 0:
        return reject("qty must be > 0")
    if otype == "limit":
        if limit_price is None or limit_price <= 0:
            return reject("limit order requires limit_price > 0")
    else:
        limit_price = None
    if tif not in ("gtc", "ioc"):
        return reject("tif must be gtc or ioc")

    # idempotency
    row = _order_row_by_coid(conn, coid)
    prior = None
    if row is None:
        prior = _fetchone(conn, "SELECT * FROM commands WHERE kind='submit' AND cmd_id<>? AND"
                                " status IN ('acked','already_submitted','unknown') AND"
                                " (client_order_id=? OR (client_order_id IS NULL AND"
                                " json_extract(payload_json,'$.client_order_id')=?))"
                                " ORDER BY received_at LIMIT 1", (cmd_id, coid, coid))
    if row is not None or prior is not None or dup:
        oid = (row or {}).get("order_id") or (prior or {}).get("order_id")
        _finish(conn, cmd_id, "already_submitted", None, order_id=oid)
        return _order_reply(cmd_id, "already_submitted", True, order_id=oid, coid=coid, order=row)

    if _ems_down(r):
        _finish(conn, cmd_id, "error", "EMS not running")
        return _order_reply(cmd_id, "error", False, coid=coid, error="EMS not running")

    req = {"kind": "submit", "account": account, "client_order_id": coid, "symbol": symbol,
           "side": side, "type": otype, "qty": qty, "limit_price": limit_price, "tif": tif,
           "source": source}
    if p.get("algo_id") is not None:
        req["algo_id"] = p.get("algo_id")
    if p.get("slice_idx") is not None:
        req["slice_idx"] = p.get("slice_idx")
    try:
        er = forward_to_ems(r, req, _ems_timeout())
    except Exception as e:
        msg = f"forward failed: {type(e).__name__}: {e}"
        _finish(conn, cmd_id, "error", msg)
        return _order_reply(cmd_id, "error", False, coid=coid, error=msg)
    if not isinstance(er, dict):
        er = {"status": "error", "error": "bad EMS reply"}
    st = er.get("status")
    order = er.get("order") if isinstance(er.get("order"), dict) else None
    oid = er.get("order_id") or (order or {}).get("id")
    oid = str(oid) if oid else None
    err = er.get("error")
    if st in ("acked", "already_submitted"):
        if order:
            upsert_order(conn, order, account=account, source=source,
                         algo_id=p.get("algo_id"), slice_idx=p.get("slice_idx"))
        _finish(conn, cmd_id, st, None, order_id=oid)
        row = _order_row_by_id(conn, oid) or _order_row_by_coid(conn, coid)
        return _order_reply(cmd_id, st, True, order_id=oid, coid=coid, order=order or row)
    if st == "timeout":
        st, err = "unknown", "EMS did not reply"
    elif st not in ("rejected", "error", "unknown"):
        err = err or f"unexpected EMS status {st!r}"
        st = "error"
    _finish(conn, cmd_id, st, err, order_id=oid)
    return _order_reply(cmd_id, st, False, order_id=oid, coid=coid, order=order, error=err)


def _cmd_cancel(conn, r, p, cmd_id) -> dict:
    account = p["account"]
    oid = p.get("order_id")
    oid = str(oid) if oid else None
    coid = p.get("client_order_id")
    coid = str(coid) if coid else None
    row = _order_row_by_id(conn, oid) if oid else None
    if row is None and coid:
        row = _order_row_by_coid(conn, coid)
    if row is not None:
        oid = oid or row.get("order_id")
        coid = coid or row.get("client_order_id")
        account = row.get("account") or account
    if not oid:
        _finish(conn, cmd_id, "not_found", "order not found")
        return _order_reply(cmd_id, "not_found", False, coid=coid, error="order not found")
    if _ems_down(r):
        _finish(conn, cmd_id, "error", "EMS not running", order_id=oid)
        return _order_reply(cmd_id, "error", False, order_id=oid, coid=coid, order=row,
                            error="EMS not running")
    req = {"kind": "cancel_order", "account": account, "order_id": oid, "client_order_id": coid,
           "source": p.get("source")}
    try:
        er = forward_to_ems(r, req, _ems_timeout())
    except Exception as e:
        msg = f"forward failed: {type(e).__name__}: {e}"
        _finish(conn, cmd_id, "error", msg, order_id=oid)
        return _order_reply(cmd_id, "error", False, order_id=oid, coid=coid, order=row, error=msg)
    if not isinstance(er, dict):
        er = {"status": "error", "error": "bad EMS reply"}
    st = er.get("status")
    err = er.get("error")
    if st == "cancel_requested":
        conn.execute(f"UPDATE orders SET status='pending_cancel', updated_at=? WHERE order_id=?"
                     f" AND (status IS NULL OR status NOT IN ({_TERMINAL_SQL}))",
                     (runtime.now_iso(), oid))
        _finish(conn, cmd_id, st, None, order_id=oid)
        return _order_reply(cmd_id, st, True, order_id=oid, coid=coid,
                            order=_order_row_by_id(conn, oid))
    if st == "timeout":
        st, err = "error", "EMS did not reply"
    elif st not in ("not_found", "error"):
        err = err or f"unexpected EMS status {st!r}"
        st = "error"
    _finish(conn, cmd_id, st, err, order_id=oid)
    return _order_reply(cmd_id, st, False, order_id=oid, coid=coid,
                        order=_order_row_by_id(conn, oid), error=err)


def _relay_algo(conn, r, cmd_id, req, algo_id=None) -> dict:
    if _ems_down(r):
        _finish(conn, cmd_id, "error", "EMS not running", algo_id=algo_id)
        return _algo_reply(cmd_id, "error", False, algo_id=algo_id, error="EMS not running")
    try:
        er = forward_to_ems(r, req, _ems_timeout())
    except Exception as e:
        msg = f"forward failed: {type(e).__name__}: {e}"
        _finish(conn, cmd_id, "error", msg, algo_id=algo_id)
        return _algo_reply(cmd_id, "error", False, algo_id=algo_id, error=msg)
    if not isinstance(er, dict):
        er = {"ok": False, "status": "error", "error": "bad EMS reply"}
    st = er.get("status") or ("error" if not er.get("ok") else "acked")
    err = er.get("error")
    ok = bool(er.get("ok"))
    if st == "timeout":
        st, err, ok = "error", "EMS did not reply", False
    aid = er.get("algo_id") or algo_id
    algo = er.get("algo")
    if not aid and isinstance(algo, dict):
        aid = algo.get("algo_id")
    _finish(conn, cmd_id, str(st), err, algo_id=aid)
    return _algo_reply(cmd_id, st, ok, algo_id=aid, algo=algo, error=err)


def _cmd_algo_create(conn, r, p, cmd_id) -> dict:
    strategy = p.get("strategy")
    side = str(p.get("side") or "").lower()
    total_qty = _f(p.get("total_qty"))

    def reject(msg):
        _finish(conn, cmd_id, "rejected", msg)
        return _algo_reply(cmd_id, "rejected", False, error=msg)

    if not isinstance(strategy, str) or not strategy:
        return reject("strategy must be a string")
    if side not in ("buy", "sell"):
        return reject("side must be buy or sell")
    if total_qty is None or total_qty <= 0:
        return reject("total_qty must be > 0")
    req = {"kind": "create", "strategy": strategy, "account": p["account"],
           "symbol": p.get("symbol"), "side": side, "total_qty": total_qty,
           "duration_s": p.get("duration_s"), "n_slices": p.get("n_slices"),
           "limit_cap": p.get("limit_cap"), "params": p.get("params") or {},
           "source": p.get("source")}
    return _relay_algo(conn, r, cmd_id, req)


def _cmd_algo_cancel(conn, r, p, cmd_id) -> dict:
    algo_id = p.get("algo_id")
    if not algo_id:
        _finish(conn, cmd_id, "rejected", "algo_id required")
        return _algo_reply(cmd_id, "rejected", False, error="algo_id required")
    algo_id = str(algo_id)
    req = {"kind": "cancel", "algo_id": algo_id, "source": p.get("source")}
    return _relay_algo(conn, r, cmd_id, req, algo_id=algo_id)


# --------------------------------------------------------------------------- reconcile
def reconcile(conn, r, accounts=("paper",), since_iso: str | None = None) -> int:
    total = 0
    after = since_iso or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    try:
        for account in accounts:
            live = account == "live"
            orders = list(trader.open_orders(live=live) or [])
            orders += list(trader.list_orders("closed", after=after, live=live) or [])
            for o in orders:
                if isinstance(o, dict):
                    upsert_order(conn, o, account=account)
                    total += 1
            conn.commit()
        if r is not None:
            runtime.redis_call(r.set, LAST_RECONCILE_KEY, runtime.now_iso())
        log.info("reconciled %d orders accounts=%s", total, ",".join(accounts))
        return total
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        transient = False
        try:
            transient = trader.is_transient(e)
        except Exception:
            pass
        log.warning("reconcile failed (%s%s): %s", type(e).__name__,
                    ", transient" if transient else "", str(e)[:300])
        return 0


# --------------------------------------------------------------------------- loops
def _parse_fields(fields) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        raise ValueError("entry has no 'json' field")
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("payload is not a JSON object")
    return d


def process_updates(conn, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        try:
            payload = _parse_fields(fields)
            handle_payload(conn, eid, payload)
            conn.commit()
            reader.ack(eid)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.exception("trade update %s failed; dead-lettering", eid)
            reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
    return len(entries)


def process_commands(conn, r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        cmd_id = None
        kind = None
        try:
            payload = _parse_fields(fields)
            cmd_id = payload.get("cmd_id")
            kind = payload.get("kind")
            handle_command(conn, r, payload, eid)
            reader.ack(eid)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.exception("command entry %s failed; dead-lettering", eid)
            if cmd_id and r is not None:
                try:
                    runtime.send_reply(r, str(cmd_id),
                                       _err_reply(kind, str(cmd_id), "error",
                                                  f"{type(e).__name__}: {e}"))
                except Exception:
                    pass
            reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
    return len(entries)


def _reconcile_accounts():
    if os.environ.get("OMS_RECONCILE_LIVE", "") == "1":
        return ("paper", "live")
    return ("paper",)


def body() -> None:
    r = runtime.get_redis()
    conn = runtime.sqlite_connect()
    try:
        setup_db(conn)
        consumer = os.environ.get("OMS_CONSUMER", "oms-1")
        upd_reader = runtime.GroupReader(r, runtime.STREAM_TRADE_UPDATES, GROUP, consumer,
                                         block_ms=500)
        cmd_reader = runtime.GroupReader(r, runtime.STREAM_OMS_COMMANDS, GROUP, consumer,
                                         block_ms=500)
        interval = max(1.0, _env_float("OMS_RECONCILE_S", 30.0))
        accounts = _reconcile_accounts()
        log.info("oms started consumer=%s reconcile=%ss accounts=%s", consumer, interval, accounts)
        reconcile(conn, r, accounts)
        next_rec = time.monotonic() + interval
        while True:
            n_cmd = process_commands(conn, r, cmd_reader)
            n_upd = process_updates(conn, upd_reader)
            if time.monotonic() >= next_rec:
                reconcile(conn, r, accounts)
                next_rec = time.monotonic() + interval
            if not n_cmd and not n_upd:
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
