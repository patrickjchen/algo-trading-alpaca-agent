"""EMS -- Execution Management System worker.

The ONLY process that calls Alpaca order endpoints (through trader.py). It serves the
requests OMS forwards on alpaca:ems:requests (submit / cancel_order / create / cancel),
publishes an EXECUTION REPORT to alpaca:trade_updates for every submit/cancel it performs,
and runs the algo engine (TWAP) whose state lives in algo_orders / algo_slices.

It never writes the OMS tables (orders, fills, trade_events, commands); it only reads them.
Nothing here performs I/O at import time.
"""
from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

import algos
import runtime
import trader

try:
    from alpaca.common.exceptions import APIError
except Exception:  # pragma: no cover
    class APIError(Exception):
        pass

NAME = "ems"
GROUP = "ems"

log = runtime.log(NAME)

SCHEMA = """
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

COID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
ACCOUNTS = ("paper", "live")
RETRY_DELAYS = (0.5, 1.0, 2.0)
UNKNOWN_LOOKUP_AFTER_S = 10.0
FINAL_GRACE_S = 5.0
QUOTE_GIVE_UP_AFTER_END_S = 60.0
ALGO_ACTIVE = ("pending", "running")
ALGO_TERMINAL = frozenset({"completed", "partial", "cancelled", "failed"})
SLICE_TERMINAL = frozenset({"filled", "rolled", "cancelled", "failed"})
SLICE_OPEN = ("submitted", "working")

_last_lookup: dict = {}


def setup_db(conn) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# --------------------------------------------------------------------------- helpers
def _f(x):
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="milliseconds")


def _ts(v) -> float:
    """ISO string or epoch (str/float) -> epoch seconds. Unparseable -> 0."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    try:
        return float(s)
    except ValueError:
        pass
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _s(x):
    if x is None:
        return None
    v = getattr(x, "value", x)
    v = str(v).strip()
    if "." in v and v.split(".", 1)[0][:1].isupper() and not v.replace(".", "").isdigit():
        v = v.rsplit(".", 1)[-1]
    return v.lower() or None


def _rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    out = []
    cols = [d[0] for d in cur.description] if cur.description else []
    for row in cur.fetchall():
        out.append(dict(row) if isinstance(row, sqlite3.Row) else dict(zip(cols, row)))
    return out


def _row(conn, sql, params=()):
    rs = _rows(conn, sql, params)
    return rs[0] if rs else None


def _oms_rows(conn, sql, params=()):
    """Read an OMS-owned table; missing table -> []."""
    try:
        return _rows(conn, sql, params)
    except sqlite3.OperationalError as e:
        log.debug("oms table read failed: %s", e)
        return []


def _order_row(conn, col, val):
    if not val or col not in ("order_id", "client_order_id"):
        return None
    rs = _oms_rows(conn, f"SELECT * FROM orders WHERE {col}=?", (str(val),))
    return rs[0] if rs else None


def _status_code(e):
    try:
        sc = e.status_code
        return int(sc) if sc is not None else None
    except Exception:
        return None


def _is_api_error(e) -> bool:
    return isinstance(e, APIError) or _status_code(e) is not None


def _err_text(e) -> str:
    if e is None:
        return "unknown error"
    msg = None
    try:
        info = trader.error_info(e)
        msg = info.get("message")
    except Exception:
        msg = None
    msg = msg or str(e)
    sc = _status_code(e) if _is_api_error(e) else None
    prefix = f"{type(e).__name__}" + (f" {sc}" if sc else "")
    return f"{prefix}: {str(msg)[:500]}"


def _is_dup_coid(e) -> bool:
    if _status_code(e) != 422:
        return False
    text = (str(e) + " " + _err_text(e)).lower()
    return "client_order_id" in text


def _is_transient(e) -> bool:
    try:
        return bool(trader.is_transient(e))
    except Exception:
        return False


def _upd_slice(conn, algo_id, idx, **fields) -> None:
    fields["updated_at"] = runtime.now_iso()
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE algo_slices SET {cols} WHERE algo_id=? AND slice_idx=?",
                 [*fields.values(), algo_id, int(idx)])


def _upd_algo(conn, algo_id, **fields) -> None:
    fields["updated_at"] = runtime.now_iso()
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE algo_orders SET {cols} WHERE algo_id=?", [*fields.values(), algo_id])


def _get_algo(conn, algo_id):
    return _row(conn, "SELECT * FROM algo_orders WHERE algo_id=?", (algo_id,))


def _get_slices(conn, algo_id):
    return _rows(conn, "SELECT * FROM algo_slices WHERE algo_id=? ORDER BY slice_idx", (algo_id,))


def _strategy_for(algo):
    params = {}
    try:
        params = json.loads(algo.get("params_json") or "{}") or {}
    except Exception:
        params = {}
    return algos.get_strategy(algo.get("strategy") or "twap", params)


def _rules_for(symbol, live, r):
    try:
        return algos.rules_from_dict(trader.get_asset_rules(symbol, live, r=r))
    except Exception as e:
        log.warning("asset rules unavailable for %s: %s", symbol, str(e)[:200])
        return algos.AssetRules()


# --------------------------------------------------------------------------- execution gateway
def publish_report(r, event: str, order, *, account, client_order_id=None, order_id=None,
                   symbol=None, side=None, qty=None, source=None, algo_id=None,
                   slice_idx=None) -> None:
    try:
        if r is None:
            return
        o = order if isinstance(order, dict) else None
        if o:
            order_id = order_id or o.get("id") or o.get("order_id")
            client_order_id = client_order_id or o.get("client_order_id")
            symbol = symbol or o.get("symbol")
            side = side or _s(o.get("side"))
            if qty is None:
                qty = _f(o.get("qty"))
        payload = {
            "event": event, "execution_id": None,
            "order_id": str(order_id) if order_id else None,
            "client_order_id": client_order_id, "symbol": symbol, "side": side,
            "price": None, "qty": qty, "position_qty": None, "ts": runtime.now_iso(),
            "account": account or "paper", "order": o, "source": source,
            "algo_id": algo_id, "slice_idx": slice_idx,
        }
        runtime.redis_call(r.xadd, runtime.STREAM_TRADE_UPDATES,
                           {"json": json.dumps(payload, default=str)},
                           maxlen=100000, approximate=True)
    except Exception as e:
        log.error("publish_report %s failed order=%s coid=%s: %s", event, order_id,
                  client_order_id, e)


def execute_submit(conn, r, p: dict) -> dict:
    p = dict(p or {})
    coid = str(p.get("client_order_id") or "")
    account = str(p.get("account") or "paper").lower()
    symbol = p.get("symbol")
    side = str(p.get("side") or "").lower()
    otype = str(p.get("type") or "market").lower()
    tif = str(p.get("tif") or "gtc").lower()
    qty = _f(p.get("qty"))
    lp = _f(p.get("limit_price"))
    source = p.get("source")
    algo_id = p.get("algo_id")
    slice_idx = p.get("slice_idx")

    def out(ok, status, order=None, order_id=None, error=None):
        oid = order_id or ((order or {}).get("id") if isinstance(order, dict) else None)
        return {"ok": bool(ok), "status": status, "order_id": str(oid) if oid else None,
                "client_order_id": coid or None, "order": order, "error": error}

    err = None
    if not COID_RE.match(coid):
        err = "client_order_id missing or invalid (^[A-Za-z0-9_-]{1,48}$)"
    elif account not in ACCOUNTS:
        err = f"invalid account {account!r}"
    elif not symbol or not isinstance(symbol, str):
        err = "symbol required"
    elif side not in ("buy", "sell"):
        err = "side must be buy or sell"
    elif otype not in ("market", "limit"):
        err = "type must be market or limit"
    elif qty is None or qty <= 0:
        err = "qty must be > 0"
    elif otype == "limit" and (lp is None or lp <= 0):
        err = "limit order requires limit_price > 0"
    elif tif not in ("gtc", "ioc"):
        err = "tif must be gtc or ioc"
    if err:
        log.warning("submit rejected coid=%s: %s", coid, err)
        return out(False, "rejected", error=err)

    live = account == "live"

    def call():
        if otype == "limit":
            return trader.submit_limit(symbol, qty, side, lp, coid, live=live, tif=tif)
        return trader.submit_market(symbol, qty, side, coid, live=live, tif=tif)

    def found(order, status):
        if not isinstance(order, dict):
            order = trader._to_dict(order) if order is not None else {}
        publish_report(r, "ems_submitted", order, account=account, client_order_id=coid,
                       source=source, algo_id=algo_id, slice_idx=slice_idx)
        log.info("submit %s coid=%s %s %s %s @%s -> %s", account, coid, side, qty, symbol, lp,
                 status)
        return out(True, status, order=order)

    def lookup():
        try:
            return trader.get_order_by_client_id(coid, live)
        except Exception as le:
            log.warning("lookup by coid %s failed: %s", coid, str(le)[:200])
            return None

    last = None
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            order = call()
        except Exception as e:
            if _is_dup_coid(e):
                existing = lookup()
                if existing:
                    return found(existing, "already_submitted")
                return out(False, "rejected", error=_err_text(e))
            if _is_transient(e):
                last = e
                if attempt < len(RETRY_DELAYS):
                    log.warning("transient submit error coid=%s (attempt %d): %s", coid,
                                attempt + 1, str(e)[:200])
                    time.sleep(RETRY_DELAYS[attempt])
                    continue
                break
            if _is_api_error(e):
                log.warning("submit rejected by API coid=%s: %s", coid, _err_text(e))
                return out(False, "rejected", error=_err_text(e))
            log.warning("submit error coid=%s: %s", coid, _err_text(e))
            return out(False, "error", error=_err_text(e))
        return found(order, "acked")

    existing = lookup()
    if existing:
        return found(existing, "acked")
    log.error("submit outcome unknown coid=%s: %s", coid, _err_text(last))
    return out(False, "unknown", error=_err_text(last))


def execute_cancel(conn, r, p: dict) -> dict:
    p = dict(p or {})
    oid = str(p["order_id"]) if p.get("order_id") else None
    coid = str(p["client_order_id"]) if p.get("client_order_id") else None
    account = p.get("account")
    source = p.get("source")

    def out(ok, status, error=None):
        return {"ok": bool(ok), "status": status, "order_id": oid, "client_order_id": coid,
                "error": error}

    row = _order_row(conn, "order_id", oid) if oid else None
    if row is None and coid:
        row = _order_row(conn, "client_order_id", coid)
    if row:
        oid = oid or row.get("order_id")
        coid = coid or row.get("client_order_id")
        account = account or row.get("account")
    account = str(account or "paper").lower()
    live = account == "live"
    order = None
    if row:
        order = {k: v for k, v in row.items() if k not in ("raw_json", "status")}
    if not oid and coid:
        try:
            o = trader.get_order_by_client_id(coid, live)
        except Exception as e:
            return out(False, "error", _err_text(e))
        if o:
            order = o
            oid = str(o.get("id") or o.get("order_id") or "") or None
    if not oid:
        return out(False, "not_found", "order not found")
    try:
        trader.cancel(oid, live)
    except Exception as e:
        sc = _status_code(e)
        text = _err_text(e)
        if sc in (404, 422) or (_is_api_error(e) and "not found" in text.lower()):
            log.info("cancel %s: not found / not cancelable: %s", oid, text)
            return out(False, "not_found", text)
        log.warning("cancel %s failed: %s", oid, text)
        return out(False, "error", text)
    publish_report(r, "ems_cancel_requested", order, account=account, order_id=oid,
                   client_order_id=coid, source=source)
    log.info("cancel requested %s order=%s coid=%s", account, oid, coid)
    return out(True, "cancel_requested")


# --------------------------------------------------------------------------- requests
def _algo_out(ok, status, algo_id=None, algo=None, error=None, **extra):
    d = {"ok": bool(ok), "status": status, "algo_id": algo_id, "algo": algo, "error": error}
    d.update(extra)
    return d


def _create_algo(conn, r, p: dict) -> dict:
    def rej(msg):
        log.warning("algo create rejected: %s", msg)
        return _algo_out(False, "rejected", error=msg)

    strategy_id = str(p.get("strategy") or "twap").strip().lower()
    cls = algos.REGISTRY.get(strategy_id)
    if cls is None:
        return rej(f"unknown strategy {strategy_id!r}; valid: {', '.join(algos.REGISTRY)}")
    if not getattr(cls, "available", False):
        return rej(f"strategy {strategy_id!r} not available: {getattr(cls, 'note', '')}")
    side = str(p.get("side") or "").lower()
    if side not in ("buy", "sell"):
        return rej("side must be buy or sell")
    total_qty = _f(p.get("total_qty"))
    if total_qty is None or total_qty <= 0:
        return rej("total_qty must be > 0")
    account = str(p.get("account") or "paper").lower()
    if account not in ACCOUNTS:
        return rej(f"invalid account {account!r}")
    symbol = p.get("symbol")
    if not symbol or not isinstance(symbol, str):
        return rej("symbol required")
    symbol = symbol.strip().upper()
    try:
        n_slices = int(p.get("n_slices") or 5)
    except (TypeError, ValueError):
        return rej("n_slices must be an integer")
    if n_slices < 1:
        return rej("n_slices must be >= 1")
    duration_s = _f(p.get("duration_s")) if p.get("duration_s") not in (None, "") else 300.0
    if duration_s is None or duration_s <= 0:
        return rej("duration_s must be > 0")
    if duration_s < 10 * n_slices:
        return rej(f"duration_s must be >= 10*n_slices ({10 * n_slices}s)")
    limit_cap = None
    if p.get("limit_cap") not in (None, ""):
        limit_cap = _f(p.get("limit_cap"))
        if limit_cap is None or limit_cap <= 0:
            return rej("limit_cap must be > 0")
    params = p.get("params") or {}
    if isinstance(params, str):
        try:
            params = json.loads(params) if params.strip() else {}
        except ValueError:
            return rej("params must be a JSON object")
    if not isinstance(params, dict):
        return rej("params must be an object")
    live = account == "live"
    try:
        rules_d = trader.get_asset_rules(symbol, live, r=r)
    except Exception as e:
        log.warning("asset rules unavailable for %s: %s", symbol, str(e)[:200])
        rules_d = None
    if isinstance(rules_d, dict) and rules_d.get("tradable") is False:
        return rej(f"{symbol} is not tradable")
    rules = algos.rules_from_dict(rules_d)
    start = runtime.utc_ts() + 2
    end = start + duration_s
    try:
        slices = algos.build_schedule(strategy_id, total_qty, start, end, n_slices, rules, params)
    except (ValueError, NotImplementedError) as e:
        return rej(str(e))
    if not slices:
        return rej("empty schedule")
    total_r = algos.round_qty(total_qty, rules.min_trade_increment)
    algo_id = runtime.new_id()
    now = runtime.now_iso()
    conn.execute(
        "INSERT INTO algo_orders(algo_id, strategy, account, symbol, side, total_qty, n_slices,"
        " start_ts, end_ts, limit_cap, params_json, source, status, filled_qty, avg_px, error,"
        " created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (algo_id, strategy_id, account, symbol, side, total_r, len(slices), _iso(start),
         _iso(end), limit_cap, json.dumps(params, default=str), p.get("source"), "pending", 0.0,
         None, None, now, now))
    for s in slices:
        conn.execute(
            "INSERT INTO algo_slices(algo_id, slice_idx, scheduled_ts, qty, client_order_id,"
            " order_id, limit_price, status, filled_qty, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (algo_id, int(s.idx), _iso(s.scheduled_ts), float(s.qty),
             f"algo-{algo_id[:12]}-{int(s.idx):03d}", None, None, "scheduled", 0.0, now))
    conn.commit()
    algo = _get_algo(conn, algo_id)
    log.info("algo %s created %s %s %s %s over %.0fs in %d slices (%s)", algo_id, strategy_id,
             side, total_r, symbol, duration_s, len(slices), account)
    return _algo_out(True, "pending", algo_id=algo_id, algo=algo,
                     slices=_get_slices(conn, algo_id))


def _cancel_open_slices(conn, r, algo, slices, mark="cancelled") -> None:
    for s in slices:
        st = s.get("status")
        if st in SLICE_OPEN:
            res = execute_cancel(conn, r, {"account": algo.get("account"),
                                           "order_id": s.get("order_id"),
                                           "client_order_id": s.get("client_order_id"),
                                           "source": "ems"})
            if res.get("status") in ("cancel_requested", "not_found"):
                _upd_slice(conn, algo["algo_id"], s["slice_idx"], status=mark)
            else:
                log.warning("algo %s slice %s cancel failed: %s", algo["algo_id"],
                            s["slice_idx"], res.get("error"))
        elif st == "scheduled":
            _upd_slice(conn, algo["algo_id"], s["slice_idx"], status=mark)


def _cancel_algo(conn, r, p: dict) -> dict:
    algo_id = p.get("algo_id")
    if not algo_id:
        return _algo_out(False, "rejected", error="algo_id required")
    algo_id = str(algo_id)
    algo = _get_algo(conn, algo_id)
    if algo is None:
        return _algo_out(False, "not_found", algo_id=algo_id, error="algo not found")
    st = algo.get("status")
    if st == "cancelled":
        return _algo_out(True, "cancelled", algo_id=algo_id, algo=algo)
    if st in ALGO_TERMINAL:
        return _algo_out(False, st, algo_id=algo_id, algo=algo, error=f"algo already {st}")
    try:
        refresh_from_oms(conn, algo_id)
    except Exception as e:
        log.warning("refresh before cancel failed for %s: %s", algo_id, e)
        try:
            conn.rollback()
        except Exception:
            pass
    _upd_algo(conn, algo_id, status="cancelled")
    conn.commit()
    _cancel_open_slices(conn, r, algo, _get_slices(conn, algo_id), mark="cancelled")
    conn.commit()
    algo = _get_algo(conn, algo_id)
    log.info("algo %s cancelled", algo_id)
    return _algo_out(True, "cancelled", algo_id=algo_id, algo=algo)


def handle_request(conn, r, payload: dict) -> dict:
    payload = dict(payload or {})
    cmd_id = payload.get("cmd_id") or payload.get("req_id")
    kind = payload.get("kind")
    try:
        if kind == "submit":
            reply = execute_submit(conn, r, payload)
        elif kind == "cancel_order":
            reply = execute_cancel(conn, r, payload)
        elif kind == "create":
            reply = _create_algo(conn, r, payload)
        elif kind == "cancel":
            reply = _cancel_algo(conn, r, payload)
        else:
            reply = {"ok": False, "status": "rejected", "error": f"unknown kind {kind!r}"}
    except Exception as e:
        log.exception("request %s (%s) failed", cmd_id, kind)
        try:
            conn.rollback()
        except Exception:
            pass
        reply = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
        if kind in ("create", "cancel"):
            reply.update({"algo_id": payload.get("algo_id"), "algo": None})
        else:
            reply.update({"order_id": payload.get("order_id"),
                          "client_order_id": payload.get("client_order_id"), "order": None})
    reply = dict(reply)
    reply["cmd_id"] = cmd_id
    if r is not None and cmd_id:
        try:
            runtime.redis_call(runtime.send_reply, r, str(cmd_id), reply)
        except Exception as e:
            log.error("send_reply failed cmd=%s: %s", cmd_id, e)
    log.info("req %s kind=%s -> %s %s", str(cmd_id)[:12], kind, reply.get("status"),
             reply.get("error") or "")
    return reply


# --------------------------------------------------------------------------- algo engine
def _slice_status_from_order(cur, ost):
    if cur in ("rolled", "cancelled", "filled"):
        return cur
    if not ost:
        return cur
    if ost == "filled":
        return "filled"
    if ost in runtime.ACTIVE_STATUSES:
        return "working"
    if ost in ("rejected", "expired"):
        return "failed"
    if ost == "canceled":
        return "cancelled"
    return cur


def refresh_from_oms(conn, algo_id) -> None:
    slices = _get_slices(conn, algo_id)
    if not slices:
        return
    coids = [s["client_order_id"] for s in slices if s.get("client_order_id")]
    rows = []
    if coids:
        qs = ",".join("?" for _ in coids)
        rows = _oms_rows(conn, f"SELECT * FROM orders WHERE client_order_id IN ({qs})", coids)
    by_coid = {r_["client_order_id"]: r_ for r_ in rows if r_.get("client_order_id")}
    by_idx = {}
    if not rows:
        fb = _oms_rows(conn, "SELECT * FROM orders WHERE algo_id LIKE ?", (algo_id[:12] + "%",))
        for r_ in fb:
            if r_.get("slice_idx") is not None:
                by_idx[int(r_["slice_idx"])] = r_
    order_ids = []
    for s in slices:
        row = by_coid.get(s.get("client_order_id")) or by_idx.get(int(s["slice_idx"]))
        if row is None:
            if s.get("order_id"):
                order_ids.append(s["order_id"])
            continue
        oid = row.get("order_id") or s.get("order_id")
        if oid:
            order_ids.append(oid)
        filled = _f(row.get("filled_qty"))
        filled = filled if filled is not None else (_f(s.get("filled_qty")) or 0.0)
        filled = max(filled, _f(s.get("filled_qty")) or 0.0)
        new_st = _slice_status_from_order(s.get("status"), _s(row.get("status")))
        if (oid != s.get("order_id") or abs(filled - (_f(s.get("filled_qty")) or 0.0)) > 1e-15
                or new_st != s.get("status")):
            _upd_slice(conn, algo_id, s["slice_idx"], order_id=oid, filled_qty=filled,
                       status=new_st)
    slices = _get_slices(conn, algo_id)
    total_filled = sum(_f(s.get("filled_qty")) or 0.0 for s in slices)
    avg_px = None
    if order_ids:
        qs = ",".join("?" for _ in order_ids)
        fills = _oms_rows(conn, f"SELECT price, qty FROM fills WHERE order_id IN ({qs})",
                          order_ids)
        pq = sum((_f(f_["price"]) or 0.0) * (_f(f_["qty"]) or 0.0) for f_ in fills
                 if _f(f_["price"]) and _f(f_["qty"]))
        q = sum(_f(f_["qty"]) or 0.0 for f_ in fills if _f(f_["price"]) and _f(f_["qty"]))
        if q > 0:
            avg_px = pq / q
        else:
            ords = _oms_rows(conn, f"SELECT filled_qty, filled_avg_price FROM orders"
                                   f" WHERE order_id IN ({qs})", order_ids)
            pq = sum((_f(o["filled_qty"]) or 0.0) * (_f(o["filled_avg_price"]) or 0.0)
                     for o in ords if _f(o["filled_avg_price"]))
            q = sum(_f(o["filled_qty"]) or 0.0 for o in ords if _f(o["filled_avg_price"]))
            if q > 0:
                avg_px = pq / q
    algo = _get_algo(conn, algo_id)
    if algo is not None:
        old_f = _f(algo.get("filled_qty")) or 0.0
        old_px = _f(algo.get("avg_px"))
        if abs(old_f - total_filled) > 1e-15 or (avg_px is not None and avg_px != old_px):
            _upd_algo(conn, algo_id, filled_qty=total_filled,
                      avg_px=avg_px if avg_px is not None else old_px)
    conn.commit()


def _resolve_unknown(conn, r, algo, slices, now) -> None:
    """Slices whose OMS orders row is still missing >10s after submission: ask Alpaca."""
    live = algo.get("account") == "live"
    for s in slices:
        if s.get("status") not in SLICE_OPEN:
            continue
        coid = s.get("client_order_id")
        if not coid or _order_row(conn, "client_order_id", coid) is not None:
            continue
        if now - _ts(s.get("updated_at")) < UNKNOWN_LOOKUP_AFTER_S:
            continue
        mono = time.monotonic()
        if mono - _last_lookup.get(coid, -1e18) < UNKNOWN_LOOKUP_AFTER_S:
            continue
        _last_lookup[coid] = mono
        try:
            o = trader.get_order_by_client_id(coid, live)
        except Exception as e:
            log.warning("algo %s slice %s lookup failed: %s", algo["algo_id"], s["slice_idx"],
                        str(e)[:200])
            continue
        if o:
            oid = str(o.get("id") or "") or s.get("order_id")
            filled = max(_f(o.get("filled_qty")) or 0.0, _f(s.get("filled_qty")) or 0.0)
            st = _slice_status_from_order(s.get("status"), _s(o.get("status")))
            _upd_slice(conn, algo["algo_id"], s["slice_idx"], order_id=oid, filled_qty=filled,
                       status=st)
            publish_report(r, "ems_submitted", o, account=algo.get("account"),
                           client_order_id=coid, source="ems", algo_id=algo["algo_id"],
                           slice_idx=s["slice_idx"])
            log.info("algo %s slice %s resolved via REST: %s", algo["algo_id"], s["slice_idx"],
                     st)
        elif not s.get("order_id"):
            _upd_slice(conn, algo["algo_id"], s["slice_idx"], status="failed")
            log.warning("algo %s slice %s (coid %s) never reached the exchange; failed",
                        algo["algo_id"], s["slice_idx"], coid)
    conn.commit()


def _submit_due(conn, r, algo, slices, due, now, last_idx) -> bool:
    """Submit the latest due slice (rolling earlier unfilled qty into it).

    Returns True if the algo failed."""
    algo_id = algo["algo_id"]
    account = algo.get("account") or "paper"
    live = account == "live"
    target = due[-1]
    tidx = int(target["slice_idx"])
    tqty = _f(target.get("qty")) or 0.0
    # 1) roll forward earlier working/submitted slices
    for s in slices:
        if int(s["slice_idx"]) >= tidx or s.get("status") not in SLICE_OPEN:
            continue
        res = execute_cancel(conn, r, {"account": account, "order_id": s.get("order_id"),
                                       "client_order_id": s.get("client_order_id"),
                                       "source": "ems"})
        if res.get("status") not in ("cancel_requested", "not_found"):
            log.warning("algo %s: roll cancel of slice %s failed (%s); deferring slice %s",
                        algo_id, s["slice_idx"], res.get("error"), tidx)
            conn.commit()
            return False
        filled = _f(s.get("filled_qty")) or 0.0
        oid = res.get("order_id") or s.get("order_id")
        final_status = "rolled"
        if oid:
            try:
                o = trader.get_order(oid, live)
                if isinstance(o, dict):
                    filled = max(filled, _f(o.get("filled_qty")) or 0.0)
                    if _s(o.get("status")) == "filled":
                        final_status = "filled"
            except Exception as e:
                log.debug("get_order %s after cancel failed: %s", oid, e)
        unfilled = max(0.0, (_f(s.get("qty")) or 0.0) - filled)
        if final_status == "filled":
            unfilled = 0.0
        tqty += unfilled
        _upd_slice(conn, algo_id, s["slice_idx"], status=final_status, filled_qty=filled,
                   order_id=oid)
        _upd_slice(conn, algo_id, tidx, qty=tqty)
        conn.commit()
        log.info("algo %s: slice %s %s, rolled %.10g into slice %s", algo_id, s["slice_idx"],
                 final_status, unfilled, tidx)
    # 2) missed scheduled slices merge into the target
    for s in due[:-1]:
        tqty += _f(s.get("qty")) or 0.0
        _upd_slice(conn, algo_id, s["slice_idx"], status="rolled")
        _upd_slice(conn, algo_id, tidx, qty=tqty)
        conn.commit()
        log.info("algo %s: missed slice %s merged into slice %s", algo_id, s["slice_idx"], tidx)
    # 3) price and submit
    final = tidx == last_idx
    rules = _rules_for(algo["symbol"], live, r)
    qty = algos.round_qty(tqty, rules.min_trade_increment)
    if qty <= 0:
        _upd_slice(conn, algo_id, tidx, status="cancelled", qty=0.0)
        conn.commit()
        return False
    strategy = _strategy_for(algo)
    try:
        quote = trader.latest_quote(algo["symbol"], r=r)
        px = strategy.child_price(algo["side"], quote, _f(algo.get("limit_cap")), final, rules)
    except Exception as e:
        log.warning("algo %s: no usable quote for slice %s: %s", algo_id, tidx, str(e)[:200])
        if now > _ts(algo.get("end_ts")) + QUOTE_GIVE_UP_AFTER_END_S:
            raise RuntimeError(f"no usable quote: {e}")
        return False
    _upd_slice(conn, algo_id, tidx, qty=qty, limit_price=px)
    conn.commit()
    res = execute_submit(conn, r, {
        "client_order_id": target["client_order_id"], "account": account,
        "symbol": algo["symbol"], "side": algo["side"], "type": "limit", "qty": qty,
        "limit_price": px, "tif": "ioc" if final else "gtc", "source": "ems",
        "algo_id": algo_id, "slice_idx": tidx})
    st = res.get("status")
    if st in ("acked", "already_submitted"):
        _upd_slice(conn, algo_id, tidx, status="submitted", order_id=res.get("order_id"))
        conn.commit()
        return False
    if st == "unknown":
        _upd_slice(conn, algo_id, tidx, status="submitted")
        conn.commit()
        return False
    err = res.get("error") or st
    _upd_slice(conn, algo_id, tidx, status="failed")
    _upd_algo(conn, algo_id, status="failed", error=f"slice {tidx}: {err}"[:1000])
    conn.commit()
    log.error("algo %s failed: slice %s %s: %s", algo_id, tidx, st, err)
    try:
        _cancel_open_slices(conn, r, algo, _get_slices(conn, algo_id), mark="cancelled")
        conn.commit()
    except Exception as e:
        log.warning("algo %s: cleanup cancel failed: %s", algo_id, e)
    return True


def _finalise(conn, r, algo_id, now, last_idx) -> None:
    algo = _get_algo(conn, algo_id)
    if algo is None or algo.get("status") not in ALGO_ACTIVE:
        return
    slices = _get_slices(conn, algo_id)
    filled = _f(algo.get("filled_qty")) or 0.0
    total = _f(algo.get("total_qty")) or 0.0
    done = total > 0 and filled >= total * (1 - 1e-9)

    def verdict():
        if done:
            return "completed"
        return "partial" if filled > 0 else "failed"

    all_term = all(s.get("status") in SLICE_TERMINAL for s in slices)
    final = slices[-1] if slices else None
    status = None
    if all_term:
        status = verdict()
    elif done:
        _cancel_open_slices(conn, r, algo, slices, mark="cancelled")
        status = "completed"
    elif (final is not None and final.get("status") in SLICE_OPEN
          and now > _ts(algo.get("end_ts")) + FINAL_GRACE_S
          and all(s.get("status") in SLICE_TERMINAL for s in slices[:-1])):
        status = verdict()
    if status:
        err = None if status != "failed" else (algo.get("error") or "no fills")
        _upd_algo(conn, algo_id, status=status, error=err)
        log.info("algo %s finished: %s filled %.10g/%.10g avg_px=%s", algo_id, status, filled,
                 total, algo.get("avg_px"))


def _tick_algo(conn, r, algo_id, now, allow_submit) -> None:
    refresh_from_oms(conn, algo_id)
    algo = _get_algo(conn, algo_id)
    if algo is None or algo.get("status") not in ALGO_ACTIVE:
        return
    slices = _get_slices(conn, algo_id)
    if not slices:
        _upd_algo(conn, algo_id, status="failed", error="no slices")
        return
    last_idx = int(slices[-1]["slice_idx"])
    if algo["status"] == "pending":
        if now >= _ts(slices[0]["scheduled_ts"]):
            _upd_algo(conn, algo_id, status="running")
            algo["status"] = "running"
            log.info("algo %s running", algo_id)
        else:
            return
    _resolve_unknown(conn, r, algo, slices, now)
    slices = _get_slices(conn, algo_id)
    if allow_submit:
        due = [s for s in slices
               if s.get("status") == "scheduled" and _ts(s.get("scheduled_ts")) <= now]
        if due:
            if _submit_due(conn, r, algo, slices, due, now, last_idx):
                return
            refresh_from_oms(conn, algo_id)
    _finalise(conn, r, algo_id, now, last_idx)


def tick(conn, r, now_ts: float | None = None, allow_submit: bool = True) -> int:
    now = float(now_ts) if now_ts is not None else runtime.utc_ts()
    active = _rows(conn, "SELECT algo_id FROM algo_orders WHERE status IN ('pending','running')"
                         " ORDER BY created_at")
    touched = 0
    for a in active:
        algo_id = a["algo_id"]
        try:
            _tick_algo(conn, r, algo_id, now, allow_submit)
            conn.commit()
        except Exception as e:
            log.exception("algo %s tick failed", algo_id)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                _upd_algo(conn, algo_id, status="failed",
                          error=f"{type(e).__name__}: {e}"[:1000])
                conn.commit()
            except Exception:
                log.exception("algo %s: could not mark failed", algo_id)
                try:
                    conn.rollback()
                except Exception:
                    pass
        touched += 1
    return touched


# --------------------------------------------------------------------------- worker
def _parse_fields(fields) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        raise ValueError("entry has no 'json' field")
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("payload is not a JSON object")
    return d


def process_requests(conn, r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        cmd_id = None
        kind = None
        try:
            payload = _parse_fields(fields)
            cmd_id = payload.get("cmd_id") or payload.get("req_id")
            kind = payload.get("kind")
            handle_request(conn, r, payload)
            conn.commit()
            reader.ack(eid)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.exception("request entry %s failed; dead-lettering", eid)
            if cmd_id and r is not None:
                try:
                    reply = {"ok": False, "status": "error", "cmd_id": cmd_id,
                             "error": f"{type(e).__name__}: {e}"}
                    if kind in ("create", "cancel"):
                        reply.update({"algo_id": None, "algo": None})
                    runtime.send_reply(r, str(cmd_id), reply)
                except Exception:
                    pass
            reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
    return len(entries)


def body() -> None:
    r = runtime.get_redis()
    conn = runtime.sqlite_connect()
    try:
        setup_db(conn)
        consumer = os.environ.get("EMS_CONSUMER", "ems-1")
        reader = runtime.GroupReader(r, runtime.STREAM_EMS_REQUESTS, GROUP, consumer,
                                     block_ms=500)
        log.info("ems started consumer=%s", consumer)
        last_warn = 0.0
        next_tick = 0.0
        while True:
            process_requests(conn, r, reader)
            mono = time.monotonic()
            if mono >= next_tick:
                next_tick = mono + 1.0
                oms_up = runtime.worker_alive(r, "oms") is not None
                if not oms_up and mono - last_warn >= 30.0:
                    last_warn = mono
                    log.warning("OMS heartbeat missing; not starting new algo children")
                try:
                    tick(conn, r, allow_submit=oms_up)
                except sqlite3.Error as e:
                    log.warning("tick failed: %s", e)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    runtime.run_forever(body, NAME)


if __name__ == "__main__":
    main()
