"""Thin Flask backend for web/index.html: Redis (live) + SQLite (history, read-only).
Every write intent (orders, cancels, algos) is forwarded to OMS via runtime.send_command.
No network / Redis / SQLite work at import time."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

import algos
import runtime
import trader

log = runtime.log("webgui")
logging.getLogger("werkzeug").setLevel(logging.WARNING)

WEB_DIR = Path(__file__).parent / "web"
WORKERS = ("subscriber", "trade_stream", "oms", "ems", "algo_engine", "persister", "webgui")
SLICE_DONE = ("filled", "rolled", "cancelled", "failed")
ORDER_COLS = ("order_id, client_order_id, account, symbol, side, type, time_in_force, qty, limit_price, "
              "filled_qty, filled_avg_price, status, source, algo_id, slice_idx, submitted_at, created_at, "
              "updated_at, last_event_ts")

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/static")

_redis = None
_redis_lock = threading.Lock()
_stats = {"n": 0, "errors": 0, "last": time.monotonic()}
_stats_lock = threading.Lock()

def _r():
    global _redis
    if _redis is None:
        with _redis_lock:
            if _redis is None:
                _redis = runtime.get_redis()
    return _redis

def _live(arg) -> bool:
    return arg if isinstance(arg, bool) else str(arg or "").strip().lower() in ("1", "true", "yes")

def _account(live) -> str: return "live" if live else "paper"

def _json_err(msg, code=400): return jsonify({"ok": False, "error": str(msg)}), code

def _db_exists() -> bool: return runtime.DB_PATH == ":memory:" or os.path.exists(runtime.DB_PATH)

def _rows(sql, params=(), strict=False) -> list:
    """Read-only query; missing DB/table -> [] (or raise when strict, for the health probe)."""
    if not _db_exists():
        if strict:
            raise sqlite3.OperationalError("db file missing")
        return []
    try:
        with closing(runtime.sqlite_connect()) as conn:
            return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]
    except sqlite3.Error as e:
        if strict:
            raise
        log.debug("sqlite query failed: %s", e)
        return []

def _limit(default=50, cap=500) -> int:
    return max(1, min(int(_f(request.args.get("limit")) or default), cap))

def _f(v):
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None

def _age_s(ts):
    try:
        s = str(ts)
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - dt).total_seconds(), 3)
    except Exception:
        return None

def _relay(payload: dict):
    """Forward an intent to OMS and map the reply to an HTTP status."""
    try:
        r = _r()
        if not runtime.worker_alive(r, "oms"):
            return jsonify({"ok": False, "status": "error", "error": "OMS not running"}), 503
        reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=12)
    except Exception as e:
        log.warning("send_command failed: %s", e)
        return jsonify({"ok": False, "status": "error", "error": f"redis unavailable: {e}"}), 503
    if not isinstance(reply, dict):
        reply = {"ok": False, "status": "error", "error": "bad reply"}
    code = 200 if reply.get("ok") else (504 if reply.get("status") == "timeout" else 502)
    log.info("command %s %s -> %s %s", payload.get("kind"), payload.get("cmd_id"), reply.get("status"), reply.get("error") or "")
    return jsonify(reply), code

@app.after_request
def _after(resp):
    with _stats_lock:
        _stats["n"] += 1
        if resp.status_code >= 500:
            _stats["errors"] += 1
        now = time.monotonic()
        if now - _stats["last"] >= 60:
            log.info("http summary last %.0fs: requests=%d 5xx=%d", now - _stats["last"],
                     _stats["n"], _stats["errors"])
            _stats.update(n=0, errors=0, last=now)
    return resp

@app.route("/")
def index():
    if (WEB_DIR / "index.html").exists():
        return send_from_directory(WEB_DIR, "index.html")
    return "web/index.html missing; API is under /api/*", 200, {"Content-Type": "text/plain"}

@app.route("/api/health")
def health():
    out = {"ok": False, "redis": False, "db": False, "workers": {}, "last_reconcile": None,
           "open_orders": 0, "active_algos": 0}
    try:
        r = _r()
        out["redis"] = bool(r.ping())
    except Exception:
        r = None
    for name in WORKERS:
        info = runtime.worker_alive(r, name) if out["redis"] else None
        out["workers"][name] = {"alive": bool(info), "age_s": _age_s(info.get("ts")) if info else None}
    if out["redis"]:
        try:
            out["last_reconcile"] = r.get("alpaca:oms:last_reconcile")
        except Exception:
            pass
        for k, key in (("open_orders", runtime.SET_OPEN_ORDERS), ("active_algos", runtime.SET_ACTIVE_ALGOS)):
            try:
                out[k] = int(r.scard(key) or 0)
            except Exception:
                out[k] = 0
    try:
        out["db"] = _rows("SELECT 1 FROM orders LIMIT 1", strict=True) is not None
    except Exception:
        pass
    w = out["workers"]
    out["ok"] = bool(out["redis"] and w["oms"]["alive"] and w["ems"]["alive"] and w["persister"]["alive"])
    return jsonify(out)

@app.route("/api/symbols")
def symbols():
    try:
        return jsonify(sorted(_r().smembers("alpaca:symbols") or []))
    except Exception:
        return jsonify([])

def _levels(h, reverse):
    out = [{"price": _f(p), "size": _f(s)} for p, s in (h or {}).items()]
    out = [x for x in out if x["price"] and x["size"] and x["price"] > 0 and x["size"] > 0]
    return sorted(out, key=lambda x: x["price"], reverse=reverse)[:10]

@app.route("/api/orderbook/<path:symbol>")
def orderbook(symbol):
    sym = symbol.upper()
    try:
        r = _r()
        bids = _levels(r.hgetall(f"alpaca:ob:bids:{sym}"), True)
        asks = _levels(r.hgetall(f"alpaca:ob:asks:{sym}"), False)
        if bids or asks:
            meta = r.hgetall(f"alpaca:orderbook:{sym}") or {}
            return jsonify({"symbol": sym, "bids": bids, "asks": asks,
                            "timestamp": meta.get("timestamp") or meta.get("ts")})
        q = r.hgetall(f"alpaca:latest:{sym}") or {}
        bp, ap = _f(q.get("bid_price")), _f(q.get("ask_price"))
        bids = [{"price": bp, "size": _f(q.get("bid_size"))}] if bp and bp > 0 else []
        asks = [{"price": ap, "size": _f(q.get("ask_size"))}] if ap and ap > 0 else []
        if bids or asks:
            return jsonify({"symbol": sym, "fallback": "quote_only", "bids": bids, "asks": asks,
                            "timestamp": q.get("timestamp") or q.get("ts")})
    except Exception as e:
        log.debug("orderbook %s failed: %s", sym, e)
    return jsonify({"symbol": sym, "bids": [], "asks": [], "timestamp": None, "fallback": "no_data"})

@app.route("/api/latest/<path:symbol>")
def latest(symbol):
    try:
        return jsonify(_r().hgetall(f"alpaca:latest:{symbol.upper()}") or {})
    except Exception:
        return jsonify({})

@app.route("/api/orders")
def orders():
    status = str(request.args.get("status", "all")).lower()
    acct = str(request.args.get("account", "all")).lower()
    where, params = [], []
    active = sorted(runtime.ACTIVE_STATUSES)
    ph = ",".join("?" for _ in active)
    if status == "open":
        where.append(f"status IN ({ph})")
        params += active
    elif status == "closed":
        where.append(f"(status IS NULL OR status NOT IN ({ph}))")
        params += active
    if acct in ("paper", "live"):
        where.append("account = ?")
        params.append(acct)
    sql = f"SELECT {ORDER_COLS} FROM orders" + (" WHERE " + " AND ".join(where) if where else "")
    params.append(_limit(50, 500))
    return jsonify(_rows(sql + " ORDER BY submitted_at DESC LIMIT ?", params))

@app.route("/api/fills")
def fills():
    return jsonify(_rows("SELECT execution_id, order_id, account, symbol, side, price, qty, event, ts, "
                         "algo_id FROM fills ORDER BY ts DESC LIMIT ?", (_limit(50, 500),)))

def _realized_pnl(account: str) -> dict:
    """FIFO: buys open lots, sells close the oldest lots; unmatched (short) sells are ignored."""
    rows = _rows("SELECT f.symbol AS symbol, f.side AS side, f.price AS price, f.qty AS qty "
                 "FROM fills f JOIN orders o USING(order_id) WHERE o.account = ? "
                 "ORDER BY f.ts ASC, f.id ASC", (account,))
    lots: dict = {}
    pnl: dict = {}
    for row in rows:
        sym, side = row.get("symbol"), str(row.get("side") or "").lower()
        px, qty = _f(row.get("price")), _f(row.get("qty"))
        if not sym or px is None or not qty or qty <= 0:
            continue
        q = lots.setdefault(sym, deque())
        pnl.setdefault(sym, 0.0)
        if side == "buy":
            q.append([qty, px])
            continue
        remaining = qty if side == "sell" else 0.0
        while remaining > 1e-12 and q:
            lot = q[0]
            take = min(lot[0], remaining)
            pnl[sym] += (px - lot[1]) * take
            lot[0] -= take
            remaining -= take
            if lot[0] <= 1e-12:
                q.popleft()
    return {k: round(v, 8) for k, v in pnl.items()}

@app.route("/api/summary")
def summary():
    live = _live(request.args.get("live"))
    out = {"account_type": _account(live), "account": None, "positions": None, "degraded": False,
           "realized_pnl_by_symbol": {}}
    for key, fn in (("account", trader.account), ("positions", trader.positions)):
        try:
            out[key] = fn(live)
        except Exception as e:
            out["degraded"] = True
            out.setdefault("error", str(e)[:300])
    try:
        out["realized_pnl_by_symbol"] = _realized_pnl(_account(live))
    except Exception:
        pass
    return jsonify(out)

def _body() -> dict:
    d = request.get_json(silent=True)
    return d if isinstance(d, dict) else {}

def _common(b: dict, qty_key: str):
    symbol = str(b.get("symbol") or "").strip().upper()
    side = str(b.get("side") or "").lower()
    qty = _f(b.get(qty_key))
    if not symbol:
        return None, "symbol required"
    if side not in ("buy", "sell"):
        return None, "side must be buy or sell"
    if qty is None or qty <= 0:
        return None, f"{qty_key} must be > 0"
    return (symbol, side, qty), None

@app.route("/api/order", methods=["POST"])
def post_order():
    b = _body()
    vals, err = _common(b, "qty")
    if err:
        return _json_err(err)
    symbol, side, qty = vals
    otype = str(b.get("type") or "market").lower()
    tif = str(b.get("tif") or "gtc").lower()
    limit_price = _f(b.get("limit_price")) if otype == "limit" else None
    if otype not in ("market", "limit"):
        return _json_err("type must be market or limit")
    if otype == "limit" and (limit_price is None or limit_price <= 0):
        return _json_err("limit_price must be > 0 for limit orders")
    if tif not in ("gtc", "ioc"):
        return _json_err("tif must be gtc or ioc")
    return _relay({"kind": "submit", "account": _account(_live(b.get("live"))),
                   "client_order_id": runtime.new_client_order_id("tc"), "symbol": symbol, "side": side,
                   "type": otype, "qty": qty, "limit_price": limit_price, "tif": tif, "source": "webgui"})

@app.route("/api/order/<order_id>", methods=["DELETE"])
def delete_order(order_id):
    live = _live(request.args.get("live"))
    return _relay({"kind": "cancel", "account": _account(live), "order_id": order_id, "source": "webgui"})

@app.route("/api/algos/strategies")
def strategies():
    return jsonify(algos.available_strategies())

@app.route("/api/algo", methods=["POST"])
def post_algo():
    b = _body()
    strategy = str(b.get("strategy") or "twap").strip().lower()
    cls = algos.REGISTRY.get(strategy)
    if cls is None:
        return _json_err(f"unknown strategy {strategy!r}")
    if not getattr(cls, "available", False):
        return _json_err(f"strategy {strategy!r} is not available: {getattr(cls, 'note', '')}")
    vals, err = _common(b, "total_qty")
    if err:
        return _json_err(err)
    symbol, side, total_qty = vals
    duration = _f(b.get("duration_s"))
    try:
        n_slices = int(b.get("n_slices"))
    except (TypeError, ValueError):
        n_slices = 0
    has_cap = b.get("limit_cap") not in (None, "")
    limit_cap = _f(b.get("limit_cap")) if has_cap else None
    params = b.get("params") if b.get("params") is not None else {}
    if duration is None or duration < 10:
        return _json_err("duration_s must be >= 10")
    if n_slices < 1:
        return _json_err("n_slices must be an integer >= 1")
    if has_cap and (limit_cap is None or limit_cap <= 0):
        return _json_err("limit_cap must be > 0")
    if not isinstance(params, dict):
        return _json_err("params must be an object")
    return _relay({"kind": "algo_create", "strategy": strategy, "account": _account(_live(b.get("live"))),
                   "symbol": symbol, "side": side, "total_qty": total_qty, "duration_s": duration,
                   "n_slices": n_slices, "limit_cap": limit_cap, "params": params, "source": "webgui"})

@app.route("/api/algos")
def list_algos():
    done_ph = ",".join("?" for _ in SLICE_DONE)
    sql = ("SELECT a.*, "
           f"(SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id = a.algo_id AND s.status IN ({done_ph})) "
           "AS slices_done, (SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id = a.algo_id) AS slices_total "
           "FROM algo_orders a")
    if str(request.args.get("status", "all")).lower() == "running":
        sql += " WHERE a.status IN ('pending', 'running')"
    return jsonify(_rows(sql + " ORDER BY a.created_at DESC LIMIT ?", list(SLICE_DONE) + [_limit(100, 500)]))

def _json_obj(s):
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else None
    except (TypeError, ValueError):
        return None

@app.route("/api/algo/<algo_id>")
def get_algo(algo_id):
    algo, slices = None, []
    try:
        r = _r()
        h = r.hgetall(runtime.algo_key(algo_id)) or {}
        if h:
            algo = dict(h)
            raw = (r.hgetall(runtime.algo_slices_key(algo_id)) or {}).values()
            slices = sorted((d for d in map(_json_obj, raw) if d), key=lambda s: _f(s.get("slice_idx")) or 0)
    except Exception as e:
        log.debug("redis algo read failed: %s", e)
    if algo is None:
        rows = _rows("SELECT * FROM algo_orders WHERE algo_id = ?", (algo_id,))
        if rows:
            algo = rows[0]
            slices = _rows("SELECT * FROM algo_slices WHERE algo_id = ? ORDER BY slice_idx", (algo_id,))
    if algo is None:
        return _json_err("algo not found", 404)
    child = _rows(f"SELECT {ORDER_COLS} FROM orders WHERE algo_id = ? ORDER BY slice_idx", (algo_id,))
    return jsonify({"algo": algo, "slices": slices, "orders": child})

@app.route("/api/algo/<algo_id>", methods=["DELETE"])
def delete_algo(algo_id):
    return _relay({"kind": "algo_cancel", "algo_id": algo_id, "source": "webgui"})

def body():
    host = os.environ.get("WEBGUI_HOST", "127.0.0.1")
    port = int(os.environ.get("WEBGUI_PORT", "5000"))
    log.info("webgui listening on http://%s:%d", host, port)
    app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)

def main():
    runtime.run_forever(body, "webgui")

if __name__ == "__main__":
    main()
