"""Thin Flask backend for web/index.html.

Reads Redis (market data, heartbeats) and SQLite (OMS/EMS tables, read-only) and forwards
writes to the OMS / EMS workers as commands. Never writes SQLite, never calls Alpaca order
endpoints. No network / Redis / SQLite work at import time.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

import algos
import runtime
import trader

NAME = "webgui"
WEB_DIR = Path(__file__).parent / "web"
WORKERS = ("subscriber", "trade_stream", "oms", "ems", "webgui")
LAST_RECONCILE_KEY = "alpaca:oms:last_reconcile"

log = runtime.log(NAME)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/static")

_redis = None
_redis_lock = threading.Lock()
_stats_lock = threading.Lock()
_stats = {"n": 0, "errors": 0, "last": time.monotonic()}


# --------------------------------------------------------------------------- helpers
def _r():
    global _redis
    with _redis_lock:
        if _redis is None:
            _redis = runtime.get_redis()
        return _redis


def _live(arg) -> bool:
    if isinstance(arg, bool):
        return arg
    return str(arg if arg is not None else "").strip().lower() in ("1", "true", "yes")


def _account(live) -> str:
    return "live" if live else "paper"


def _rows(sql: str, params=()) -> list:
    try:
        if not Path(runtime.DB_PATH).exists():
            return []
    except Exception:
        return []
    conn = None
    try:
        conn = runtime.sqlite_connect()
        return [{k: row[k] for k in row.keys()} for row in conn.execute(sql, tuple(params)).fetchall()]
    except sqlite3.Error as e:
        log.debug("query failed: %s", e)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _json_err(msg, code: int = 400):
    return jsonify({"ok": False, "error": str(msg)}), code


def _f(x):
    if x is None or x == "" or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _int_arg(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(request.args.get(name, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def _body() -> dict:
    d = request.get_json(silent=True)
    return d if isinstance(d, dict) else {}


def _reply_code(reply: dict) -> int:
    if reply.get("ok"):
        return 200
    if reply.get("status") == "timeout":
        return 504
    return 502


def _send(stream: str, worker: str, payload: dict):
    try:
        r = _r()
        if not runtime.worker_alive(r, worker):
            return jsonify({"ok": False, "status": "unavailable",
                            "error": f"{worker.upper()} not running"}), 503
        reply = runtime.send_command(r, stream, payload, timeout=12)
    except Exception as e:
        log.warning("command to %s failed: %s", worker, e)
        return jsonify({"ok": False, "status": "error", "error": f"redis unavailable: {e}"}), 503
    if not isinstance(reply, dict):
        reply = {"ok": False, "status": "error", "error": "bad reply"}
    return jsonify(reply), _reply_code(reply)


@app.after_request
def _after(resp):
    with _stats_lock:
        _stats["n"] += 1
        if resp.status_code >= 500:
            _stats["errors"] += 1
        now = time.monotonic()
        if now - _stats["last"] >= 60:
            log.info("http summary: %d requests, %d 5xx in last %.0fs", _stats["n"],
                     _stats["errors"], now - _stats["last"])
            _stats.update(n=0, errors=0, last=now)
    return resp


# --------------------------------------------------------------------------- pages / health
@app.route("/")
def index():
    if (WEB_DIR / "index.html").exists():
        return send_from_directory(WEB_DIR, "index.html")
    return "web/index.html missing; API available under /api/", 200, {"Content-Type": "text/plain"}


def _age(ts):
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - d).total_seconds(), 2)
    except (TypeError, ValueError):
        return None


@app.route("/api/health")
def health():
    workers = {w: {"alive": False, "age_s": None} for w in WORKERS}
    redis_ok = False
    last_rec = None
    try:
        r = _r()
        redis_ok = bool(r.ping())
        for w in WORKERS:
            hb = runtime.worker_alive(r, w)
            workers[w] = {"alive": hb is not None, "age_s": _age(hb.get("ts")) if hb else None}
        last_rec = r.get(LAST_RECONCILE_KEY)
    except Exception as e:
        log.debug("health redis: %s", e)
    db_ok = False
    try:
        if Path(runtime.DB_PATH).exists():
            conn = runtime.sqlite_connect()
            try:
                conn.execute("SELECT 1 FROM orders LIMIT 1").fetchall()
                db_ok = True
            finally:
                conn.close()
    except Exception:
        db_ok = False
    return jsonify({"ok": bool(redis_ok and workers["oms"]["alive"]), "redis": redis_ok, "db": db_ok,
                    "workers": workers, "last_reconcile": last_rec, "ts": runtime.now_iso()})


# --------------------------------------------------------------------------- market data
@app.route("/api/symbols")
def symbols():
    try:
        return jsonify(sorted(s for s in (_r().smembers("alpaca:symbols") or []) if s))
    except Exception:
        return jsonify([])


def _levels(h: dict, reverse: bool) -> list:
    out = []
    for p, s in (h or {}).items():
        pf, sf = _f(p), _f(s)
        if pf is None or sf is None or pf <= 0 or sf <= 0:
            continue
        out.append({"price": pf, "size": sf})
    out.sort(key=lambda x: x["price"], reverse=reverse)
    return out[:10]


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
        if (bp and bp > 0) or (ap and ap > 0):
            return jsonify({
                "symbol": sym, "fallback": "quote_only", "timestamp": q.get("timestamp") or q.get("ts"),
                "bids": [{"price": bp, "size": _f(q.get("bid_size"))}] if bp and bp > 0 else [],
                "asks": [{"price": ap, "size": _f(q.get("ask_size"))}] if ap and ap > 0 else []})
    except Exception as e:
        log.debug("orderbook %s: %s", sym, e)
    return jsonify({"symbol": sym, "bids": [], "asks": [], "timestamp": None, "fallback": "no_data"})


@app.route("/api/latest/<path:symbol>")
def latest(symbol):
    try:
        return jsonify(_r().hgetall(f"alpaca:latest:{symbol.upper()}") or {})
    except Exception:
        return jsonify({})


# --------------------------------------------------------------------------- orders / fills
@app.route("/api/orders")
def orders():
    limit = _int_arg("limit", 50, 1, 500)
    status = (request.args.get("status") or "all").lower()
    acct = (request.args.get("account") or "all").lower()
    where, params = [], []
    active = sorted(runtime.ACTIVE_STATUSES)
    ph = ",".join("?" * len(active))
    if status == "open":
        where.append(f"status IN ({ph})")
        params += active
    elif status == "closed":
        where.append(f"(status IS NULL OR status NOT IN ({ph}))")
        params += active
    if acct in ("paper", "live"):
        where.append("account=?")
        params.append(acct)
    sql = ("SELECT order_id, client_order_id, account, symbol, side, type, time_in_force, qty, "
           "limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx, "
           "submitted_at, created_at, updated_at FROM orders")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(submitted_at, created_at, updated_at) DESC LIMIT ?"
    return jsonify(_rows(sql, params + [limit]))


@app.route("/api/fills")
def fills():
    limit = _int_arg("limit", 50, 1, 500)
    return jsonify(_rows("SELECT id, execution_id, order_id, account, symbol, side, price, qty, event, "
                         "ts, algo_id FROM fills ORDER BY ts DESC, id DESC LIMIT ?", [limit]))


def _realized_pnl(account: str) -> dict:
    rows = _rows("SELECT f.symbol AS symbol, f.side AS side, f.price AS price, f.qty AS qty "
                 "FROM fills f JOIN orders o USING(order_id) WHERE o.account=? "
                 "ORDER BY f.ts ASC, f.id ASC", [account])
    lots = defaultdict(deque)
    pnl: dict = {}
    for row in rows:
        sym, side = row.get("symbol"), (row.get("side") or "").lower()
        px, qty = _f(row.get("price")), _f(row.get("qty"))
        if not sym or px is None or not qty or qty <= 0:
            continue
        pnl.setdefault(sym, 0.0)
        if side == "buy":
            lots[sym].append([px, qty])
        elif side == "sell":
            remaining = qty
            q = lots[sym]
            while remaining > 1e-12 and q:
                lot = q[0]
                take = min(lot[1], remaining)
                pnl[sym] += (px - lot[0]) * take
                lot[1] -= take
                remaining -= take
                if lot[1] <= 1e-12:
                    q.popleft()
    return {k: round(v, 8) for k, v in pnl.items()}


@app.route("/api/summary")
def summary():
    live = _live(request.args.get("live"))
    out = {"account_type": _account(live), "account": None, "positions": None, "degraded": False}
    try:
        out["account"] = trader.account(live)
    except Exception as e:
        log.debug("summary account: %s", e)
        out["degraded"] = True
        out["error"] = str(e)[:300]
    try:
        out["positions"] = trader.positions(live)
    except Exception as e:
        log.debug("summary positions: %s", e)
        out["degraded"] = True
        out.setdefault("error", str(e)[:300])
    try:
        out["realized_pnl_by_symbol"] = _realized_pnl(_account(live))
    except Exception:
        out["realized_pnl_by_symbol"] = {}
    return jsonify(out)


@app.route("/api/order", methods=["POST"])
def post_order():
    b = _body()
    symbol = str(b.get("symbol") or "").strip().upper()
    if not symbol:
        return _json_err("symbol required")
    qty = _f(b.get("qty"))
    if qty is None or qty <= 0:
        return _json_err("qty must be > 0")
    side = str(b.get("side") or "").strip().lower()
    if side not in ("buy", "sell"):
        return _json_err("side must be buy or sell")
    otype = str(b.get("type") or "market").strip().lower()
    if otype not in ("market", "limit"):
        return _json_err("type must be market or limit")
    limit_price = _f(b.get("limit_price"))
    if otype == "limit":
        if limit_price is None or limit_price <= 0:
            return _json_err("limit_price must be > 0 for limit orders")
    else:
        limit_price = None
    tif = str(b.get("tif") or "gtc").strip().lower()
    if tif not in ("gtc", "ioc"):
        return _json_err("tif must be gtc or ioc")
    live = _live(b.get("live"))
    account = _account(live)
    if live:
        log.warning("LIVE order requested via webgui: %s %s %s", side, qty, symbol)
    payload = {"kind": "submit", "account": account,
               "client_order_id": runtime.new_client_order_id("tc"), "symbol": symbol, "side": side,
               "type": otype, "qty": qty, "limit_price": limit_price, "tif": tif, "source": "webgui"}
    return _send(runtime.STREAM_OMS_COMMANDS, "oms", payload)


@app.route("/api/order/<order_id>", methods=["DELETE"])
def delete_order(order_id):
    live = _live(request.args.get("live"))
    payload = {"kind": "cancel", "account": _account(live), "order_id": order_id, "source": "webgui"}
    return _send(runtime.STREAM_OMS_COMMANDS, "oms", payload)


# --------------------------------------------------------------------------- algos
@app.route("/api/algos/strategies")
def strategies():
    try:
        return jsonify(algos.available_strategies())
    except Exception as e:
        log.warning("strategies: %s", e)
        return jsonify([])


@app.route("/api/algo", methods=["POST"])
def post_algo():
    b = _body()
    strategy = str(b.get("strategy") or "twap").strip().lower()
    cls = algos.REGISTRY.get(strategy)
    if cls is None:
        return _json_err(f"unknown strategy {strategy!r}")
    if not getattr(cls, "available", False):
        return _json_err(f"strategy {strategy!r} not available")
    symbol = str(b.get("symbol") or "").strip().upper()
    if not symbol:
        return _json_err("symbol required")
    side = str(b.get("side") or "").strip().lower()
    if side not in ("buy", "sell"):
        return _json_err("side must be buy or sell")
    total_qty = _f(b.get("total_qty"))
    if total_qty is None or total_qty <= 0:
        return _json_err("total_qty must be > 0")
    duration_s = _f(b.get("duration_s"))
    if duration_s is None or duration_s < 10:
        return _json_err("duration_s must be >= 10")
    n = _f(b.get("n_slices"))
    if n is None or n < 1 or int(n) != n:
        return _json_err("n_slices must be an integer >= 1")
    limit_cap = None
    if b.get("limit_cap") not in (None, ""):
        limit_cap = _f(b.get("limit_cap"))
        if limit_cap is None or limit_cap <= 0:
            return _json_err("limit_cap must be > 0")
    params = b.get("params") if b.get("params") is not None else {}
    if not isinstance(params, dict):
        return _json_err("params must be an object")
    live = _live(b.get("live"))
    if live:
        log.warning("LIVE algo requested via webgui: %s %s %s %s", strategy, side, total_qty, symbol)
    payload = {"req_id": runtime.new_id(), "kind": "create", "strategy": strategy,
               "account": _account(live), "symbol": symbol, "side": side, "total_qty": total_qty,
               "duration_s": duration_s, "n_slices": int(n), "limit_cap": limit_cap,
               "params": params, "source": "webgui"}
    return _send(runtime.STREAM_EMS_REQUESTS, "ems", payload)


@app.route("/api/algos")
def list_algos():
    status = (request.args.get("status") or "all").lower()
    limit = _int_arg("limit", 100, 1, 500)
    sql = ("SELECT a.*, "
           "(SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id=a.algo_id AND s.status IN "
           "('filled','rolled','cancelled','failed')) AS slices_done, "
           "(SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id=a.algo_id) AS slices_total "
           "FROM algo_orders a")
    if status == "running":
        sql += " WHERE a.status IN ('pending','running')"
    sql += " ORDER BY a.created_at DESC LIMIT ?"
    return jsonify(_rows(sql, [limit]))


@app.route("/api/algo/<algo_id>")
def get_algo(algo_id):
    rows = _rows("SELECT * FROM algo_orders WHERE algo_id=?", [algo_id])
    if not rows:
        return _json_err("algo not found", 404)
    slices = _rows("SELECT * FROM algo_slices WHERE algo_id=? ORDER BY slice_idx", [algo_id])
    ords = _rows("SELECT order_id, client_order_id, account, symbol, side, type, time_in_force, qty, "
                 "limit_price, filled_qty, filled_avg_price, status, slice_idx, submitted_at, "
                 "updated_at FROM orders WHERE algo_id=? OR algo_id=? OR client_order_id LIKE ? "
                 "ORDER BY slice_idx", [algo_id, algo_id[:12], f"algo-{algo_id[:12]}-%"])
    return jsonify({"algo": rows[0], "slices": slices, "orders": ords})


@app.route("/api/algo/<algo_id>", methods=["DELETE"])
def delete_algo(algo_id):
    payload = {"req_id": runtime.new_id(), "kind": "cancel", "algo_id": algo_id, "source": "webgui"}
    return _send(runtime.STREAM_EMS_REQUESTS, "ems", payload)


# --------------------------------------------------------------------------- main
def body() -> None:
    host = os.environ.get("WEBGUI_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("WEBGUI_PORT", "5000"))
    except ValueError:
        port = 5000
    log.info("webgui listening on http://%s:%d (web dir %s)", host, port, WEB_DIR)
    app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)


def main() -> None:
    runtime.run_forever(body, NAME)


if __name__ == "__main__":
    main()
