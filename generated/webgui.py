"""webgui -- thin Flask backend for web/index.html.

Reads Redis + SQLite; forwards EVERY order/algo intent to OMS over alpaca:oms:commands.
Never writes SQLite, never talks to EMS or the exchange's order endpoints.
No network / Redis / SQLite work at import time.
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
SLICE_DONE = ("filled", "rolled", "cancelled", "failed")

log = runtime.log(NAME)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/static")

_redis = None
_redis_lock = threading.Lock()
_stats = {"n": 0, "err": 0, "last": time.monotonic()}
_stats_lock = threading.Lock()


# --------------------------------------------------------------------------- helpers
def _r():
    global _redis
    if _redis is None:
        with _redis_lock:
            if _redis is None:
                _redis = runtime.get_redis()
    return _redis


def _live(arg) -> bool:
    if isinstance(arg, bool):
        return arg
    return str(arg or "").strip().lower() in ("1", "true", "yes")


def _account(live) -> str:
    return "live" if live else "paper"


def _json_err(msg, code=400):
    return jsonify({"ok": False, "error": str(msg)}), code


def _f(x):
    if x is None or x == "" or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def _int_arg(name, default, lo=1, hi=500):
    try:
        v = int(request.args.get(name, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def _rows(sql, params=()) -> list:
    if runtime.DB_PATH != ":memory:" and not os.path.exists(runtime.DB_PATH):
        return []
    conn = None
    try:
        conn = runtime.sqlite_connect()
        cur = conn.execute(sql, tuple(params))
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(row) if isinstance(row, sqlite3.Row) else dict(zip(cols, row))
                for row in cur.fetchall()]
    except sqlite3.Error as e:
        log.debug("sqlite read failed: %s", e)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _age_s(ts):
    if not ts:
        return None
    try:
        s = str(ts)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - dt).total_seconds(), 3)
    except Exception:
        return None


def _oms_alive():
    try:
        return runtime.worker_alive(_r(), "oms") is not None
    except Exception:
        return False


def _send_oms(payload: dict):
    if not _oms_alive():
        return jsonify({"ok": False, "status": "error", "error": "OMS not running"}), 503
    try:
        reply = runtime.send_command(_r(), runtime.STREAM_OMS_COMMANDS, payload, timeout=12)
    except Exception as e:
        log.warning("send_command failed: %s", e)
        return jsonify({"ok": False, "status": "error", "error": f"redis error: {e}"}), 503
    if not isinstance(reply, dict):
        reply = {"ok": False, "status": "error", "error": "bad reply"}
    if reply.get("ok"):
        code = 200
    elif reply.get("status") == "timeout":
        code = 504
    else:
        code = 502
    log.info("%s -> %s %s", payload.get("kind"), reply.get("status"), reply.get("error") or "")
    return jsonify(reply), code


@app.after_request
def _after(resp):
    try:
        with _stats_lock:
            _stats["n"] += 1
            if resp.status_code >= 500:
                _stats["err"] += 1
            now = time.monotonic()
            if now - _stats["last"] >= 60:
                log.info("served %d requests (%d 5xx) in last %.0fs", _stats["n"], _stats["err"],
                         now - _stats["last"])
                _stats.update(n=0, err=0, last=now)
    except Exception:
        pass
    return resp


# --------------------------------------------------------------------------- read routes
@app.route("/")
def index():
    if (WEB_DIR / "index.html").exists():
        return send_from_directory(str(WEB_DIR), "index.html")
    return "web/index.html missing; API available under /api/*", 200, {"Content-Type": "text/plain"}


@app.route("/api/health")
def health():
    out = {"workers": {w: {"alive": False, "age_s": None} for w in WORKERS}, "redis": False,
           "db": False, "last_reconcile": None, "ok": False}
    try:
        r = _r()
        out["redis"] = bool(r.ping())
        for w in WORKERS:
            hb = runtime.worker_alive(r, w)
            out["workers"][w] = {"alive": hb is not None,
                                 "age_s": _age_s(hb.get("ts")) if hb else None}
        out["last_reconcile"] = r.get(LAST_RECONCILE_KEY)
    except Exception as e:
        log.debug("health redis failed: %s", e)
    try:
        if os.path.exists(runtime.DB_PATH):
            conn = runtime.sqlite_connect()
            try:
                conn.execute("SELECT 1 FROM orders LIMIT 1").fetchall()
                out["db"] = True
            finally:
                conn.close()
    except Exception:
        out["db"] = False
    out["ok"] = bool(out["redis"] and out["workers"]["oms"]["alive"]
                     and out["workers"]["ems"]["alive"])
    return jsonify(out)


@app.route("/api/symbols")
def symbols():
    try:
        return jsonify(sorted(s for s in (_r().smembers("alpaca:symbols") or []) if s))
    except Exception:
        return jsonify([])


def _levels(h, reverse):
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
    book = {"symbol": sym, "bids": [], "asks": [], "timestamp": None}
    try:
        r = _r()
        bids = _levels(r.hgetall(f"alpaca:ob:bids:{sym}"), True)
        asks = _levels(r.hgetall(f"alpaca:ob:asks:{sym}"), False)
        if bids or asks:
            meta = r.hgetall(f"alpaca:orderbook:{sym}") or {}
            book.update(bids=bids, asks=asks, timestamp=meta.get("timestamp") or meta.get("ts"))
            return jsonify(book)
        q = r.hgetall(f"alpaca:latest:{sym}") or {}
        bp, ap = _f(q.get("bid_price")), _f(q.get("ask_price"))
        if (bp and bp > 0) or (ap and ap > 0):
            if bp and bp > 0:
                book["bids"] = [{"price": bp, "size": _f(q.get("bid_size"))}]
            if ap and ap > 0:
                book["asks"] = [{"price": ap, "size": _f(q.get("ask_size"))}]
            book["timestamp"] = q.get("timestamp") or q.get("ts")
            book["fallback"] = "quote_only"
            return jsonify(book)
    except Exception as e:
        log.debug("orderbook %s failed: %s", sym, e)
    book["fallback"] = "no_data"
    return jsonify(book)


@app.route("/api/latest/<path:symbol>")
def latest(symbol):
    try:
        return jsonify(_r().hgetall(f"alpaca:latest:{symbol.upper()}") or {})
    except Exception:
        return jsonify({})


@app.route("/api/orders")
def orders():
    limit = _int_arg("limit", 50)
    status = str(request.args.get("status", "all")).lower()
    account = str(request.args.get("account", "all")).lower()
    where, params = [], []
    act = sorted(runtime.ACTIVE_STATUSES)
    qs = ",".join("?" for _ in act)
    if status == "open":
        where.append(f"status IN ({qs})")
        params += act
    elif status == "closed":
        where.append(f"(status IS NULL OR status NOT IN ({qs}))")
        params += act
    if account in ("paper", "live"):
        where.append("account=?")
        params.append(account)
    sql = ("SELECT order_id, client_order_id, account, symbol, side, type, time_in_force, qty,"
           " limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx,"
           " submitted_at, created_at, updated_at, last_event_ts FROM orders")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY submitted_at DESC LIMIT ?"
    return jsonify(_rows(sql, params + [limit]))


@app.route("/api/fills")
def fills():
    limit = _int_arg("limit", 50)
    return jsonify(_rows("SELECT id, execution_id, order_id, account, symbol, side, price, qty,"
                         " event, ts, algo_id FROM fills ORDER BY ts DESC LIMIT ?", (limit,)))


def realized_pnl(account: str) -> dict:
    rows = _rows("SELECT f.symbol AS symbol, f.side AS side, f.price AS price, f.qty AS qty"
                 " FROM fills f JOIN orders o USING(order_id) WHERE o.account=?"
                 " ORDER BY f.ts ASC, f.id ASC", (account,))
    lots = defaultdict(deque)
    pnl = defaultdict(float)
    for row in rows:
        sym, side = row.get("symbol"), str(row.get("side") or "").lower()
        px, qty = _f(row.get("price")), _f(row.get("qty"))
        if not sym or px is None or not qty or qty <= 0:
            continue
        if side == "buy":
            lots[sym].append([px, qty])
        elif side == "sell":
            pnl[sym] += 0.0
            remaining = qty
            q = lots[sym]
            while remaining > 1e-15 and q:
                lot = q[0]
                take = min(lot[1], remaining)
                pnl[sym] += (px - lot[0]) * take
                lot[1] -= take
                remaining -= take
                if lot[1] <= 1e-15:
                    q.popleft()
    return {k: round(v, 10) for k, v in pnl.items()}


@app.route("/api/summary")
def summary():
    live = _live(request.args.get("live"))
    out = {"account_type": _account(live), "account": None, "positions": None,
           "degraded": False, "realized_pnl_by_symbol": {}}
    try:
        out["account"] = trader.account(live)
        out["positions"] = trader.positions(live)
    except Exception as e:
        out["degraded"] = True
        out["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    try:
        out["realized_pnl_by_symbol"] = realized_pnl(_account(live))
    except Exception as e:
        log.warning("pnl failed: %s", e)
    return jsonify(out)


# --------------------------------------------------------------------------- write routes
@app.route("/api/order", methods=["POST"])
def post_order():
    b = request.get_json(silent=True)
    if not isinstance(b, dict):
        return _json_err("JSON object body required", 400)
    symbol = b.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return _json_err("symbol required", 400)
    qty = _f(b.get("qty"))
    if qty is None or qty <= 0:
        return _json_err("qty must be > 0", 400)
    side = str(b.get("side") or "").lower()
    if side not in ("buy", "sell"):
        return _json_err("side must be buy or sell", 400)
    otype = str(b.get("type") or "").lower()
    if otype not in ("market", "limit"):
        return _json_err("type must be market or limit", 400)
    lp = None
    if otype == "limit":
        lp = _f(b.get("limit_price"))
        if lp is None or lp <= 0:
            return _json_err("limit_price must be > 0 for limit orders", 400)
    tif = str(b.get("tif") or "gtc").lower()
    if tif not in ("gtc", "ioc"):
        return _json_err("tif must be gtc or ioc", 400)
    live = _live(b.get("live"))
    payload = {"kind": "submit", "account": _account(live),
               "client_order_id": runtime.new_client_order_id("tc"),
               "symbol": symbol.strip().upper(), "side": side, "type": otype, "qty": qty,
               "limit_price": lp, "tif": tif, "source": "webgui"}
    return _send_oms(payload)


@app.route("/api/order/<order_id>", methods=["DELETE"])
def delete_order(order_id):
    live = _live(request.args.get("live"))
    return _send_oms({"kind": "cancel", "account": _account(live), "order_id": order_id,
                      "source": "webgui"})


@app.route("/api/algos/strategies")
def strategies():
    try:
        return jsonify(algos.available_strategies())
    except Exception:
        return jsonify([])


@app.route("/api/algo", methods=["POST"])
def post_algo():
    b = request.get_json(silent=True)
    if not isinstance(b, dict):
        return _json_err("JSON object body required", 400)
    strategy = str(b.get("strategy") or "twap").strip().lower()
    cls = algos.REGISTRY.get(strategy)
    if cls is None:
        return _json_err(f"unknown strategy {strategy!r}", 400)
    if not getattr(cls, "available", False):
        return _json_err(f"strategy {strategy!r} not available: {getattr(cls, 'note', '')}", 400)
    symbol = b.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return _json_err("symbol required", 400)
    side = str(b.get("side") or "").lower()
    if side not in ("buy", "sell"):
        return _json_err("side must be buy or sell", 400)
    total_qty = _f(b.get("total_qty"))
    if total_qty is None or total_qty <= 0:
        return _json_err("total_qty must be > 0", 400)
    duration_s = _f(b.get("duration_s"))
    if duration_s is None or duration_s < 10:
        return _json_err("duration_s must be >= 10", 400)
    n_slices = _f(b.get("n_slices"))
    if n_slices is None or n_slices < 1 or int(n_slices) != n_slices:
        return _json_err("n_slices must be an integer >= 1", 400)
    limit_cap = None
    if b.get("limit_cap") not in (None, ""):
        limit_cap = _f(b.get("limit_cap"))
        if limit_cap is None or limit_cap <= 0:
            return _json_err("limit_cap must be > 0", 400)
    params = b.get("params") or {}
    if not isinstance(params, dict):
        return _json_err("params must be an object", 400)
    live = _live(b.get("live"))
    payload = {"kind": "algo_create", "strategy": strategy, "account": _account(live),
               "symbol": symbol.strip().upper(), "side": side, "total_qty": total_qty,
               "duration_s": duration_s, "n_slices": int(n_slices), "limit_cap": limit_cap,
               "params": params, "source": "webgui"}
    return _send_oms(payload)


@app.route("/api/algos")
def list_algos():
    status = str(request.args.get("status", "all")).lower()
    limit = _int_arg("limit", 100)
    done = ",".join(f"'{s}'" for s in SLICE_DONE)
    sql = ("SELECT a.*, (SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id=a.algo_id"
           f" AND s.status IN ({done})) AS slices_done,"
           " (SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id=a.algo_id) AS slices_total"
           " FROM algo_orders a")
    if status == "running":
        sql += " WHERE a.status IN ('pending','running')"
    sql += " ORDER BY a.created_at DESC LIMIT ?"
    return jsonify(_rows(sql, (limit,)))


@app.route("/api/algo/<algo_id>")
def get_algo(algo_id):
    rows = _rows("SELECT * FROM algo_orders WHERE algo_id=?", (algo_id,))
    if not rows:
        return _json_err("algo not found", 404)
    slices = _rows("SELECT * FROM algo_slices WHERE algo_id=? ORDER BY slice_idx", (algo_id,))
    ords = _rows("SELECT order_id, client_order_id, account, symbol, side, type, qty, limit_price,"
                 " filled_qty, filled_avg_price, status, slice_idx, submitted_at FROM orders"
                 " WHERE algo_id=? OR algo_id=? ORDER BY slice_idx",
                 (algo_id, algo_id[:12]))
    return jsonify({"algo": rows[0], "slices": slices, "orders": ords})


@app.route("/api/algo/<algo_id>", methods=["DELETE"])
def delete_algo(algo_id):
    return _send_oms({"kind": "algo_cancel", "algo_id": algo_id, "source": "webgui"})


# --------------------------------------------------------------------------- worker
def body() -> None:
    host = os.environ.get("WEBGUI_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("WEBGUI_PORT", "5000"))
    except ValueError:
        port = 5000
    log.info("webgui listening on http://%s:%d", host, port)
    app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)


def main() -> None:
    runtime.run_forever(body, NAME)


if __name__ == "__main__":
    main()
