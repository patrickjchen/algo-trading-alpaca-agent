"""Thin Flask backend for web/index.html: Redis (live) + SQLite (history, read-only);
every write intent goes to the OMS via runtime.send_command. No I/O at import time."""
from __future__ import annotations

import json, logging, os, sqlite3, threading, time  # noqa: E401
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

import algos
import runtime
import trader

log = runtime.log("webgui")
logging.getLogger("werkzeug").setLevel(logging.WARNING)

WEB_DIR = Path(__file__).parent / "web"
WEBGUI_HOST = os.environ.get("WEBGUI_HOST", "127.0.0.1")
WEBGUI_PORT = int(os.environ.get("WEBGUI_PORT", "5000") or 5000)
WORKERS = ("subscriber", "trade_stream", "oms", "ems", "crossing", "algo_engine", "persister", "webgui")
SLICE_DONE = ("filled", "rolled", "cancelled", "failed")
_ALGO_STR = ("algo_id", "strategy", "account", "symbol", "side", "status", "error", "source",
             "params_json", "created_at", "updated_at", "book", "start_ts", "end_ts")

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/static")
_redis = None
_redis_lock = threading.Lock()
_stats = {"n": 0, "err": 0, "last": time.monotonic()}
_stats_lock = threading.Lock()

class _Bad(ValueError): pass  # noqa: E701

def _r():
    global _redis
    if _redis is None:
        with _redis_lock:
            if _redis is None:
                _redis = runtime.get_redis()
    return _redis

def _safe(fn, default=None):
    try:
        return fn()
    except Exception as e:
        log.debug("read failed: %s", e)
        return default

def _live(arg) -> bool:
    return arg if isinstance(arg, bool) else str(arg or "").strip().lower() in ("1", "true", "yes")

def _account(live) -> str:
    return "live" if live else "paper"
def _rows(sql, params=()) -> list:
    conn = None
    try:
        if not Path(runtime.DB_PATH).exists():
            return []
        conn = runtime.sqlite_connect()
        return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]
    except sqlite3.Error as e:
        log.debug("sqlite query failed: %s", e)
        return []
    finally:
        conn is not None and conn.close()

def _json_err(msg, code=400):
    return jsonify({"ok": False, "error": str(msg)}), code
def _f(x):
    return None if x is None or x in ("", "None") else _safe(lambda: float(x))

def _floats(h: dict) -> dict:
    return {k: (_f(v) if _f(v) is not None else v) for k, v in (h or {}).items()}

def _int_arg(name, default, lo=1, hi=500) -> int:
    return max(lo, min(hi, _safe(lambda: int(request.args.get(name, default)), default)))

def _arg(name, default="all") -> str:
    return (request.args.get(name) or default).lower()
def _need(cond, msg):
    if not cond:
        raise _Bad(msg)

def _age_s(ts):
    try:
        s = str(ts)
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - dt).total_seconds(), 3)
    except Exception:
        return None

def _qs(values) -> str:
    return ",".join("?" for _ in values)
def _send_oms(payload: dict):
    try:
        r = _r()
        if not runtime.worker_alive(r, "oms"):
            return jsonify({"ok": False, "status": "error", "error": "OMS not running"}), 503
        reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=12)
    except Exception as e:
        log.warning("send to OMS failed: %s", e)
        return jsonify({"ok": False, "status": "error", "error": f"redis unavailable: {e}"}), 503
    if not isinstance(reply, dict):
        reply = {"ok": False, "status": "error", "error": "bad reply"}
    return jsonify(reply), (200 if reply.get("ok") else 504 if reply.get("status") == "timeout" else 502)

def _common(qty_field: str) -> tuple:
    b = request.get_json(silent=True)
    _need(isinstance(b, dict), "JSON body required")
    symbol, side = str(b.get("symbol") or "").strip().upper(), str(b.get("side") or "").lower()
    qty = _f(b.get(qty_field))
    _need(symbol, "symbol required")
    _need(side in ("buy", "sell"), "side must be buy or sell")
    _need(qty is not None and qty > 0, f"{qty_field} must be > 0")
    book = b.get("book") if b.get("book") not in (None, "") else runtime.DEFAULT_BOOK
    _need(isinstance(book, str), "book must be a string")
    return b, {"account": _account(_live(b.get("live"))), "book": book.strip() or runtime.DEFAULT_BOOK,
               "symbol": symbol, "side": side, qty_field: qty}

@app.errorhandler(_Bad)
def _bad(e):
    return _json_err(str(e), 400)

@app.after_request
def _after(resp):
    with _stats_lock:
        _stats["n"] += 1
        _stats["err"] += int(resp.status_code >= 500)
        if time.monotonic() - _stats["last"] >= 60:
            log.info("http last 60s: requests=%d errors_5xx=%d", _stats["n"], _stats["err"])
            _stats.update(n=0, err=0, last=time.monotonic())
    return resp

@app.route("/")
def index():
    if (WEB_DIR / "index.html").exists():
        return send_from_directory(WEB_DIR, "index.html")
    return "web/index.html not found; API is available under /api/*", 200, {"Content-Type": "text/plain"}

def _db_ok() -> bool:
    if not Path(runtime.DB_PATH).exists():
        return False
    conn = runtime.sqlite_connect()
    try:
        return conn.execute("SELECT 1 FROM orders LIMIT 1").fetchall() is not None
    finally:
        conn.close()

@app.route("/api/health")
def health():
    out = {"ok": False, "redis": False, "db": False, "workers": {}, "last_reconcile": None,
           "open_orders": 0, "active_algos": 0}
    r = _safe(_r)
    out["redis"] = bool(r is not None and _safe(r.ping, False))
    for name in WORKERS:
        info = runtime.worker_alive(r, name) if out["redis"] else None
        out["workers"][name] = {"alive": bool(info), "age_s": _age_s(info.get("ts")) if info else None}
    if out["redis"]:
        out["last_reconcile"] = _safe(lambda: r.get("alpaca:oms:last_reconcile"))
        out["open_orders"] = _safe(lambda: int(r.scard(runtime.SET_OPEN_ORDERS) or 0), 0)
        out["active_algos"] = _safe(lambda: int(r.scard(runtime.SET_ACTIVE_ALGOS) or 0), 0)
    out["db"] = bool(_safe(_db_ok, False))
    w = out["workers"]
    out["ok"] = bool(out["redis"] and all(w[n]["alive"] for n in ("oms", "ems", "crossing", "persister")))
    return jsonify(out)

@app.route("/api/symbols")
def symbols():
    return jsonify(_safe(lambda: sorted(_r().smembers("alpaca:symbols") or []), []))

def _levels(h: dict, reverse: bool) -> list:
    lv = [{"price": _f(p), "size": _f(s)} for p, s in (h or {}).items()]
    lv = [x for x in lv if x["price"] and x["size"] and x["price"] > 0 and x["size"] > 0]
    return sorted(lv, key=lambda x: x["price"], reverse=reverse)[:10]

@app.route("/api/orderbook/<path:symbol>")
def orderbook(symbol):
    sym = symbol.upper()
    out = {"symbol": sym, "bids": [], "asks": [], "timestamp": None}
    try:
        r = _r()
        out["bids"] = _levels(r.hgetall(f"alpaca:ob:bids:{sym}"), True)
        out["asks"] = _levels(r.hgetall(f"alpaca:ob:asks:{sym}"), False)
        meta = _safe(lambda: r.hgetall(f"alpaca:orderbook:{sym}"))
        out["timestamp"] = ((meta or {}).get("timestamp") or (meta or {}).get("ts")) if meta is not None \
            else _safe(lambda: r.get(f"alpaca:orderbook:{sym}"))  # WRONGTYPE: plain string
        if out["bids"] or out["asks"]:
            return jsonify(out)
        q = r.hgetall(f"alpaca:latest:{sym}") or {}
        bp, ap = _f(q.get("bid_price")), _f(q.get("ask_price"))
        if (bp and bp > 0) or (ap and ap > 0):
            out["bids"] = [{"price": bp, "size": _f(q.get("bid_size"))}] if bp and bp > 0 else []
            out["asks"] = [{"price": ap, "size": _f(q.get("ask_size"))}] if ap and ap > 0 else []
            out.update(timestamp=q.get("timestamp") or q.get("ts"), fallback="quote_only")
            return jsonify(out)
    except Exception as e:
        log.debug("orderbook read failed: %s", e)
    out["fallback"] = "no_data"
    return jsonify(out)

@app.route("/api/latest/<path:symbol>")
def latest(symbol):
    return jsonify(_safe(lambda: _r().hgetall(f"alpaca:latest:{symbol.upper()}") or {}, {}))

@app.route("/api/orders")
def orders():
    status, account, venue, book = _arg("status"), _arg("account"), _arg("venue"), request.args.get("book")
    active = sorted(runtime.ACTIVE_STATUSES)
    conds = [(status == "open", f"status IN ({_qs(active)})", active),
             (status == "closed", f"(status IS NULL OR status NOT IN ({_qs(active)}))", active),
             (account in ("paper", "live"), "account = ?", [account]),
             (venue in ("alpaca", "internal"), "COALESCE(venue, 'alpaca') = ?", [venue]),
             (bool(book), "book = ?", [book])]
    where = [c for on, c, _ in conds if on]
    params = [p for on, _, ps in conds if on for p in ps]
    sql = ("SELECT order_id, client_order_id, account, symbol, side, type, time_in_force, qty, "
           "limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx, book, venue, "
           "routed_order_id, error, submitted_at, created_at, updated_at, last_event_ts FROM orders")
    sql += (" WHERE " + " AND ".join(where)) if where else ""
    sql += " ORDER BY COALESCE(submitted_at, created_at, updated_at) DESC LIMIT ?"
    return jsonify(_rows(sql, params + [_int_arg("limit", 50)]))

@app.route("/api/fills")
def fills():
    venue = _arg("venue")
    params = [venue] if venue in ("alpaca", "internal") else []
    sql = ("SELECT id, execution_id, order_id, account, symbol, side, price, qty, event, ts, algo_id, "
           "book, venue, cross_id, counter_client_order_id FROM fills"
           + (" WHERE COALESCE(venue, 'alpaca') = ?" if params else ""))
    return jsonify(_rows(sql + " ORDER BY ts DESC, id DESC LIMIT ?", params + [_int_arg("limit", 50)]))

def _fifo_pnl(rows) -> dict:
    lots, pnl = {}, {}
    for f in rows:
        sym, side = f.get("symbol"), str(f.get("side") or "").lower()
        px, qty = _f(f.get("price")), _f(f.get("qty"))
        if not sym or px is None or not qty or qty <= 0:
            continue
        q = lots.setdefault(sym, [])
        pnl.setdefault(sym, 0.0)
        if side == "buy":
            q.append([qty, px])
        elif side == "sell":
            rem = qty
            while rem > 1e-12 and q:  # unmatched remainder (short) is ignored
                take = min(rem, q[0][0])
                pnl[sym] += (px - q[0][1]) * take
                q[0][0] -= take
                rem -= take
                if q[0][0] <= 1e-12:
                    q.pop(0)
    return {k: round(v, 8) for k, v in pnl.items()}

def _by_book(rows) -> list:
    agg = {}
    for f in rows:
        px, qty = _f(f.get("price")) or 0.0, _f(f.get("qty")) or 0.0
        key = (f.get("book") or runtime.DEFAULT_BOOK, f.get("symbol"))
        a = agg.setdefault(key, {"book": key[0], "symbol": key[1], "qty": 0.0, "buy_qty": 0.0,
                                 "sell_qty": 0.0, "buy_notional": 0.0, "sell_notional": 0.0})
        side = str(f.get("side") or "").lower()
        if side in ("buy", "sell"):
            a[f"{side}_qty"] += qty
            a[f"{side}_notional"] += qty * px
        a["qty"] = a["buy_qty"] - a["sell_qty"]
    return sorted(agg.values(), key=lambda x: (str(x["book"]), str(x["symbol"])))

@app.route("/api/summary")
def summary():
    live = _live(request.args.get("live")); acct = _account(live)  # noqa: E702
    out = {"account_type": acct, "account": None, "positions": None, "degraded": False,
           "realized_pnl_by_symbol": {}, "positions_by_book": [], "crossing": {}}
    try:
        out["account"], out["positions"] = trader.account(live), trader.positions(live)
    except Exception as e:
        out.update(account=None, positions=None, degraded=True, error=str(e)[:200])
    rows = _rows("SELECT f.symbol AS symbol, f.side AS side, f.price AS price, f.qty AS qty, "
                 "COALESCE(f.book, o.book, ?) AS book FROM fills f LEFT JOIN orders o "
                 "ON o.order_id = f.order_id WHERE COALESCE(f.account, o.account, 'paper') = ? "
                 "ORDER BY f.ts ASC, f.id ASC", (runtime.DEFAULT_BOOK, acct))
    out["realized_pnl_by_symbol"] = _safe(lambda: _fifo_pnl(rows), {})
    out["positions_by_book"] = _safe(lambda: _by_book(rows), [])
    out["crossing"] = _safe(lambda: _floats(_r().hgetall(runtime.CROSS_STATS_KEY) or {}), {})
    return jsonify(out)

@app.route("/api/order", methods=["POST"])
def post_order():
    b, c = _common("qty")
    typ, tif = str(b.get("type") or "").lower(), str(b.get("tif") or "gtc").lower()
    lp, cross, hold_s = _f(b.get("limit_price")), b.get("cross"), b.get("hold_s")
    _need(typ in ("market", "limit"), "type must be market or limit")
    _need(typ != "limit" or (lp is not None and lp > 0), "limit_price must be > 0 for limit orders")
    _need(tif in ("gtc", "ioc"), "tif must be gtc or ioc")
    _need(cross in (None, "", "allow", "deny"), "cross must be allow or deny")
    _need(hold_s in (None, "") or (_f(hold_s) is not None and _f(hold_s) >= 0), "hold_s must be >= 0")
    payload = {"kind": "submit", "client_order_id": runtime.new_client_order_id("tc"), **c,
               "type": typ, "limit_price": lp if typ == "limit" else None, "tif": tif, "source": "webgui"}
    extra = {"cross": cross if cross in ("allow", "deny") else None, "hold_s": _f(hold_s)}
    payload.update({k: v for k, v in extra.items() if v is not None})
    return _send_oms(payload)

@app.route("/api/order/<order_id>", methods=["DELETE"])
def delete_order(order_id):
    return _send_oms({"kind": "cancel", "account": _account(_live(request.args.get("live"))),
                      "order_id": order_id, "source": "webgui"})

@app.route("/api/algos/strategies")
def strategies():
    return jsonify(_safe(algos.available_strategies, []))

@app.route("/api/algo", methods=["POST"])
def post_algo():
    b, c = _common("total_qty")
    strategy = str(b.get("strategy") or "").strip().lower()
    cls = algos.REGISTRY.get(strategy)
    _need(cls is not None and getattr(cls, "available", False), f"strategy {strategy!r} unknown or unavailable")
    duration_s, n, cap = _f(b.get("duration_s")), _f(b.get("n_slices")), b.get("limit_cap")
    _need(duration_s is not None and duration_s >= 10, "duration_s must be >= 10")
    _need(n is not None and n >= 1 and n == int(n), "n_slices must be an integer >= 1")
    _need(cap in (None, "") or (_f(cap) is not None and _f(cap) > 0), "limit_cap must be > 0")
    params = b.get("params") if b.get("params") is not None else {}
    _need(isinstance(params, dict), "params must be an object")
    return _send_oms({"kind": "algo_create", "strategy": strategy, **c, "duration_s": duration_s,
                      "n_slices": int(n), "limit_cap": _f(cap), "params": params, "source": "webgui"})

@app.route("/api/algos")
def list_algos():
    done = list(SLICE_DONE)
    sql = (f"SELECT a.*, (SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id = a.algo_id "
           f"AND s.status IN ({_qs(done)})) AS slices_done, "
           f"(SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id = a.algo_id) AS slices_total "
           f"FROM algo_orders a")
    if _arg("status") == "running":
        sql += " WHERE a.status IN ('pending', 'running')"
    return jsonify(_rows(sql + " ORDER BY a.created_at DESC LIMIT ?", done + [_int_arg("limit", 100)]))

@app.route("/api/algo/<algo_id>", methods=["GET"])
def get_algo(algo_id):
    algo, slices = None, []
    try:
        r = _r()
        h = r.hgetall(runtime.algo_key(algo_id))
        if h:
            algo = {k: (v if k in _ALGO_STR or _f(v) is None else _f(v)) for k, v in h.items()}
            raw = (r.hgetall(runtime.algo_slices_key(algo_id)) or {}).values()
            slices = [d for d in (_safe(lambda v=v: json.loads(v)) for v in raw) if isinstance(d, dict)]
            slices.sort(key=lambda s: int(_f(s.get("slice_idx")) or 0))
    except Exception as e:
        log.debug("redis algo read failed: %s", e)
    if algo is None:
        rows = _rows("SELECT * FROM algo_orders WHERE algo_id = ?", (algo_id,))
        if rows:
            algo = rows[0]
            slices = _rows("SELECT * FROM algo_slices WHERE algo_id = ? ORDER BY slice_idx", (algo_id,))
    if algo is None:
        return jsonify({"ok": False, "error": "algo not found", "algo_id": algo_id}), 404
    child = [{k: v for k, v in c.items() if k != "raw_json"} for c in
             _rows("SELECT * FROM orders WHERE algo_id = ? ORDER BY slice_idx, submitted_at", (algo_id,))]
    return jsonify({"algo": algo, "slices": slices, "orders": child})

@app.route("/api/algo/<algo_id>", methods=["DELETE"])
def delete_algo(algo_id):
    return _send_oms({"kind": "algo_cancel", "algo_id": algo_id, "source": "webgui"})

@app.route("/api/cross/stats")
def cross_stats():
    r, p = _safe(_r), runtime.CROSS_STATS_KEY + ":"
    keys = _safe(lambda: list(r.scan_iter(match=p + "*", count=200)), [])
    return jsonify({"total": _safe(lambda: _floats(r.hgetall(runtime.CROSS_STATS_KEY)), {}),
                    "by_symbol": {k[len(p):]: _safe(lambda k=k: _floats(r.hgetall(k)), {}) for k in keys}})

def _book_side(r, sym, side) -> list:
    rows, now = [], time.time()
    for coid, score in r.zrange(runtime.book_key(sym, side), 0, -1, desc=(side == "buy"), withscores=True) or []:
        h = r.hgetall(runtime.book_order_key(coid)) or {}
        if h.get("status") and h.get("status") != "resting":
            continue
        px = None if (h.get("type") == "market" or score >= 1e12 or score <= 0) else float(score)
        ts = _f(h.get("created_ts") or h.get("accepted_ts"))
        age = round(now - ts, 1) if ts else _age_s(h.get("created_at") or h.get("accepted_at"))
        qty = _f(h.get("remaining")) if h.get("remaining") is not None else _f(h.get("qty"))
        rows.append({"price": px, "qty": qty, "book": h.get("book") or runtime.DEFAULT_BOOK,
                     "client_order_id": coid, "age_s": age})
    return rows

@app.route("/api/cross/book/<path:symbol>")
def cross_book(symbol):
    sym = symbol.upper()
    return jsonify({"symbol": sym, "bids": _safe(lambda: _book_side(_r(), sym, "buy"), []),
                    "asks": _safe(lambda: _book_side(_r(), sym, "sell"), [])})

def body() -> None:
    log.info("webgui listening on http://%s:%d", WEBGUI_HOST, WEBGUI_PORT)
    app.run(host=WEBGUI_HOST, port=WEBGUI_PORT, debug=False, threaded=True, use_reloader=False)
def main() -> None:
    runtime.run_forever(body, "webgui")
if __name__ == "__main__":
    main()
