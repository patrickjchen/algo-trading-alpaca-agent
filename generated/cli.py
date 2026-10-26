"""Command-line interface for the Alpaca crypto pipeline.

Every trading intent goes through OMS (alpaca:oms:commands) unless --direct is given.
Reads live state from Redis and history from SQLite (read-only; the persister writes it).
No I/O at import time.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import runtime
import trader

WORKERS_REQUIRED = ("subscriber", "trade_stream", "oms", "ems", "crossing", "algo_engine", "persister")
OMS_DOWN_MSG = "OMS is not running (no heartbeat); start generated/oms_ems.py or use --direct"


# --------------------------------------------------------------------------- helpers
def _out(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _err(msg: str) -> None:
    print(msg, file=sys.stderr)


def _table(rows: list, cols: list) -> None:
    if not rows:
        print("(no rows)")
        return
    data = [[("" if r.get(c) is None else str(r.get(c))) for c in cols] for r in rows]
    widths = [max(len(c), *(len(d[i]) for d in data)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for d in data:
        print("  ".join(v.ljust(w) for v, w in zip(d, widths)))


def _f(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _floatify(d: dict) -> dict:
    out = {}
    for k, v in (d or {}).items():
        fv = _f(v)
        out[k] = fv if fv is not None else v
    return out


def _parse_dt(v):
    if v in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except (TypeError, ValueError):
        pass
    try:
        s = str(v)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _redis():
    r = runtime.get_redis()
    try:
        r.ping()
    except Exception as e:
        _err(f"Redis unreachable at {runtime.REDIS_URL}: {e}")
        sys.exit(2)
    return r


def _db():
    """Read-only-by-convention SQLite connection, or None when the DB does not exist yet."""
    path = runtime.DB_PATH
    if path != ":memory:" and not os.path.exists(path):
        return None
    try:
        return runtime.sqlite_connect()
    except Exception as e:
        _err(f"cannot open SQLite {path}: {e}")
        return None


def _query(sql: str, params=()) -> list | None:
    conn = _db()
    if conn is None:
        return None
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    except Exception as e:
        _err(f"SQLite query failed: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _account(args) -> str:
    return "live" if getattr(args, "live", False) else "paper"


def _require_oms(r) -> None:
    if runtime.worker_alive(r, "oms") is None:
        _err(OMS_DOWN_MSG)
        sys.exit(2)


def _send(r, payload: dict, timeout: float = 15.0) -> dict:
    reply = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=timeout)
    _out(reply)
    return reply


def _warn_direct() -> None:
    _err("WARNING: --direct bypasses OMS/EMS (no dedupe, no live state, no crossing)")


# --------------------------------------------------------------------------- read-only redis
def cmd_symbols(args) -> int:
    r = _redis()
    syms = sorted(r.smembers("alpaca:symbols") or [])
    if not syms:
        _err("no symbols in Redis (is the subscriber running?)")
        return 1
    for s in syms:
        print(s)
    return 0


def cmd_latest(args) -> int:
    r = _redis()
    sym = args.symbol.upper()
    h = r.hgetall(f"alpaca:latest:{sym}")
    if not h:
        _err(f"no latest data for {sym} in Redis")
        return 1
    _out(_floatify(h))
    return 0


def cmd_recent(args) -> int:
    r = _redis()
    sym = args.symbol.upper()
    raw = r.lrange(f"alpaca:recent:{sym}:{args.kind}", 0, max(0, args.n - 1)) or []
    if not raw:
        _err(f"no recent {args.kind}s for {sym} in Redis")
        return 1
    items = []
    for x in raw:
        try:
            items.append(json.loads(x))
        except (TypeError, ValueError):
            items.append(x)
    _out(items)
    return 0


def cmd_health(args) -> int:
    r = _redis()
    now = time.time()
    seen = {}
    for key in r.scan_iter(match="alpaca:heartbeat:*", count=200):
        name = key.split("alpaca:heartbeat:", 1)[1]
        seen[name] = runtime.worker_alive(r, name)
    rows = []
    any_down = False
    for name in sorted(set(seen) | set(WORKERS_REQUIRED)):
        hb = seen.get(name)
        if hb is None:
            hb = runtime.worker_alive(r, name)
        age = None
        if hb:
            dt = _parse_dt(hb.get("ts"))
            age = round(now - dt.timestamp(), 1) if dt else None
        status = "OK" if hb else "DOWN"
        if status == "DOWN" and name in WORKERS_REQUIRED:
            any_down = True
        rows.append({"worker": name, "status": status, "age_s": age,
                     "pid": (hb or {}).get("pid"), "host": (hb or {}).get("host")})
    _table(rows, ["worker", "status", "age_s", "pid", "host"])
    print()
    try:
        last_rec = r.get("alpaca:oms:last_reconcile")
    except Exception:
        last_rec = None
    print(f"oms last_reconcile : {last_rec or '-'}")
    print(f"open orders        : {r.scard(runtime.SET_OPEN_ORDERS)}")
    print(f"active algos       : {r.scard(runtime.SET_ACTIVE_ALGOS)}")
    print(f"resting (crossing) : {r.scard(runtime.SET_RESTING)}")
    return 1 if any_down else 0


# --------------------------------------------------------------------------- account
def cmd_account(args) -> int:
    _out(trader.account(args.live))
    return 0


def cmd_positions(args) -> int:
    _out(trader.positions(args.live))
    return 0


def cmd_orders(args) -> int:
    if args.direct:
        _warn_direct()
        _out(trader.list_orders(status=args.status, limit=args.limit, live=args.live))
        return 0
    term = sorted(runtime.TERMINAL_STATUSES)
    ph = ",".join("?" for _ in term)
    sql = ("SELECT order_id, client_order_id, account, symbol, side, type, qty, limit_price, "
           "filled_qty, filled_avg_price, status, source, book, venue, algo_id, submitted_at, "
           "updated_at FROM orders WHERE account = ?")
    params: list = [_account(args)]
    if args.status == "open":
        sql += f" AND (status IS NULL OR status NOT IN ({ph}))"
        params += term
    elif args.status == "closed":
        sql += f" AND status IN ({ph})"
        params += term
    sql += " ORDER BY COALESCE(updated_at, submitted_at) DESC LIMIT ?"
    params.append(int(args.limit))
    rows = _query(sql, params)
    if rows is None:
        _err(f"no order history (SQLite {runtime.DB_PATH} missing or unreadable)")
        return 1
    if args.json:
        _out(rows)
    else:
        _table(rows, ["order_id", "client_order_id", "symbol", "side", "type", "qty",
                      "limit_price", "filled_qty", "status", "venue", "book", "source", "updated_at"])
    return 0


def cmd_order(args) -> int:
    ident = args.id
    try:
        r = runtime.get_redis()
        h = r.hgetall(runtime.order_key(ident))
        if not h:
            oid = r.get(runtime.coid_key(ident))
            if oid:
                h = r.hgetall(runtime.order_key(oid))
        if h:
            _out(_floatify(h))
            return 0
    except Exception as e:
        _err(f"Redis lookup failed ({e}); falling back to SQLite")
    rows = _query("SELECT * FROM orders WHERE order_id = ? OR client_order_id = ? "
                  "OR order_id = ?", (ident, ident, runtime.internal_order_id(ident)))
    if rows:
        _out(rows if len(rows) > 1 else rows[0])
        return 0
    if args.direct:
        _warn_direct()
        o = trader.get_order_by_client_id(ident, args.live)
        if o is None:
            try:
                o = trader.get_order(ident, args.live)
            except Exception as e:
                _err(f"order {ident} not found: {e}")
                return 1
        _out(o)
        return 0
    _err(f"order {ident} not found in Redis or SQLite")
    return 1


# --------------------------------------------------------------------------- trading
def _submit(args, side: str) -> int:
    typ = "limit" if args.limit is not None else "market"
    coid = runtime.new_client_order_id("tc")
    if args.direct:
        _warn_direct()
        if typ == "limit":
            o = trader.submit_limit(args.symbol, args.qty, side, args.limit, coid,
                                    live=args.live, tif=args.tif)
        else:
            o = trader.submit_market(args.symbol, args.qty, side, coid, live=args.live, tif=args.tif)
        _out(o)
        return 0
    r = _redis()
    _require_oms(r)
    payload = {"kind": "submit", "account": _account(args), "client_order_id": coid,
               "symbol": args.symbol.upper(), "side": side, "type": typ, "qty": float(args.qty),
               "limit_price": float(args.limit) if args.limit is not None else None,
               "tif": args.tif, "source": "cli", "book": args.book or runtime.DEFAULT_BOOK}
    if args.no_cross:
        payload["cross"] = "deny"
    if args.hold is not None:
        payload["hold_s"] = float(args.hold)
    reply = _send(r, payload)
    return 0 if reply.get("ok") else 1


def cmd_buy(args) -> int:
    return _submit(args, "buy")


def cmd_sell(args) -> int:
    return _submit(args, "sell")


def _cancel_payload(args, ident: str) -> dict:
    p = {"kind": "cancel", "account": _account(args), "source": "cli"}
    if ident.startswith(("tc-", "algo-")) or not _looks_uuid(ident):
        p["client_order_id"] = ident
        if ident.startswith("int-"):
            p["order_id"] = ident
            p.pop("client_order_id")
    else:
        p["order_id"] = ident
    return p


def _looks_uuid(s: str) -> bool:
    import uuid
    try:
        uuid.UUID(str(s))
        return True
    except ValueError:
        return False


def cmd_cancel(args) -> int:
    if args.direct:
        _warn_direct()
        trader.cancel(args.order_id, args.live)
        _out({"ok": True, "status": "cancel_requested", "order_id": args.order_id})
        return 0
    r = _redis()
    _require_oms(r)
    reply = _send(r, _cancel_payload(args, args.order_id))
    return 0 if reply.get("ok") else 1


def cmd_cancel_all(args) -> int:
    if args.direct:
        _warn_direct()
        _out(trader.cancel_all(args.live))
        return 0
    r = _redis()
    _require_oms(r)
    term = sorted(runtime.TERMINAL_STATUSES)
    ph = ",".join("?" for _ in term)
    rows = _query(f"SELECT order_id, client_order_id FROM orders WHERE account = ? "
                  f"AND (status IS NULL OR status NOT IN ({ph}))", [_account(args)] + term)
    ids = []
    for row in rows or []:
        if row.get("order_id"):
            ids.append(row["order_id"])
    if not ids:
        # fall back to the live open set in Redis
        for oid in r.smembers(runtime.SET_OPEN_ORDERS) or []:
            acct = r.hget(runtime.order_key(oid), "account") or "paper"
            if acct == _account(args):
                ids.append(oid)
    if not ids:
        print("no open orders")
        return 0
    failed = 0
    for oid in sorted(set(ids)):
        p = {"kind": "cancel", "account": _account(args), "order_id": oid, "source": "cli"}
        reply = _send(r, p)
        if not reply.get("ok"):
            failed += 1
    return 1 if failed else 0


# --------------------------------------------------------------------------- algos
def cmd_twap(args) -> int:
    r = _redis()
    _require_oms(r)
    payload = {"kind": "algo_create", "strategy": "twap", "account": _account(args),
               "book": args.book or runtime.DEFAULT_BOOK, "symbol": args.symbol.upper(),
               "side": args.side, "total_qty": float(args.qty), "duration_s": float(args.duration),
               "n_slices": int(args.slices),
               "limit_cap": float(args.limit_cap) if args.limit_cap is not None else None,
               "params": {}, "source": "cli"}
    reply = _send(r, payload)
    return 0 if reply.get("ok") else 1


def cmd_algos(args) -> int:
    if args.status == "running":
        r = _redis()
        out = []
        for aid in sorted(r.smembers(runtime.SET_ACTIVE_ALGOS) or []):
            h = r.hgetall(runtime.algo_key(aid))
            if h:
                out.append(_floatify(h))
        if not out:
            print("no running algos")
            return 0
        _out(out)
        return 0
    rows = _query("SELECT a.*, (SELECT COUNT(*) FROM algo_slices s WHERE s.algo_id = a.algo_id) "
                  "AS slice_count FROM algo_orders a ORDER BY a.created_at DESC")
    if rows is None:
        _err(f"no algo history (SQLite {runtime.DB_PATH} missing or unreadable)")
        return 1
    _out(rows)
    return 0


def cmd_algo(args) -> int:
    aid = args.algo_id
    result: dict = {}
    try:
        r = runtime.get_redis()
        h = r.hgetall(runtime.algo_key(aid))
        if h:
            result["algo"] = _floatify(h)
            result["source"] = "redis"
            sl = r.hgetall(runtime.algo_slices_key(aid)) or {}
            slices = []
            for k, v in sl.items():
                try:
                    slices.append(json.loads(v))
                except (TypeError, ValueError):
                    slices.append({"slice_idx": k, "raw": v})
            slices.sort(key=lambda s: int(s.get("slice_idx", 0)) if str(s.get("slice_idx", "0")).isdigit() else 0)
            result["slices"] = slices
    except Exception as e:
        _err(f"Redis lookup failed ({e}); falling back to SQLite")
    if "algo" not in result:
        rows = _query("SELECT * FROM algo_orders WHERE algo_id = ?", (aid,))
        if rows:
            result["algo"] = rows[0]
            result["source"] = "sqlite"
            result["slices"] = _query("SELECT * FROM algo_slices WHERE algo_id = ? ORDER BY slice_idx",
                                      (aid,)) or []
    if "algo" not in result:
        _err(f"algo {aid} not found in Redis or SQLite")
        return 1
    result["child_orders"] = _query("SELECT * FROM orders WHERE algo_id = ? OR algo_id = ? "
                                    "ORDER BY slice_idx", (aid, aid[:12])) or []
    _out(result)
    return 0


def cmd_algo_cancel(args) -> int:
    r = _redis()
    _require_oms(r)
    reply = _send(r, {"kind": "algo_cancel", "algo_id": args.algo_id, "source": "cli"})
    return 0 if reply.get("ok") else 1


# --------------------------------------------------------------------------- crossing
def cmd_book(args) -> int:
    r = _redis()
    sym = args.symbol.upper()
    now = time.time()
    out = {"symbol": sym, "buy": [], "sell": []}
    for side in ("buy", "sell"):
        members = r.zrange(runtime.book_key(sym, side), 0, -1, withscores=True, desc=(side == "buy")) or []
        for coid, score in members:
            h = r.hgetall(runtime.book_order_key(coid)) or {}
            created = None
            for f in ("created_at", "accepted_at", "ts", "received_at"):
                created = _parse_dt(h.get(f))
                if created:
                    break
            price = _f(h.get("limit_price"))
            if price is None and score not in (0, 1e12):
                price = score
            out[side].append({
                "client_order_id": coid, "price": price if price is not None else "market",
                "remaining": _f(h.get("remaining")), "qty": _f(h.get("qty")),
                "book": h.get("book"), "status": h.get("status"), "account": h.get("account"),
                "age_s": round(now - created.timestamp(), 1) if created else None,
            })
    if not out["buy"] and not out["sell"]:
        print(f"no resting internal interest for {sym}")
        return 0
    _out(out)
    return 0


def cmd_cross_stats(args) -> int:
    r = _redis()
    key = runtime.cross_stats_key(args.symbol.upper()) if args.symbol else runtime.CROSS_STATS_KEY
    h = r.hgetall(key)
    if not h:
        _err(f"no crossing stats in Redis ({key})")
        return 1
    _out(_floatify(h))
    return 0


def cmd_positions_by_book(args) -> int:
    rows = _query("SELECT COALESCE(book, ?) AS book, symbol, "
                  "SUM(CASE WHEN side = 'buy' THEN qty ELSE 0 END) - "
                  "SUM(CASE WHEN side = 'sell' THEN qty ELSE 0 END) AS net_qty, "
                  "SUM(CASE WHEN venue = 'internal' THEN 1 ELSE 0 END) AS internal_fills, "
                  "SUM(CASE WHEN venue != 'internal' OR venue IS NULL THEN 1 ELSE 0 END) AS external_fills "
                  "FROM fills WHERE account = ? GROUP BY COALESCE(book, ?), symbol ORDER BY 1, 2",
                  (runtime.DEFAULT_BOOK, _account(args), runtime.DEFAULT_BOOK))
    if rows is None:
        _err(f"no fill history (SQLite {runtime.DB_PATH} missing or unreadable)")
        return 1
    _out(rows)
    return 0


# --------------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cli.py", description="Alpaca crypto pipeline CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add(name, fn, help_, live=False, direct=False):
        sp = sub.add_parser(name, help=help_)
        sp.set_defaults(func=fn)
        if live:
            sp.add_argument("--live", action="store_true", help="LIVE account (real money)")
        if direct:
            sp.add_argument("--direct", action="store_true", help="bypass OMS/EMS, call trader directly")
        return sp

    add("symbols", cmd_symbols, "symbols seen by the subscriber")
    sp = add("latest", cmd_latest, "latest trade/quote for a symbol")
    sp.add_argument("symbol")
    sp = add("recent", cmd_recent, "recent trades/quotes")
    sp.add_argument("symbol")
    sp.add_argument("--kind", choices=["trade", "quote"], default="trade")
    sp.add_argument("-n", type=int, default=20)
    add("health", cmd_health, "worker heartbeats")

    add("account", cmd_account, "account info (REST)", live=True)
    add("positions", cmd_positions, "positions (REST)", live=True)
    sp = add("orders", cmd_orders, "order history (SQLite)", live=True, direct=True)
    sp.add_argument("--status", choices=["open", "closed", "all"], default="all")
    sp.add_argument("--limit", type=int, default=50)
    sp.add_argument("--json", action="store_true")
    sp = add("order", cmd_order, "one order (live Redis, else SQLite)", live=True, direct=True)
    sp.add_argument("id", metavar="ORDER_ID_or_CLIENT_ORDER_ID")

    for side, fn in (("buy", cmd_buy), ("sell", cmd_sell)):
        sp = add(side, fn, f"{side} via OMS", live=True, direct=True)
        sp.add_argument("symbol")
        sp.add_argument("qty", type=float)
        sp.add_argument("--limit", type=float, default=None, help="limit price")
        sp.add_argument("--tif", choices=["gtc", "ioc"], default="gtc")
        sp.add_argument("--book", default=runtime.DEFAULT_BOOK)
        sp.add_argument("--no-cross", action="store_true", dest="no_cross")
        sp.add_argument("--hold", type=float, default=None, help="internal hold window seconds")

    sp = add("cancel", cmd_cancel, "cancel one order via OMS", live=True, direct=True)
    sp.add_argument("order_id")
    add("cancel-all", cmd_cancel_all, "cancel all open orders via OMS", live=True, direct=True)

    sp = add("twap", cmd_twap, "start a TWAP algo via OMS", live=True)
    sp.add_argument("symbol")
    sp.add_argument("side", choices=["buy", "sell"])
    sp.add_argument("qty", type=float)
    sp.add_argument("--duration", type=float, default=300.0)
    sp.add_argument("--slices", type=int, default=10)
    sp.add_argument("--limit-cap", type=float, default=None, dest="limit_cap")
    sp.add_argument("--book", default=runtime.DEFAULT_BOOK)
    sp = add("algos", cmd_algos, "list algos")
    sp.add_argument("--status", choices=["running", "all"], default="running")
    sp = add("algo", cmd_algo, "one algo with slices and child orders")
    sp.add_argument("algo_id")
    sp = add("algo-cancel", cmd_algo_cancel, "cancel an algo via OMS")
    sp.add_argument("algo_id")

    sp = add("book", cmd_book, "resting internal interest (crossing)")
    sp.add_argument("symbol")
    sp = add("cross-stats", cmd_cross_stats, "crossing savings stats")
    sp.add_argument("symbol", nargs="?", default=None)
    add("positions-by-book", cmd_positions_by_book, "net positions per book from fills", live=True)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if getattr(args, "live", False):
        _err("WARNING: --live mode -- this places real orders")
    try:
        rc = args.func(args)
    except KeyboardInterrupt:
        return 130
    except SystemExit:
        raise
    except Exception as e:
        _err(f"error: {type(e).__name__}: {e}")
        return 1
    return int(rc or 0)


if __name__ == "__main__":
    sys.exit(main())
