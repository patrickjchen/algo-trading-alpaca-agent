"""Command-line interface for the Alpaca crypto pipeline.

Reads Redis market data / SQLite order state; sends every order/algo intent to OMS
(which forwards execution to EMS). Never talks to EMS directly.
Never writes SQLite. Nothing happens at import time.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import runtime
import trader

WORKERS_REQUIRED = ("subscriber", "trade_stream", "oms", "ems")
OMS_DOWN_MSG = "OMS is not running (no heartbeat); start generated/oms_ems.py or use --direct"


# --------------------------------------------------------------------------- helpers
def _out(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _err(msg: str) -> None:
    print(msg, file=sys.stderr)


def _table(rows: list, cols: list | None = None) -> None:
    if not rows:
        print("(no rows)")
        return
    cols = cols or list(rows[0].keys())
    data = [["" if r.get(c) is None else str(r.get(c)) for c in cols] for r in rows]
    widths = [max(len(c), *(len(d[i]) for d in data)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for d in data:
        print("  ".join(v.ljust(w) for v, w in zip(d, widths)))


def _redis():
    r = runtime.get_redis()
    try:
        r.ping()
    except Exception as e:
        _err(f"Redis unreachable at {runtime.REDIS_URL}: {e}")
        sys.exit(2)
    return r


def _db():
    p = runtime.DB_PATH
    if p != ":memory:" and not Path(p).exists():
        _err(f"SQLite database not found at {p} (has the OMS run yet?)")
        sys.exit(1)
    try:
        return runtime.sqlite_connect()
    except Exception as e:
        _err(f"cannot open SQLite database {p}: {e}")
        sys.exit(1)


def _query(conn, sql: str, params=()) -> list:
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError as e:
        _err(f"query failed ({e}); table may not exist yet")
        return []


def _account(args) -> str:
    return "live" if getattr(args, "live", False) else "paper"


def _sym(s: str) -> str:
    s = str(s).strip().upper()
    if "/" not in s and s.endswith("USD") and len(s) > 3:
        s = s[:-3] + "/USD"
    return s


def _age_s(ts: str | None):
    if not ts:
        return None
    try:
        s = str(ts)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - dt).total_seconds(), 1)
    except Exception:
        return None


def _require_oms(r) -> None:
    if runtime.worker_alive(r, "oms") is None:
        _err(OMS_DOWN_MSG)
        sys.exit(2)


def _warn_direct() -> None:
    _err("WARNING: --direct bypasses OMS and EMS; order state will only sync on OMS reconcile")


def _reply_exit(reply: dict) -> int:
    _out(reply)
    return 0 if reply.get("ok") else 1


# --------------------------------------------------------------------------- read-only redis
def cmd_symbols(args) -> int:
    r = _redis()
    syms = sorted(r.smembers("alpaca:symbols") or [])
    if not syms:
        _err("no symbols in Redis (is subscriber running?)")
        return 1
    for s in syms:
        print(s)
    return 0


def cmd_latest(args) -> int:
    r = _redis()
    sym = _sym(args.symbol)
    h = r.hgetall(f"alpaca:latest:{sym}")
    if not h:
        _err(f"no latest data for {sym} in Redis")
        return 1
    _out({"symbol": sym, **h})
    return 0


def cmd_recent(args) -> int:
    r = _redis()
    sym = _sym(args.symbol)
    n = max(1, int(args.n))
    raw = r.lrange(f"alpaca:recent:{sym}:{args.kind}", 0, n - 1)
    if not raw:
        _err(f"no recent {args.kind}s for {sym} in Redis")
        return 1
    items = []
    for x in raw:
        try:
            items.append(json.loads(x))
        except Exception:
            items.append(x)
    _out(items)
    return 0


def cmd_health(args) -> int:
    r = _redis()
    workers = {}
    try:
        keys = sorted(r.scan_iter(match="alpaca:heartbeat:*", count=100))
    except Exception as e:
        _err(f"heartbeat scan failed: {e}")
        keys = []
    for k in keys:
        name = k.split("alpaca:heartbeat:", 1)[-1]
        workers[name] = runtime.worker_alive(r, name)
    for name in WORKERS_REQUIRED:
        workers.setdefault(name, None)
    rows = []
    any_down = False
    for name in sorted(workers):
        hb = workers[name]
        age = _age_s(hb.get("ts")) if hb else None
        state = "OK" if hb else "DOWN"
        if not hb and name in WORKERS_REQUIRED:
            any_down = True
        rows.append({"worker": name, "status": state, "age_s": age,
                     "pid": hb.get("pid") if hb else None, "host": hb.get("host") if hb else None})
    _table(rows, ["worker", "status", "age_s", "pid", "host"])
    lr = None
    try:
        lr = r.get("alpaca:oms:last_reconcile")
    except Exception:
        pass
    print(f"oms last_reconcile: {lr or 'never'}" + (f" ({_age_s(lr)}s ago)" if lr else ""))
    return 1 if any_down else 0


# --------------------------------------------------------------------------- trading
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
    conn = _db()
    try:
        where, params = ["account = ?"], [_account(args)]
        active = sorted(runtime.ACTIVE_STATUSES)
        terminal = sorted(runtime.TERMINAL_STATUSES)
        if args.status == "open":
            where.append(f"status IN ({','.join('?' * len(active))})")
            params += active
        elif args.status == "closed":
            where.append(f"status IN ({','.join('?' * len(terminal))})")
            params += terminal
        sql = (f"SELECT order_id, client_order_id, account, symbol, side, type, time_in_force, qty, "
               f"limit_price, filled_qty, filled_avg_price, status, source, algo_id, slice_idx, "
               f"submitted_at, updated_at FROM orders WHERE {' AND '.join(where)} "
               f"ORDER BY COALESCE(submitted_at, created_at) DESC LIMIT ?")
        params.append(max(1, int(args.limit)))
        _out(_query(conn, sql, params))
    finally:
        conn.close()
    return 0


def _submit(args, side: str) -> int:
    sym = _sym(args.symbol)
    try:
        qty = float(args.qty)
    except ValueError:
        _err(f"invalid qty {args.qty!r}")
        return 2
    if qty <= 0:
        _err("qty must be > 0")
        return 2
    otype = "limit" if args.limit is not None else "market"
    coid = runtime.new_client_order_id("tc")
    if args.direct:
        _warn_direct()
        if otype == "limit":
            res = trader.submit_limit(sym, qty, side, args.limit, coid, live=args.live, tif=args.tif)
        else:
            res = trader.submit_market(sym, qty, side, coid, live=args.live, tif=args.tif)
        _out(res)
        return 0
    r = _redis()
    _require_oms(r)
    payload = {"kind": "submit", "account": _account(args), "client_order_id": coid,
               "symbol": sym, "side": side, "type": otype, "qty": qty,
               "limit_price": args.limit, "tif": args.tif, "source": "cli"}
    return _reply_exit(runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload,
                                            timeout=args.timeout))


def cmd_buy(args) -> int:
    return _submit(args, "buy")


def cmd_sell(args) -> int:
    return _submit(args, "sell")


def cmd_cancel(args) -> int:
    if args.direct:
        _warn_direct()
        trader.cancel(args.order_id, live=args.live)
        _out({"ok": True, "status": "cancel_requested", "order_id": args.order_id})
        return 0
    r = _redis()
    _require_oms(r)
    payload = {"kind": "cancel", "account": _account(args), "source": "cli"}
    if args.client:
        payload["client_order_id"] = args.order_id
    else:
        payload["order_id"] = args.order_id
    return _reply_exit(runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload,
                                            timeout=args.timeout))


def cmd_cancel_all(args) -> int:
    if args.direct:
        _warn_direct()
        _out(trader.cancel_all(live=args.live))
        return 0
    r = _redis()
    _require_oms(r)
    conn = _db()
    try:
        active = sorted(runtime.ACTIVE_STATUSES)
        rows = _query(conn, f"SELECT order_id, client_order_id FROM orders WHERE account = ? AND "
                            f"status IN ({','.join('?' * len(active))})",
                      [_account(args), *active])
    finally:
        conn.close()
    if not rows:
        _out({"ok": True, "canceled": 0, "replies": []})
        return 0
    replies, ok = [], True
    for row in rows:
        payload = {"kind": "cancel", "account": _account(args), "source": "cli",
                   "order_id": row.get("order_id"), "client_order_id": row.get("client_order_id")}
        rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=args.timeout)
        ok = ok and bool(rep.get("ok"))
        replies.append(rep)
    _out({"ok": ok, "canceled": len(replies), "replies": replies})
    return 0 if ok else 1


# --------------------------------------------------------------------------- algos
def cmd_twap(args) -> int:
    sym = _sym(args.symbol)
    side = args.side.lower()
    try:
        qty = float(args.qty)
    except ValueError:
        _err(f"invalid qty {args.qty!r}")
        return 2
    if qty <= 0 or args.duration <= 0 or args.slices <= 0:
        _err("qty, --duration and --slices must be > 0")
        return 2
    r = _redis()
    _require_oms(r)
    payload = {"kind": "algo_create", "strategy": "twap",
               "account": _account(args), "symbol": sym, "side": side, "total_qty": qty,
               "duration_s": float(args.duration), "n_slices": int(args.slices),
               "limit_cap": args.limit_cap, "params": {}, "source": "cli"}
    return _reply_exit(runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload,
                                            timeout=args.timeout))


def cmd_algos(args) -> int:
    conn = _db()
    try:
        sql = "SELECT * FROM algo_orders"
        params: list = []
        if args.status == "running":
            sql += " WHERE status IN ('running', 'pending')"
        sql += " ORDER BY rowid DESC LIMIT ?"
        params.append(max(1, int(args.limit)))
        algos = _query(conn, sql, params)
        for a in algos:
            aid = a.get("algo_id")
            counts = _query(conn, "SELECT status, COUNT(*) AS n FROM algo_slices WHERE algo_id = ? "
                                  "GROUP BY status", (aid,))
            a["slice_counts"] = {c.get("status"): c.get("n") for c in counts}
            a["slices_total"] = sum(int(c.get("n") or 0) for c in counts)
        _out(algos)
    finally:
        conn.close()
    return 0


def cmd_algo(args) -> int:
    conn = _db()
    try:
        rows = _query(conn, "SELECT * FROM algo_orders WHERE algo_id = ?", (args.algo_id,))
        if not rows:
            rows = _query(conn, "SELECT * FROM algo_orders WHERE algo_id LIKE ? LIMIT 2",
                          (args.algo_id + "%",))
            if len(rows) != 1:
                _err(f"algo {args.algo_id} not found")
                return 1
        algo = rows[0]
        aid = algo.get("algo_id")
        slices = _query(conn, "SELECT * FROM algo_slices WHERE algo_id = ? ORDER BY rowid", (aid,))
        children = _query(conn, "SELECT order_id, client_order_id, symbol, side, type, time_in_force, "
                                "qty, limit_price, filled_qty, filled_avg_price, status, slice_idx, "
                                "submitted_at, updated_at FROM orders WHERE algo_id = ? OR "
                                "client_order_id LIKE ? ORDER BY slice_idx",
                          (aid, f"algo-{str(aid)[:12]}-%"))
        _out({"algo": algo, "slices": slices, "child_orders": children})
    finally:
        conn.close()
    return 0


def cmd_algo_cancel(args) -> int:
    r = _redis()
    _require_oms(r)
    payload = {"kind": "algo_cancel", "algo_id": args.algo_id, "source": "cli"}
    return _reply_exit(runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload,
                                            timeout=args.timeout))


# --------------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cli.py", description="Alpaca crypto pipeline CLI")
    sub = p.add_subparsers(dest="cmd", metavar="COMMAND")
    sub.required = True

    def add(name, fn, help_, live=False, direct=False, timeout=False):
        sp = sub.add_parser(name, help=help_)
        sp.set_defaults(func=fn)
        if live:
            sp.add_argument("--live", action="store_true", help="use the LIVE account (default paper)")
        else:
            sp.set_defaults(live=False)
        if direct:
            sp.add_argument("--direct", action="store_true", help="bypass OMS, call Alpaca directly")
        else:
            sp.set_defaults(direct=False)
        if timeout:
            sp.add_argument("--timeout", type=float, default=15.0, help="reply timeout seconds")
        return sp

    add("symbols", cmd_symbols, "list symbols seen in Redis")
    sp = add("latest", cmd_latest, "latest trade/quote for a symbol")
    sp.add_argument("symbol")
    sp = add("recent", cmd_recent, "recent trades/quotes for a symbol")
    sp.add_argument("symbol")
    sp.add_argument("--kind", choices=["trade", "quote"], default="trade")
    sp.add_argument("-n", type=int, default=20)
    add("health", cmd_health, "worker heartbeats")

    add("account", cmd_account, "account info (REST)", live=True)
    add("positions", cmd_positions, "positions (REST)", live=True)
    sp = add("orders", cmd_orders, "orders (SQLite; --direct for REST)", live=True, direct=True)
    sp.add_argument("--status", choices=["open", "closed", "all"], default="open")
    sp.add_argument("--limit", type=int, default=50)
    for name, fn in (("buy", cmd_buy), ("sell", cmd_sell)):
        sp = add(name, fn, f"{name} via OMS", live=True, direct=True, timeout=True)
        sp.add_argument("symbol")
        sp.add_argument("qty")
        sp.add_argument("--limit", type=float, default=None, help="limit price (else market)")
        sp.add_argument("--tif", choices=["gtc", "ioc"], default="gtc")
    sp = add("cancel", cmd_cancel, "cancel an order via OMS", live=True, direct=True, timeout=True)
    sp.add_argument("order_id")
    sp.add_argument("--client", action="store_true", help="ORDER_ID is a client_order_id")
    add("cancel-all", cmd_cancel_all, "cancel all open orders", live=True, direct=True, timeout=True)

    sp = add("twap", cmd_twap, "start a TWAP algo via OMS", live=True, timeout=True)
    sp.add_argument("symbol")
    sp.add_argument("side", choices=["buy", "sell"], type=str.lower)
    sp.add_argument("qty")
    sp.add_argument("--duration", type=float, default=300.0, help="seconds")
    sp.add_argument("--slices", type=int, default=10)
    sp.add_argument("--limit-cap", dest="limit_cap", type=float, default=None)
    sp = add("algos", cmd_algos, "list algos (SQLite)")
    sp.add_argument("--status", choices=["running", "all"], default="all")
    sp.add_argument("--limit", type=int, default=50)
    sp = add("algo", cmd_algo, "algo detail (SQLite)")
    sp.add_argument("algo_id")
    sp = add("algo-cancel", cmd_algo_cancel, "cancel an algo via OMS", timeout=True)
    sp.add_argument("algo_id")
    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "live", False):
        _err("WARNING: --live mode -- this places real orders")
    try:
        rc = args.func(args)
    except KeyboardInterrupt:
        return 130
    except SystemExit:
        raise
    except Exception as e:
        try:
            info = trader.error_info(e)
        except Exception:
            info = {"type": type(e).__name__, "message": str(e)}
        _err(f"error: {info}")
        return 1
    return int(rc or 0)


if __name__ == "__main__":
    sys.exit(main())
