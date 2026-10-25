"""Execution Management System worker (algo engine).

- Consumes ``alpaca:ems:requests`` (group ``ems``): create / cancel algo parent orders.
- ``tick`` drives every pending/running algo: refreshes child state from the OMS-owned
  ``orders``/``fills`` tables, rolls unfilled qty of resting children into the next slice,
  prices and submits children THROUGH OMS commands (never calls Alpaca order endpoints).
- Owns SQLite tables ``algo_orders`` and ``algo_slices``. All state needed to resume lives
  there, so a restart simply continues where it left off. No I/O at import time.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import algos
import runtime
import trader

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

ALGO_ACTIVE = ("pending", "running")
ALGO_TERMINAL = frozenset({"completed", "partial", "cancelled", "failed"})
SLICE_TERMINAL = frozenset({"filled", "rolled", "cancelled", "failed"})
SLICE_OPEN = ("submitted", "working")
LOOKUP_AFTER_S = 10.0
LOOKUP_GIVE_UP_S = 120.0
RULES_TTL_S = 300.0

_rules_cache: dict = {}
_warn_last: dict = {}


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


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat(timespec="milliseconds")


def _parse_ts(s) -> float | None:
    if s is None or s == "":
        return None
    if isinstance(s, (int, float)):
        return float(s)
    try:
        return float(s)
    except (TypeError, ValueError):
        pass
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.timestamp()
    except (TypeError, ValueError):
        return None


def _row(row):
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def _warn_throttled(key: str, msg: str, *args, every: float = 30.0) -> None:
    now = time.monotonic()
    if now - _warn_last.get(key, -1e9) >= every:
        _warn_last[key] = now
        log.warning(msg, *args)


def coid_for(algo_id: str, idx: int) -> str:
    return f"algo-{algo_id[:12]}-{int(idx):03d}"


def get_algo(conn, algo_id):
    return _row(conn.execute("SELECT * FROM algo_orders WHERE algo_id=?", (algo_id,)).fetchone())


def get_slices(conn, algo_id) -> list:
    return [_row(x) for x in conn.execute(
        "SELECT * FROM algo_slices WHERE algo_id=? ORDER BY slice_idx", (algo_id,)).fetchall()]


def _set_algo(conn, algo_id, **fields) -> None:
    fields["updated_at"] = runtime.now_iso()
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE algo_orders SET {cols} WHERE algo_id=?", (*fields.values(), algo_id))


def _set_slice(conn, algo_id, idx, **fields) -> None:
    fields["updated_at"] = runtime.now_iso()
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE algo_slices SET {cols} WHERE algo_id=? AND slice_idx=?",
                 (*fields.values(), algo_id, int(idx)))


def _algo_view(conn, algo_id):
    a = get_algo(conn, algo_id)
    if a is None:
        return None
    a["slices"] = get_slices(conn, algo_id)
    return a


def _rules(symbol: str, live: bool, r) -> algos.AssetRules:
    key = (symbol, bool(live))
    hit = _rules_cache.get(key)
    if hit and time.monotonic() - hit[0] < RULES_TTL_S:
        return hit[1]
    try:
        rules = algos.rules_from_dict(trader.get_asset_rules(symbol, live, r=r))
    except Exception as e:
        log.warning("asset rules for %s unavailable: %s", symbol, e)
        return algos.AssetRules()
    _rules_cache[key] = (time.monotonic(), rules)
    return rules


def _send_reply(r, rid, reply) -> None:
    if r is None or not rid:
        return
    try:
        runtime.redis_call(runtime.send_reply, r, str(rid), reply)
    except Exception as e:
        log.error("reply send failed for %s: %s", rid, e)


def _oms_cancel(r, account, client_order_id, order_id=None, timeout=10.0) -> dict:
    payload = {"cmd_id": runtime.new_id(), "kind": "cancel", "account": account,
               "client_order_id": client_order_id, "source": "ems"}
    if order_id:
        payload["order_id"] = order_id
    try:
        return runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=timeout)
    except Exception as e:
        return {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}


def _map_slice_status(order_status, current: str) -> str:
    if current in SLICE_TERMINAL:
        return current
    st = (order_status or "").lower()
    if st == "filled":
        return "filled"
    if st in runtime.ACTIVE_STATUSES:
        return "working"
    if st in ("canceled", "replaced", "done_for_day"):
        return "cancelled"
    if st in ("rejected", "expired", "suspended", "stopped"):
        return "failed"
    return current


# --------------------------------------------------------------------------- requests
def _reply(rid, *, ok, status=None, algo_id=None, error=None, algo=None) -> dict:
    return {"ok": bool(ok), "req_id": rid, "cmd_id": rid, "algo_id": algo_id, "status": status,
            "error": error, "algo": algo}


def _create(conn, r, rid, p) -> dict:
    strategy = str(p.get("strategy") or "twap").strip().lower()
    cls = algos.REGISTRY.get(strategy)
    if cls is None:
        return _reply(rid, ok=False, status="rejected",
                      error=f"unknown strategy {strategy!r}; valid: {', '.join(algos.REGISTRY)}")
    if not getattr(cls, "available", False):
        return _reply(rid, ok=False, status="rejected",
                      error=f"strategy {strategy!r} not available: {getattr(cls, 'note', '')}")
    account = str(p.get("account") or "paper").strip().lower()
    if account not in ("paper", "live"):
        return _reply(rid, ok=False, status="rejected", error="account must be paper or live")
    symbol = str(p.get("symbol") or "").strip().upper()
    if not symbol:
        return _reply(rid, ok=False, status="rejected", error="symbol required")
    side = str(p.get("side") or "").strip().lower()
    if side not in ("buy", "sell"):
        return _reply(rid, ok=False, status="rejected", error="side must be buy or sell")
    total_qty = _f(p.get("total_qty"))
    if total_qty is None or total_qty <= 0:
        return _reply(rid, ok=False, status="rejected", error="total_qty must be > 0")
    try:
        n_slices = int(p.get("n_slices") if p.get("n_slices") not in (None, "") else 5)
        duration_s = float(p.get("duration_s") if p.get("duration_s") not in (None, "") else 300)
    except (TypeError, ValueError):
        return _reply(rid, ok=False, status="rejected", error="n_slices/duration_s must be numbers")
    if n_slices < 1:
        return _reply(rid, ok=False, status="rejected", error="n_slices must be >= 1")
    if duration_s < algos.MIN_INTERVAL_S * n_slices:
        return _reply(rid, ok=False, status="rejected",
                      error=f"duration_s must be >= {algos.MIN_INTERVAL_S:.0f}*n_slices "
                            f"({algos.MIN_INTERVAL_S * n_slices:.0f}s)")
    limit_cap = _f(p.get("limit_cap"))
    if p.get("limit_cap") not in (None, "") and (limit_cap is None or limit_cap <= 0):
        return _reply(rid, ok=False, status="rejected", error="limit_cap must be > 0")
    params = p.get("params") or {}
    if isinstance(params, str):
        try:
            params = json.loads(params) or {}
        except ValueError:
            return _reply(rid, ok=False, status="rejected", error="params must be a JSON object")
    if not isinstance(params, dict):
        return _reply(rid, ok=False, status="rejected", error="params must be an object")
    live = account == "live"
    try:
        rules = algos.rules_from_dict(trader.get_asset_rules(symbol, live, r=r))
    except Exception as e:
        log.warning("asset rules for %s unavailable: %s", symbol, e)
        rules = algos.AssetRules()
    start = runtime.utc_ts() + 2
    end = start + duration_s
    try:
        slices = algos.build_schedule(strategy, total_qty, start, end, n_slices, rules, params)
    except (ValueError, NotImplementedError) as e:
        return _reply(rid, ok=False, status="rejected", error=str(e))
    if not slices:
        return _reply(rid, ok=False, status="rejected", error="empty schedule")
    algo_id = runtime.new_id()
    now = runtime.now_iso()
    if live:
        log.warning("LIVE algo %s requested", algo_id)
    conn.execute(
        "INSERT INTO algo_orders(algo_id, strategy, account, symbol, side, total_qty, n_slices, "
        "start_ts, end_ts, limit_cap, params_json, source, status, filled_qty, avg_px, error, "
        "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (algo_id, strategy, account, symbol, side, float(sum(s.qty for s in slices)), len(slices),
         _iso(start), _iso(end), limit_cap, json.dumps(params, default=str), p.get("source"),
         "pending", 0.0, None, None, now, now))
    for s in slices:
        conn.execute(
            "INSERT OR IGNORE INTO algo_slices(algo_id, slice_idx, scheduled_ts, qty, client_order_id, "
            "order_id, limit_price, status, filled_qty, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (algo_id, s.idx, _iso(s.scheduled_ts), s.qty, coid_for(algo_id, s.idx), None, None,
             "scheduled", 0.0, now))
    conn.commit()
    log.info("algo %s created %s %s %s %s qty=%s slices=%d duration=%.0fs cap=%s", algo_id, strategy,
             account, symbol, side, total_qty, len(slices), duration_s, limit_cap)
    return _reply(rid, ok=True, status="pending", algo_id=algo_id, algo=_algo_view(conn, algo_id))


def _cancel(conn, r, rid, p) -> dict:
    algo_id = p.get("algo_id")
    algo = get_algo(conn, algo_id) if algo_id else None
    if algo is None:
        return _reply(rid, ok=False, status="not_found", algo_id=algo_id, error="algo not found")
    if algo["status"] in ALGO_TERMINAL:
        return _reply(rid, ok=True, status=algo["status"], algo_id=algo_id,
                      algo=_algo_view(conn, algo_id))
    errors = _cancel_children(conn, r, algo)
    _set_algo(conn, algo_id, status="cancelled", error="; ".join(errors)[:500] if errors else None)
    conn.commit()
    log.info("algo %s cancelled (child cancel errors=%d)", algo_id, len(errors))
    return _reply(rid, ok=True, status="cancelled", algo_id=algo_id,
                  error="; ".join(errors)[:500] if errors else None, algo=_algo_view(conn, algo_id))


def _cancel_children(conn, r, algo, timeout=10.0) -> list:
    """Cancel open children via OMS and scheduled slices locally. Returns error strings."""
    algo_id = algo["algo_id"]
    errors = []
    oms_up = r is not None and runtime.worker_alive(r, "oms") is not None
    for s in get_slices(conn, algo_id):
        if s["status"] == "scheduled":
            _set_slice(conn, algo_id, s["slice_idx"], status="cancelled")
        elif s["status"] in SLICE_OPEN:
            if not oms_up:
                errors.append(f"slice {s['slice_idx']}: OMS unavailable, child may remain open")
            else:
                rep = _oms_cancel(r, algo["account"], s["client_order_id"], s["order_id"], timeout)
                if not rep.get("ok") and rep.get("status") != "not_found":
                    errors.append(f"slice {s['slice_idx']}: {rep.get('status')} {rep.get('error')}")
            _set_slice(conn, algo_id, s["slice_idx"], status="cancelled")
    return errors


def handle_request(conn, r, payload: dict) -> dict:
    if not isinstance(payload, dict):
        payload = {}
    rid = str(payload.get("req_id") or payload.get("cmd_id") or runtime.new_id())
    kind = str(payload.get("kind") or "").strip().lower()
    if kind == "create":
        reply = _create(conn, r, rid, payload)
    elif kind == "cancel":
        reply = _cancel(conn, r, rid, payload)
    else:
        reply = _reply(rid, ok=False, status="rejected", error=f"unknown kind {kind!r}")
    conn.commit()
    _send_reply(r, rid, reply)
    return reply


# --------------------------------------------------------------------------- refresh
def refresh_from_oms(conn, algo_id) -> None:
    slices = get_slices(conn, algo_id)
    if not slices:
        return
    by_coid = {s["client_order_id"]: s for s in slices if s["client_order_id"]}
    by_idx = {s["slice_idx"]: s for s in slices}
    prefix = algo_id[:12]
    coids = list(by_coid)
    try:
        q = "SELECT * FROM orders WHERE "
        args: list = []
        if coids:
            q += f"client_order_id IN ({','.join('?' * len(coids))}) OR "
            args += coids
        q += "algo_id LIKE ?"
        args.append(prefix + "%")
        rows = [_row(x) for x in conn.execute(q, args).fetchall()]
    except sqlite3.OperationalError as e:
        log.debug("refresh %s: orders table unavailable: %s", algo_id, e)
        return
    order_ids = []
    seen = set()
    for o in rows:
        s = by_coid.get(o.get("client_order_id"))
        if s is None:
            coid = o.get("client_order_id") or ""
            if not coid.startswith(f"algo-{prefix}-"):
                continue
            s = by_idx.get(o.get("slice_idx"))
        if s is None or s["slice_idx"] in seen:
            continue
        seen.add(s["slice_idx"])
        if o.get("order_id"):
            order_ids.append(o["order_id"])
        filled = max(_f(o.get("filled_qty")) or 0.0, _f(s.get("filled_qty")) or 0.0)
        new_status = _map_slice_status(o.get("status"), s["status"])
        if (new_status != s["status"] or o.get("order_id") != s["order_id"]
                or abs(filled - (_f(s["filled_qty"]) or 0.0)) > 1e-12):
            _set_slice(conn, algo_id, s["slice_idx"], order_id=o.get("order_id") or s["order_id"],
                       filled_qty=filled, status=new_status)
    slices = get_slices(conn, algo_id)
    filled_total = sum(_f(s["filled_qty"]) or 0.0 for s in slices)
    avg_px = None
    if order_ids:
        ph = ",".join("?" * len(order_ids))
        try:
            row = conn.execute(f"SELECT SUM(price*qty) AS n, SUM(qty) AS q FROM fills "
                               f"WHERE order_id IN ({ph}) AND price IS NOT NULL AND qty > 0",
                               order_ids).fetchone()
            if row is not None and row["q"]:
                avg_px = row["n"] / row["q"]
        except sqlite3.OperationalError:
            pass
        if avg_px is None:
            row = conn.execute(f"SELECT SUM(filled_avg_price*filled_qty) AS n, SUM(filled_qty) AS q "
                               f"FROM orders WHERE order_id IN ({ph}) AND filled_avg_price IS NOT NULL "
                               f"AND filled_qty > 0", order_ids).fetchone()
            if row is not None and row["q"]:
                avg_px = row["n"] / row["q"]
    a = get_algo(conn, algo_id)
    if a is not None and (abs((_f(a["filled_qty"]) or 0.0) - filled_total) > 1e-12
                          or (avg_px is not None and a["avg_px"] != avg_px)):
        _set_algo(conn, algo_id, filled_qty=filled_total,
                  avg_px=avg_px if avg_px is not None else a["avg_px"])
    conn.commit()


def _order_active(conn, order_id) -> bool:
    if not order_id:
        return False
    try:
        row = conn.execute("SELECT status FROM orders WHERE order_id=?", (order_id,)).fetchone()
    except sqlite3.OperationalError:
        return False
    return row is not None and (row["status"] or "") in runtime.ACTIVE_STATUSES


# --------------------------------------------------------------------------- tick
def _fail_algo(conn, r, algo_id, error, allow_cancel=True) -> None:
    algo = get_algo(conn, algo_id)
    if algo is None:
        return
    errs = []
    if allow_cancel:
        try:
            errs = _cancel_children(conn, r, algo, timeout=5.0)
        except Exception as e:
            errs = [f"child cancel failed: {e}"]
    else:
        conn.execute("UPDATE algo_slices SET status='cancelled', updated_at=? "
                     "WHERE algo_id=? AND status='scheduled'", (runtime.now_iso(), algo_id))
    text = str(error) + (("; " + "; ".join(errs)) if errs else "")
    _set_algo(conn, algo_id, status="failed", error=text[:1000])
    conn.commit()
    log.error("algo %s failed: %s", algo_id, text)


def _resolve_unknown(conn, r, algo, slices, now) -> bool:
    """Slices stuck in 'submitted' without an order row: look up by client id. False => algo failed."""
    live = algo["account"] == "live"
    for s in slices:
        if s["status"] != "submitted" or s["order_id"]:
            continue
        age = now - (_parse_ts(s["updated_at"]) or now)
        if age < LOOKUP_AFTER_S:
            continue
        try:
            found = trader.get_order_by_client_id(s["client_order_id"], live=live)
        except Exception as e:
            _warn_throttled(f"lookup:{s['client_order_id']}", "lookup %s failed: %s",
                            s["client_order_id"], e)
            continue
        if found:
            if not isinstance(found, dict):
                found = trader._to_dict(found)
            oid = str(found.get("id") or found.get("order_id") or "") or None
            st = _map_slice_status(str(found.get("status") or "").lower(), "submitted")
            conn.execute("UPDATE algo_slices SET order_id=?, filled_qty=?, status=? "
                         "WHERE algo_id=? AND slice_idx=?",
                         (oid, _f(found.get("filled_qty")) or 0.0, st, algo["algo_id"], s["slice_idx"]))
            conn.commit()
            log.info("algo %s slice %d resolved via lookup order=%s status=%s", algo["algo_id"],
                     s["slice_idx"], oid, st)
        elif age >= LOOKUP_GIVE_UP_S:
            _set_slice(conn, algo["algo_id"], s["slice_idx"], status="failed")
            _fail_algo(conn, r, algo["algo_id"],
                       f"slice {s['slice_idx']} ({s['client_order_id']}) not found at broker")
            return False
    return True


def _submit_due(conn, r, algo, now) -> None:
    algo_id = algo["algo_id"]
    account = algo["account"]
    live = account == "live"
    slices = get_slices(conn, algo_id)
    last_idx = max(s["slice_idx"] for s in slices)
    due = [s for s in slices if s["status"] == "scheduled"
           and (_parse_ts(s["scheduled_ts"]) or 0.0) <= now]
    if not due:
        return
    target = due[-1]
    tidx = target["slice_idx"]
    extra = 0.0
    # slices that became due while we were down: merge into the latest due one
    for s in due[:-1]:
        _set_slice(conn, algo_id, s["slice_idx"], status="rolled")
        extra += max(0.0, (_f(s["qty"]) or 0.0) - (_f(s["filled_qty"]) or 0.0))
    # roll forward: cancel resting earlier children, add their unfilled qty
    for s in slices:
        if s["slice_idx"] >= tidx or s["status"] not in SLICE_OPEN:
            continue
        rep = _oms_cancel(r, account, s["client_order_id"], s["order_id"])
        if rep.get("ok") and rep.get("status") == "cancel_requested":
            unfilled = max(0.0, (_f(s["qty"]) or 0.0) - (_f(s["filled_qty"]) or 0.0))
            _set_slice(conn, algo_id, s["slice_idx"], status="rolled")
            extra += unfilled
            log.info("algo %s slice %d rolled unfilled=%s into slice %d", algo_id, s["slice_idx"],
                     unfilled, tidx)
        else:
            log.warning("algo %s roll cancel of slice %d -> %s %s", algo_id, s["slice_idx"],
                        rep.get("status"), rep.get("error"))
    conn.commit()

    rules = _rules(algo["symbol"], live, r)
    slices = get_slices(conn, algo_id)
    total = _f(algo["total_qty"]) or 0.0
    filled_total = sum(_f(s["filled_qty"]) or 0.0 for s in slices)
    later = sum(_f(s["qty"]) or 0.0 for s in slices
                if s["slice_idx"] > tidx and s["status"] == "scheduled")
    outstanding = sum(max(0.0, (_f(s["qty"]) or 0.0) - (_f(s["filled_qty"]) or 0.0))
                      for s in slices if s["slice_idx"] < tidx and s["status"] in SLICE_OPEN)
    cap = total - filled_total - later - outstanding
    new_qty = min((_f(target["qty"]) or 0.0) + extra, cap)
    new_qty = algos.round_qty(max(new_qty, 0.0), rules.min_trade_increment)
    final = tidx == last_idx
    min_size = rules.min_order_size
    if new_qty <= 0 or (min_size and new_qty < min_size):
        nxt = next((s for s in slices if s["slice_idx"] > tidx and s["status"] == "scheduled"), None)
        if nxt is not None and new_qty > 0:
            conn.execute("UPDATE algo_slices SET qty=qty+?, updated_at=? WHERE algo_id=? AND slice_idx=?",
                         (new_qty, runtime.now_iso(), algo_id, nxt["slice_idx"]))
            _set_slice(conn, algo_id, tidx, qty=new_qty, status="rolled")
        else:
            _set_slice(conn, algo_id, tidx, qty=new_qty, status="cancelled")
        conn.commit()
        log.info("algo %s slice %d qty %s too small/none; %s", algo_id, tidx, new_qty,
                 "rolled forward" if nxt is not None and new_qty > 0 else "skipped")
        return
    _set_slice(conn, algo_id, tidx, qty=new_qty)
    conn.commit()

    try:
        params = json.loads(algo["params_json"] or "{}") or {}
    except ValueError:
        params = {}
    strat = algos.get_strategy(algo["strategy"], params)
    try:
        quote = trader.latest_quote(algo["symbol"], r=r)
        px = strat.child_price(algo["side"], quote, _f(algo["limit_cap"]), final, rules)
    except Exception as e:
        _warn_throttled(f"quote:{algo_id}", "algo %s slice %d: no price yet (%s); will retry",
                        algo_id, tidx, e, every=10.0)
        return
    if px is None or px <= 0:
        _warn_throttled(f"quote:{algo_id}", "algo %s slice %d: unusable price %r", algo_id, tidx, px,
                        every=10.0)
        return
    _set_slice(conn, algo_id, tidx, limit_price=px)
    conn.commit()
    payload = {"cmd_id": runtime.new_id(), "kind": "submit", "account": account,
               "client_order_id": target["client_order_id"] or coid_for(algo_id, tidx),
               "symbol": algo["symbol"], "side": algo["side"], "type": "limit", "qty": new_qty,
               "limit_price": px, "tif": "ioc" if final else "gtc", "source": "ems",
               "algo_id": algo_id, "slice_idx": tidx}
    try:
        rep = runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, payload, timeout=10.0)
    except Exception as e:
        rep = {"ok": False, "status": "timeout", "error": f"{type(e).__name__}: {e}"}
    status = rep.get("status")
    if rep.get("ok") and status in ("acked", "already_submitted"):
        order = rep.get("order") if isinstance(rep.get("order"), dict) else {}
        st = _map_slice_status(order.get("status"), "submitted")
        if st == "submitted" and rep.get("order_id"):
            st = "working"
        _set_slice(conn, algo_id, tidx, status=st, order_id=rep.get("order_id"),
                   filled_qty=_f(order.get("filled_qty")) or 0.0)
        conn.commit()
        log.info("algo %s slice %d submitted qty=%s px=%s tif=%s -> %s order=%s", algo_id, tidx,
                 new_qty, px, payload["tif"], status, rep.get("order_id"))
    elif status in ("rejected", "error", "already_submitted", "not_found"):
        _set_slice(conn, algo_id, tidx, status="failed")
        conn.commit()
        _fail_algo(conn, r, algo_id, f"slice {tidx} {status}: {rep.get('error')}")
    else:
        _set_slice(conn, algo_id, tidx, status="submitted")
        _set_algo(conn, algo_id, error=f"slice {tidx} submit {status}: {rep.get('error')}"[:500])
        conn.commit()
        log.warning("algo %s slice %d submit status %s (%s); will resolve later", algo_id, tidx,
                    status, rep.get("error"))


def _maybe_finalize(conn, r, algo_id, now) -> None:
    algo = get_algo(conn, algo_id)
    if algo is None or algo["status"] not in ALGO_ACTIVE:
        return
    slices = get_slices(conn, algo_id)
    total = _f(algo["total_qty"]) or 0.0
    filled = sum(_f(s["filled_qty"]) or 0.0 for s in slices)
    end_ts = _parse_ts(algo["end_ts"]) or now
    done = filled >= total * (1 - 1e-9)
    if done:
        conn.execute("UPDATE algo_slices SET status='cancelled', updated_at=? "
                     "WHERE algo_id=? AND status='scheduled'", (runtime.now_iso(), algo_id))
        slices = get_slices(conn, algo_id)
    non_term = [s for s in slices if s["status"] not in SLICE_TERMINAL]
    if non_term:
        last = slices[-1]
        others_term = all(s["status"] in SLICE_TERMINAL for s in slices[:-1])
        if not (others_term and last["status"] in SLICE_OPEN and now > end_ts + 5):
            if not done:
                return
    else:
        # wait briefly for rolled/cancelled children still active at the broker to settle
        if any(_order_active(conn, s["order_id"]) for s in slices) and now <= end_ts + 5 and not done:
            return
    if done:
        status = "completed"
    elif filled > 0:
        status = "partial"
    else:
        status = "failed"
    _set_algo(conn, algo_id, status=status, filled_qty=filled,
              error=algo["error"] if status != "failed" else (algo["error"] or "no fills"))
    conn.commit()
    log.info("algo %s finished %s filled=%s/%s avg_px=%s", algo_id, status, filled, total,
             algo["avg_px"])


def _process_algo(conn, r, algo_id, now, allow_submit) -> None:
    refresh_from_oms(conn, algo_id)
    algo = get_algo(conn, algo_id)
    if algo is None or algo["status"] not in ALGO_ACTIVE:
        return
    slices = get_slices(conn, algo_id)
    if not slices:
        _fail_algo(conn, r, algo_id, "algo has no slices", allow_cancel=False)
        return
    first_ts = min(_parse_ts(s["scheduled_ts"]) or 0.0 for s in slices)
    if algo["status"] == "pending" and now >= first_ts:
        _set_algo(conn, algo_id, status="running")
        conn.commit()
        algo["status"] = "running"
        log.info("algo %s running", algo_id)
    if not _resolve_unknown(conn, r, algo, slices, now):
        return
    if algo["status"] == "running" and allow_submit and r is not None:
        _submit_due(conn, r, algo, now)
        algo = get_algo(conn, algo_id)
        if algo is None or algo["status"] not in ALGO_ACTIVE:
            return
        refresh_from_oms(conn, algo_id)
    _maybe_finalize(conn, r, algo_id, now)


def tick(conn, r, now_ts: float | None = None, allow_submit: bool = True) -> int:
    now = float(now_ts) if now_ts is not None else runtime.utc_ts()
    try:
        ids = [row["algo_id"] for row in conn.execute(
            "SELECT algo_id FROM algo_orders WHERE status IN ('pending','running') "
            "ORDER BY created_at").fetchall()]
    except sqlite3.OperationalError as e:
        log.warning("tick: algo tables unavailable: %s", e)
        return 0
    n = 0
    for algo_id in ids:
        n += 1
        try:
            _process_algo(conn, r, algo_id, now, allow_submit)
            conn.commit()
        except Exception as e:
            log.exception("algo %s tick failed", algo_id)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                _fail_algo(conn, r, algo_id, f"{type(e).__name__}: {e}", allow_cancel=r is not None)
            except Exception:
                log.exception("could not mark algo %s failed", algo_id)
                try:
                    conn.rollback()
                except Exception:
                    pass
    return n


# --------------------------------------------------------------------------- worker
def _parse_fields(fields) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        raise ValueError("missing json field")
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("json payload is not an object")
    return d


def process_requests(conn, r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        rid = None
        try:
            payload = _parse_fields(fields)
            rid = payload.get("req_id") or payload.get("cmd_id")
            handle_request(conn, r, payload)
            conn.commit()
            reader.ack(eid)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            log.exception("request %s (%s) failed; dead-lettering", eid, rid)
            if rid:
                _send_reply(r, rid, _reply(str(rid), ok=False, status="error",
                                           error=f"ems error: {e!r}"[:500]))
            try:
                reader.dead_letter(eid, fields, repr(e))
            except Exception:
                log.exception("dead_letter failed for %s", eid)
    return len(entries)


def body() -> None:
    r = runtime.get_redis()
    conn = runtime.sqlite_connect()
    try:
        setup_db(conn)
        consumer = os.environ.get("EMS_CONSUMER", "ems-1")
        reader = runtime.GroupReader(r, runtime.STREAM_EMS_REQUESTS, GROUP, consumer, block_ms=500)
        log.info("ems started consumer=%s", consumer)
        while True:
            t0 = time.monotonic()
            process_requests(conn, r, reader)
            oms_up = runtime.worker_alive(r, "oms") is not None
            if not oms_up:
                _warn_throttled("oms_down", "OMS heartbeat missing; skipping child submits this tick")
            tick(conn, r, allow_submit=oms_up)
            elapsed = time.monotonic() - t0
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    runtime.run_forever(body, NAME)


if __name__ == "__main__":
    main()
