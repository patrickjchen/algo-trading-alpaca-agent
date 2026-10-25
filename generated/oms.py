"""OMS worker: front door for every order intent and owner of the order LIFECYCLE.

Consumes ``alpaca:oms:commands`` (submit / cancel / algo_create / algo_cancel) and ``alpaca:trade_updates``
(group ``oms``). Validates, dedupes on client_order_id (Redis), forwards execution to EMS
(``alpaca:ems:requests``) and algos to the algo engine (``alpaca:algo:requests``), relays replies, keeps
live order state in Redis hashes and emits snapshots to ``alpaca:oms:events``.
Never calls Alpaca order endpoints; never touches SQLite. No I/O at import time.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import runtime
import trader

log = runtime.log("oms")

COID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
ALGO_COID_RE = re.compile(r"^algo-([A-Za-z0-9]+)-(\d+)$")
KINDS = ("submit", "cancel", "algo_create", "algo_cancel")
NUMERIC_FIELDS = ("qty", "limit_price", "filled_qty", "filled_avg_price")
LAST_RECONCILE_KEY = "alpaca:oms:last_reconcile"


# --------------------------------------------------------------------------- helpers
def _float(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _empty(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() in ("", "None", "null"))


def _s(v):
    if v is None:
        return None
    return runtime._sval(v)


def _derive_algo(coid):
    if not coid:
        return None, None
    m = ALGO_COID_RE.match(str(coid))
    if not m:
        return None, None
    return m.group(1), str(int(m.group(2)))


# --------------------------------------------------------------------------- live state
def apply_order(r, order: dict, *, account, source=None, algo_id=None, slice_idx=None,
                event_ts=None) -> dict:
    new = runtime.normalize_order(order or {})
    order_id = new.get("order_id")
    if not order_id:
        return {}
    key = runtime.order_key(order_id)
    old = runtime.redis_call(r.hgetall, key) or {}

    old_status = old.get("status") or None
    new_status = new.get("status")
    if old_status in runtime.TERMINAL_STATUSES and new_status not in runtime.TERMINAL_STATUSES:
        status = old_status
    else:
        status = new_status or old_status

    fq_old, fq_new = _float(old.get("filled_qty")), new.get("filled_qty")
    vals = [x for x in (fq_old, fq_new) if x is not None]
    filled_qty = max(vals) if vals else None

    merged: dict = {}
    for k, v in new.items():
        merged[k] = _s(v) if v is not None else old.get(k)
    merged["status"] = status
    merged["filled_qty"] = _s(filled_qty) if filled_qty is not None else old.get("filled_qty")
    for k in ("filled_avg_price", "limit_price"):
        merged[k] = _s(new.get(k)) if new.get(k) is not None else old.get(k)

    d_algo, d_idx = _derive_algo(merged.get("client_order_id"))
    for k, given, derived in (("source", source, "algo" if d_algo else None),
                              ("algo_id", algo_id, d_algo),
                              ("slice_idx", slice_idx, d_idx)):
        ov = old.get(k)
        if not _empty(ov):
            merged[k] = ov
        elif not _empty(given):
            merged[k] = _s(given)
        elif derived is not None:
            merged[k] = derived
        else:
            merged[k] = None

    merged["account"] = "live" if account == "live" else "paper"
    merged["last_event_ts"] = _s(event_ts) if event_ts else runtime.now_iso()
    merged["updated_at"] = runtime.now_iso()
    merged["order_id"] = str(order_id)
    merged = {k: v for k, v in merged.items() if v is not None}

    terminal = status in runtime.TERMINAL_STATUSES
    runtime.redis_call(runtime.hset_state, r, key, merged,
                       ttl=runtime.STATE_TTL_S if terminal else None)
    coid = merged.get("client_order_id")
    if coid:
        if terminal:
            runtime.redis_call(r.set, runtime.coid_key(coid), order_id, ex=runtime.STATE_TTL_S)
        else:
            runtime.redis_call(r.set, runtime.coid_key(coid), order_id)
    if terminal:
        runtime.redis_call(r.srem, runtime.SET_OPEN_ORDERS, order_id)
    else:
        runtime.redis_call(r.sadd, runtime.SET_OPEN_ORDERS, order_id)
    return merged


def changed(old: dict, merged: dict) -> bool:
    old = old or {}
    merged = merged or {}
    if (old.get("status") or None) != (merged.get("status") or None):
        return True
    for k in ("filled_qty", "filled_avg_price", "limit_price"):
        if _float(old.get(k)) != _float(merged.get(k)):
            return True
    return False


def _order_view(h: dict | None) -> dict | None:
    if not h:
        return None
    out = dict(h)
    for k in NUMERIC_FIELDS:
        out[k] = _float(h.get(k))
    si = h.get("slice_idx")
    if not _empty(si):
        try:
            out["slice_idx"] = int(float(si))
        except (TypeError, ValueError):
            pass
    return out


def emit_order(r, merged: dict) -> None:
    try:
        m = merged or {}
        view = _order_view(m) or {}
        runtime.publish(r, runtime.STREAM_OMS_EVENTS, {
            "type": "order", "ts": runtime.now_iso(), "account": m.get("account"),
            "source": m.get("source"), "algo_id": m.get("algo_id"),
            "slice_idx": view.get("slice_idx"), "order": view,
        })
    except Exception as e:
        log.error("emit_order failed: %s", e)


def emit_command(r, record: dict) -> None:
    try:
        runtime.publish(r, runtime.STREAM_OMS_EVENTS,
                        {"type": "command", "ts": runtime.now_iso(), "command": record})
    except Exception as e:
        log.error("emit_command failed: %s", e)


def get_order_state(r, order_id=None, client_order_id=None) -> dict | None:
    try:
        if not order_id and client_order_id:
            order_id = runtime.redis_call(r.get, runtime.coid_key(client_order_id))
        if not order_id:
            return None
        h = runtime.redis_call(r.hgetall, runtime.order_key(order_id))
        return _order_view(h) if h else None
    except Exception as e:
        log.warning("get_order_state failed: %s", e)
        return None


# --------------------------------------------------------------------------- forwarding
def forward_to_ems(r, payload: dict, timeout: float = 10.0) -> dict:
    p = dict(payload)
    p["cmd_id"] = runtime.new_id()
    return runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, p, timeout)


def forward_to_algo(r, payload: dict, timeout: float = 10.0) -> dict:
    p = dict(payload)
    p["cmd_id"] = runtime.new_id()
    return runtime.send_command(r, runtime.STREAM_ALGO_REQUESTS, p, timeout)


# --------------------------------------------------------------------------- trade updates
def handle_payload(r, stream_id: str, payload: dict) -> dict | None:
    try:
        if not isinstance(payload, dict):
            return None
        order = payload.get("order")
        if not isinstance(order, dict) or not (order.get("id") or order.get("order_id")):
            return None
        merged = apply_order(r, order, account=payload.get("account", "paper"),
                             source=payload.get("source"), algo_id=payload.get("algo_id"),
                             slice_idx=payload.get("slice_idx"), event_ts=payload.get("ts"))
        return merged or None
    except Exception as e:
        log.error("handle_payload %s failed: %s", stream_id, e)
        return None


# --------------------------------------------------------------------------- commands
def _order_reply(ok, status, *, order_id=None, client_order_id=None, order=None, error=None) -> dict:
    return {"ok": bool(ok), "status": status, "order_id": order_id, "client_order_id": client_order_id,
            "order": order, "error": error}


def _algo_reply(ok, status, *, algo_id=None, algo=None, error=None) -> dict:
    return {"ok": bool(ok), "status": status, "algo_id": algo_id, "algo": algo, "error": error}


def _cmd_submit(r, p: dict, account: str, ctx: dict) -> dict:
    coid = p.get("client_order_id")
    ctx["client_order_id"] = coid
    side = str(p.get("side") or "").lower()
    typ = str(p.get("type") or "").lower()
    tif = str(p.get("tif") or p.get("time_in_force") or "gtc").lower()
    qty = _float(p.get("qty"))
    lp = _float(p.get("limit_price"))
    symbol = p.get("symbol")
    err = None
    if side not in ("buy", "sell"):
        err = "side must be buy or sell"
    elif typ not in ("market", "limit"):
        err = "type must be market or limit"
    elif qty is None or qty <= 0:
        err = "qty must be > 0"
    elif typ == "limit" and (lp is None or lp <= 0):
        err = "limit order requires limit_price > 0"
    elif tif not in ("gtc", "ioc"):
        err = "tif must be gtc or ioc"
    elif not isinstance(coid, str) or not COID_RE.match(coid):
        err = "client_order_id must match ^[A-Za-z0-9_-]{1,48}$"
    elif not symbol:
        err = "symbol required"
    if err:
        return _order_reply(False, "rejected", client_order_id=coid, error=err)

    existing_id = runtime.redis_call(r.get, runtime.coid_key(coid))
    if existing_id:
        h = runtime.redis_call(r.hgetall, runtime.order_key(existing_id))
        if h:
            ctx["order_id"] = existing_id
            return _order_reply(True, "already_submitted", order_id=existing_id, client_order_id=coid,
                                order=_order_view(h))

    if runtime.worker_alive(r, "ems") is None:
        return _order_reply(False, "error", client_order_id=coid, error="EMS not running")

    source, algo_id, slice_idx = p.get("source"), p.get("algo_id"), p.get("slice_idx")
    req = {"kind": "submit", "account": account, "client_order_id": coid, "symbol": str(symbol),
           "side": side, "type": typ, "qty": qty, "limit_price": lp if typ == "limit" else None,
           "tif": tif, "source": source, "algo_id": algo_id, "slice_idx": slice_idx}
    rep = forward_to_ems(r, req) or {}
    st = rep.get("status")
    if st in ("acked", "already_submitted"):
        order = rep.get("order")
        oid = rep.get("order_id")
        view = None
        if isinstance(order, dict) and (order.get("id") or order.get("order_id")):
            merged = apply_order(r, order, account=account, source=source, algo_id=algo_id,
                                 slice_idx=slice_idx)
            if merged:
                emit_order(r, merged)
                view = _order_view(merged)
                oid = merged.get("order_id") or oid
        ctx["order_id"] = oid
        return _order_reply(True, st, order_id=oid, client_order_id=rep.get("client_order_id") or coid,
                            order=view or order)
    if st == "timeout":
        return _order_reply(False, "unknown", client_order_id=coid, error="EMS did not reply")
    if st == "unknown":
        return _order_reply(False, "unknown", client_order_id=coid, error=rep.get("error"))
    if st in ("rejected", "error"):
        return _order_reply(False, st, client_order_id=coid, error=rep.get("error"))
    return _order_reply(bool(rep.get("ok")), st or "error", order_id=rep.get("order_id"),
                        client_order_id=coid, order=rep.get("order"),
                        error=rep.get("error") or (None if rep.get("ok") else "unexpected EMS reply"))


def _cmd_cancel(r, p: dict, account: str, ctx: dict) -> dict:
    order_id = p.get("order_id") or None
    coid = p.get("client_order_id") or None
    state = get_order_state(r, order_id=order_id, client_order_id=coid)
    if state:
        order_id = state.get("order_id") or order_id
        coid = state.get("client_order_id") or coid
    ctx["order_id"], ctx["client_order_id"] = order_id, coid
    if not order_id:
        return _order_reply(False, "not_found", client_order_id=coid, error="order not found")
    if runtime.worker_alive(r, "ems") is None:
        return _order_reply(False, "error", order_id=order_id, client_order_id=coid,
                            error="EMS not running")
    rep = forward_to_ems(r, {"kind": "cancel_order", "account": account, "order_id": str(order_id),
                             "client_order_id": coid, "source": p.get("source")}) or {}
    st = rep.get("status")
    if st == "cancel_requested":
        cur = runtime.redis_call(r.hgetall, runtime.order_key(order_id)) or {}
        view = None
        if cur:
            if cur.get("status") not in runtime.TERMINAL_STATUSES:
                upd = {"status": "pending_cancel", "updated_at": runtime.now_iso()}
                runtime.redis_call(runtime.hset_state, r, runtime.order_key(order_id), upd)
                cur.update(upd)
                emit_order(r, cur)
            view = _order_view(cur)
        return _order_reply(True, "cancel_requested", order_id=str(order_id), client_order_id=coid,
                            order=view)
    if st == "timeout":
        return _order_reply(False, "timeout", order_id=order_id, client_order_id=coid,
                            error="EMS did not reply")
    return _order_reply(bool(rep.get("ok")), st or "error", order_id=rep.get("order_id") or order_id,
                        client_order_id=coid, error=rep.get("error"))


def _relay_algo(rep: dict, ctx: dict, fallback_algo_id=None) -> dict:
    rep = rep or {}
    st = rep.get("status")
    algo_id = rep.get("algo_id") or fallback_algo_id
    ctx["algo_id"] = algo_id
    if st == "timeout":
        return _algo_reply(False, "timeout", algo_id=algo_id, error="algo_engine did not reply")
    return _algo_reply(bool(rep.get("ok")), st or "error", algo_id=algo_id, algo=rep.get("algo"),
                       error=rep.get("error"))


def _cmd_algo_create(r, p: dict, account: str, ctx: dict) -> dict:
    strategy = p.get("strategy")
    side = str(p.get("side") or "").lower()
    total_qty = _float(p.get("total_qty"))
    if not isinstance(strategy, str) or not strategy:
        return _algo_reply(False, "rejected", error="strategy required")
    if side not in ("buy", "sell"):
        return _algo_reply(False, "rejected", error="side must be buy or sell")
    if total_qty is None or total_qty <= 0:
        return _algo_reply(False, "rejected", error="total_qty must be > 0")
    if not p.get("symbol"):
        return _algo_reply(False, "rejected", error="symbol required")
    if runtime.worker_alive(r, "algo_engine") is None:
        return _algo_reply(False, "error", error="algo_engine not running")
    req = {"kind": "create", "strategy": strategy, "account": account, "symbol": p.get("symbol"),
           "side": side, "total_qty": total_qty, "duration_s": p.get("duration_s"),
           "n_slices": p.get("n_slices"), "limit_cap": p.get("limit_cap"),
           "params": p.get("params") or {}, "source": p.get("source")}
    return _relay_algo(forward_to_algo(r, req), ctx)


def _cmd_algo_cancel(r, p: dict, account: str, ctx: dict) -> dict:
    algo_id = p.get("algo_id")
    ctx["algo_id"] = algo_id
    if not algo_id:
        return _algo_reply(False, "rejected", error="algo_id required")
    if runtime.worker_alive(r, "algo_engine") is None:
        return _algo_reply(False, "error", algo_id=algo_id, error="algo_engine not running")
    rep = forward_to_algo(r, {"kind": "cancel", "algo_id": algo_id, "source": p.get("source")})
    return _relay_algo(rep, ctx, fallback_algo_id=algo_id)


def handle_command(r, payload: dict, stream_id: str | None = None) -> dict:
    payload = payload if isinstance(payload, dict) else {}
    cmd_id = str(payload.get("cmd_id") or payload.get("req_id") or runtime.new_id())

    try:
        stored = runtime.redis_call(r.get, runtime.cmd_key(cmd_id))
    except Exception as e:
        log.warning("cmd replay lookup failed: %s", e)
        stored = None
    if stored:
        try:
            reply = json.loads(stored)
            if isinstance(reply, dict):
                reply["cmd_id"] = cmd_id
                runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
                log.info("cmd %s replayed -> %s", cmd_id, reply.get("status"))
                return reply
        except Exception as e:
            log.warning("stored reply for %s unusable: %s", cmd_id, e)

    received_at = runtime.now_iso()
    kind = payload.get("kind")
    raw_account = payload.get("account")
    account = "paper" if raw_account in (None, "") else str(raw_account).lower()
    ctx = {"order_id": payload.get("order_id"), "client_order_id": payload.get("client_order_id"),
           "algo_id": payload.get("algo_id"), "slice_idx": payload.get("slice_idx")}
    reply = None
    try:
        if kind not in KINDS:
            reply = {"ok": False, "status": "error", "error": f"unknown kind {kind!r}"}
        elif account not in ("paper", "live"):
            reply = {"ok": False, "status": "rejected", "error": "account must be paper or live"}
        elif kind == "submit":
            reply = _cmd_submit(r, payload, account, ctx)
        elif kind == "cancel":
            reply = _cmd_cancel(r, payload, account, ctx)
        elif kind == "algo_create":
            reply = _cmd_algo_create(r, payload, account, ctx)
        else:
            reply = _cmd_algo_cancel(r, payload, account, ctx)
    except Exception as e:
        log.exception("command %s (%s) failed", cmd_id, kind)
        reply = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
    if not isinstance(reply, dict):
        reply = {"ok": False, "status": "error", "error": "no reply"}
    reply["cmd_id"] = cmd_id
    if kind in ("algo_create", "algo_cancel"):
        reply.setdefault("algo_id", ctx.get("algo_id"))
        reply.setdefault("algo", None)
    else:
        reply.setdefault("order_id", ctx.get("order_id"))
        reply.setdefault("client_order_id", ctx.get("client_order_id"))
        reply.setdefault("order", None)
    reply.setdefault("error", None)

    record = {
        "cmd_id": cmd_id, "kind": kind, "account": account,
        "client_order_id": reply.get("client_order_id") or ctx.get("client_order_id"),
        "order_id": reply.get("order_id") or ctx.get("order_id"),
        "algo_id": reply.get("algo_id") or ctx.get("algo_id"),
        "slice_idx": ctx.get("slice_idx"), "source": payload.get("source"),
        "payload": payload, "status": reply.get("status"), "error": reply.get("error"),
        "received_at": received_at, "completed_at": runtime.now_iso(),
    }
    emit_command(r, record)
    try:
        runtime.redis_call(r.set, runtime.cmd_key(cmd_id), json.dumps(reply, default=str),
                           ex=runtime.STATE_TTL_S)
    except Exception as e:
        log.error("storing cmd %s failed: %s", cmd_id, e)
    try:
        runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
    except Exception as e:
        log.error("send_reply %s failed: %s", cmd_id, e)
    log.info("cmd %s kind=%s source=%s coid=%s -> %s%s", cmd_id, kind, payload.get("source"),
             record["client_order_id"], reply.get("status"),
             f" ({reply.get('error')})" if reply.get("error") else "")
    return reply


# --------------------------------------------------------------------------- reconciliation
def reconcile(r, accounts=("paper",), since_iso=None) -> int:
    since = since_iso or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    count = 0
    try:
        for account in accounts:
            live = account == "live"
            orders = list(trader.open_orders(live=live) or [])
            orders += list(trader.list_orders("closed", after=since, live=live) or [])
            for o in orders:
                if not isinstance(o, dict):
                    continue
                oid = o.get("id") or o.get("order_id")
                if not oid:
                    continue
                old = runtime.redis_call(r.hgetall, runtime.order_key(oid)) or {}
                merged = apply_order(r, o, account=account)
                if merged and (not old or changed(old, merged)):
                    emit_order(r, merged)
                    count += 1
        runtime.redis_call(r.set, LAST_RECONCILE_KEY, runtime.now_iso())
        if count:
            log.info("reconcile updated %d orders", count)
        return count
    except Exception as e:
        log.warning("reconcile failed: %s: %s", type(e).__name__, e)
        return 0


def _reconcile_accounts():
    accts = ["paper"]
    if os.environ.get("OMS_RECONCILE_LIVE") == "1":
        accts.append("live")
    return tuple(accts)


# --------------------------------------------------------------------------- loops
def process_updates(r, reader) -> int:
    entries = reader.read()
    n = 0
    for entry_id, fields in entries:
        try:
            payload = json.loads(fields.get("json") or "{}")
            if isinstance(payload, dict):
                handle_payload(r, entry_id, payload)
            reader.ack(entry_id)
        except Exception as e:
            log.exception("trade update %s failed", entry_id)
            try:
                reader.dead_letter(entry_id, fields, f"{type(e).__name__}: {e}")
            except Exception as e2:
                log.error("dead_letter %s failed: %s", entry_id, e2)
        n += 1
    return n


def process_commands(r, reader) -> int:
    entries = reader.read()
    n = 0
    for entry_id, fields in entries:
        payload = None
        try:
            payload = json.loads(fields.get("json") or "{}")
            if not isinstance(payload, dict):
                raise ValueError("payload is not an object")
            handle_command(r, payload, entry_id)
            reader.ack(entry_id)
        except Exception as e:
            log.exception("command entry %s failed", entry_id)
            try:
                reader.dead_letter(entry_id, fields, f"{type(e).__name__}: {e}")
            except Exception as e2:
                log.error("dead_letter %s failed: %s", entry_id, e2)
            cmd_id = payload.get("cmd_id") if isinstance(payload, dict) else None
            if cmd_id:
                try:
                    runtime.send_reply(r, cmd_id, {"ok": False, "status": "error", "cmd_id": cmd_id,
                                                   "error": f"{type(e).__name__}: {e}"})
                except Exception:
                    pass
        n += 1
    return n


def body():
    r = runtime.get_redis()
    consumer = os.environ.get("OMS_CONSUMER", "oms-1")
    interval = float(os.environ.get("OMS_RECONCILE_S", "30") or 30)
    cmd_reader = runtime.GroupReader(r, runtime.STREAM_OMS_COMMANDS, "oms", consumer, block_ms=500)
    upd_reader = runtime.GroupReader(r, runtime.STREAM_TRADE_UPDATES, "oms", consumer, block_ms=500)
    log.info("oms started consumer=%s reconcile_every=%ss", consumer, interval)
    accounts = _reconcile_accounts()
    reconcile(r, accounts)
    last = time.monotonic()
    while True:
        nc = process_commands(r, cmd_reader)
        nu = process_updates(r, upd_reader)
        if time.monotonic() - last >= interval:
            reconcile(r, accounts)
            last = time.monotonic()
        if not nc and not nu:
            time.sleep(0.05)


def main():
    runtime.run_forever(body, "oms")


if __name__ == "__main__":
    main()
