"""Order Management System worker: the FRONT DOOR for every order intent and owner of the
order LIFECYCLE.

- Consumes alpaca:oms:commands (submit / cancel / algo_create / algo_cancel): validate, replay
  check (cmd_id), dedupe (client_order_id), forward to crossing.py (alpaca:cross:requests) or the
  algo engine (alpaca:algo:requests), relay the reply.
- Consumes alpaca:trade_updates (exchange events, EMS execution reports, crossing internal_*
  events) and keeps live order hashes in Redis.
- Reconciles with Alpaca REST (read-only) periodically.

Never calls Alpaca order endpoints, never touches SQLite. Durable history is written by
persister.py from the events emitted on alpaca:oms:events.
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
ALGO_COID_RE = re.compile(r"^algo-([A-Za-z0-9]{1,12})-(\d+)$")
KINDS = ("submit", "cancel", "algo_create", "algo_cancel")
LAST_RECONCILE_KEY = "alpaca:oms:last_reconcile"
NUMERIC_FIELDS = ("qty", "limit_price", "filled_qty", "filled_avg_price")

_spill = runtime.SpillBuffer()


# --------------------------------------------------------------------------- helpers
def _f(x, default=None):
    if x is None or x == "" or x == "None":
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _s(x):
    if x is None or x == "" or x == "None":
        return None
    return str(x)


def _int(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def _derive_algo(coid):
    if not coid:
        return None, None
    m = ALGO_COID_RE.match(str(coid))
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


def _convert(h: dict) -> dict:
    """Redis hash (strings) -> dict with numerics as float/None."""
    out = {}
    for k, v in (h or {}).items():
        if k in NUMERIC_FIELDS:
            out[k] = _f(v)
        elif k == "slice_idx":
            out[k] = _int(v)
        else:
            out[k] = _s(v)
    return out


def changed(old: dict, merged: dict) -> bool:
    old = old or {}
    merged = merged or {}
    if _s(old.get("status")) != _s(merged.get("status")):
        return True
    for k in ("filled_qty", "filled_avg_price", "limit_price"):
        if _f(old.get(k)) != _f(merged.get(k)):
            return True
    return False


# --------------------------------------------------------------------------- live state
def apply_order(r, order: dict, *, account, source=None, algo_id=None, slice_idx=None,
                book=None, event_ts=None) -> dict:
    order = order or {}
    new = runtime.normalize_order(order)
    order_id = new.get("order_id")
    if not order_id:
        raise ValueError("order has no id")
    okey = runtime.order_key(order_id)
    old = runtime.redis_call(r.hgetall, okey) or {}

    old_status = _s(old.get("status"))
    new_status = new.get("status")
    if old_status in runtime.TERMINAL_STATUSES and new_status not in runtime.TERMINAL_STATUSES:
        status = old_status
    else:
        status = new_status or old_status

    fq_old, fq_new = _f(old.get("filled_qty")), new.get("filled_qty")
    if fq_old is None:
        filled_qty = fq_new
    elif fq_new is None:
        filled_qty = fq_old
    else:
        filled_qty = max(fq_old, fq_new)

    merged = {k: (_s(v) if not isinstance(v, (int, float)) else v) for k, v in old.items()}
    for k, v in new.items():
        if v is not None:
            merged[k] = v
    merged["order_id"] = order_id
    merged["status"] = status
    merged["filled_qty"] = filled_qty
    for k in ("filled_avg_price", "limit_price", "qty"):
        merged[k] = new.get(k) if new.get(k) is not None else _f(old.get(k))

    coid = new.get("client_order_id") or _s(old.get("client_order_id"))
    d_algo, d_idx = _derive_algo(coid)

    def keep(field, given, derived=None):
        ov = _s(old.get(field))
        if ov is not None:
            return ov
        if given not in (None, ""):
            return given
        ov2 = order.get(field)
        if ov2 not in (None, ""):
            return ov2
        return derived

    merged["source"] = keep("source", source)
    merged["algo_id"] = keep("algo_id", algo_id, d_algo)
    merged["slice_idx"] = keep("slice_idx", slice_idx, d_idx)
    merged["book"] = keep("book", book, runtime.DEFAULT_BOOK)

    venue = order.get("venue") or _s(old.get("venue"))
    if not venue:
        venue = "internal" if str(order_id).startswith("int-") else "alpaca"
    merged["venue"] = venue
    for extra in ("routed_order_id", "error"):
        if order.get(extra) not in (None, ""):
            merged[extra] = order.get(extra)
    merged["account"] = account or _s(old.get("account")) or "paper"
    merged["last_event_ts"] = event_ts or runtime.now_iso()
    merged["updated_at"] = runtime.now_iso()

    terminal = status in runtime.TERMINAL_STATUSES
    ttl = runtime.STATE_TTL_S if terminal else None
    runtime.redis_call(runtime.hset_state, r, okey, merged, ttl=ttl)

    if coid:
        ckey = runtime.coid_key(coid)
        is_internal = str(order_id).startswith("int-")
        write = True
        if is_internal:
            cur = runtime.redis_call(r.get, ckey)
            if cur and not str(cur).startswith("int-") and cur != order_id:
                write = False
        if write:
            if terminal:
                runtime.redis_call(r.set, ckey, order_id, ex=runtime.STATE_TTL_S)
            else:
                runtime.redis_call(r.set, ckey, order_id)
    if terminal:
        runtime.redis_call(r.srem, runtime.SET_OPEN_ORDERS, order_id)
    else:
        runtime.redis_call(r.sadd, runtime.SET_OPEN_ORDERS, order_id)

    return {k: runtime._sval(v) for k, v in merged.items() if v is not None}


def emit_order(r, merged: dict) -> None:
    m = merged or {}
    runtime.publish(r, runtime.STREAM_OMS_EVENTS, {
        "type": "order", "ts": runtime.now_iso(), "account": m.get("account"),
        "source": _s(m.get("source")), "algo_id": _s(m.get("algo_id")),
        "slice_idx": _int(m.get("slice_idx")), "order": _convert(m),
    }, buffer=_spill)


def emit_command(r, record: dict) -> None:
    runtime.publish(r, runtime.STREAM_OMS_EVENTS,
                    {"type": "command", "ts": runtime.now_iso(), "command": record}, buffer=_spill)


def get_order_state(r, order_id=None, client_order_id=None) -> dict | None:
    oid = order_id
    if not oid and client_order_id:
        oid = runtime.redis_call(r.get, runtime.coid_key(client_order_id))
    if not oid:
        return None
    h = runtime.redis_call(r.hgetall, runtime.order_key(oid)) or {}
    if not h:
        return None
    d = _convert(h)
    d.setdefault("order_id", str(oid))
    return d


# --------------------------------------------------------------------------- forwarding
def forward_to_cross(r, payload: dict, timeout: float = 10.0) -> dict:
    p = {k: v for k, v in dict(payload or {}).items() if v is not None}
    p["cmd_id"] = runtime.new_id()
    return runtime.send_command(r, runtime.STREAM_CROSS_REQUESTS, p, timeout)


def forward_to_algo(r, payload: dict, timeout: float = 10.0) -> dict:
    p = {k: v for k, v in dict(payload or {}).items() if v is not None}
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
        return apply_order(r, order, account=payload.get("account") or "paper",
                           source=payload.get("source"), algo_id=payload.get("algo_id"),
                           slice_idx=payload.get("slice_idx"), book=payload.get("book"),
                           event_ts=payload.get("ts"))
    except Exception:
        log.exception("handle_payload failed for %s", stream_id)
        return None


# --------------------------------------------------------------------------- commands
def _submit(r, payload: dict, cmd_id: str, account: str) -> dict:
    coid = payload.get("client_order_id")
    base = {"cmd_id": cmd_id, "order_id": None, "client_order_id": coid, "order": None}
    side = str(payload.get("side") or "").lower()
    otype = str(payload.get("type") or "market").lower()
    tif = str(payload.get("tif") or payload.get("time_in_force") or "gtc").lower()
    qty = _f(payload.get("qty"))
    lim = _f(payload.get("limit_price"))
    symbol = payload.get("symbol")
    err = None
    if side not in ("buy", "sell"):
        err = "side must be buy or sell"
    elif otype not in ("market", "limit"):
        err = "type must be market or limit"
    elif qty is None or qty <= 0:
        err = "qty must be > 0"
    elif otype == "limit" and (lim is None or lim <= 0):
        err = "limit order requires limit_price > 0"
    elif tif not in ("gtc", "ioc"):
        err = "tif must be gtc or ioc"
    elif not isinstance(coid, str) or not COID_RE.match(coid):
        err = "client_order_id must match ^[A-Za-z0-9_-]{1,48}$"
    elif not symbol or not isinstance(symbol, str):
        err = "symbol required"
    if err:
        return {**base, "ok": False, "status": "rejected", "error": err}
    if otype == "market":
        lim = None
    book = payload.get("book") or runtime.DEFAULT_BOOK
    source = payload.get("source")
    algo_id = payload.get("algo_id")
    slice_idx = payload.get("slice_idx")

    existing = get_order_state(r, client_order_id=coid)
    if existing:
        return {**base, "ok": True, "status": "already_submitted",
                "order_id": existing.get("order_id"), "order": existing, "error": None}

    if runtime.worker_alive(r, "crossing") is None:
        return {**base, "ok": False, "status": "error", "error": "crossing not running"}

    fwd = {"kind": "submit", "account": account, "client_order_id": coid, "symbol": symbol,
           "side": side, "type": otype, "qty": qty, "limit_price": lim, "tif": tif,
           "source": source, "book": book, "cross": payload.get("cross"),
           "hold_s": payload.get("hold_s"), "algo_id": algo_id, "slice_idx": slice_idx}
    rep = forward_to_cross(r, {k: v for k, v in fwd.items() if v is not None})
    rep = rep if isinstance(rep, dict) else {"ok": False, "status": "unknown", "error": "bad reply"}
    st = rep.get("status")
    extras = {k: rep.get(k) for k in ("venue", "crossed_qty", "remaining", "crosses",
                                       "routed_order_id") if k in rep}
    if st in ("acked", "already_submitted"):
        order_out = rep.get("order")
        oid = rep.get("order_id")
        if isinstance(order_out, dict) and (order_out.get("id") or order_out.get("order_id")):
            merged = apply_order(r, order_out, account=account, source=source, algo_id=algo_id,
                                 slice_idx=slice_idx, book=book)
            emit_order(r, merged)
            order_out = _convert(merged)
            oid = order_out.get("order_id") or oid
        return {**base, **extras, "ok": bool(rep.get("ok", True)), "status": st,
                "order_id": oid, "order": order_out, "error": rep.get("error")}
    if st == "timeout":
        return {**base, "ok": False, "status": "unknown", "error": "crossing did not reply"}
    if st in ("rejected", "error"):
        return {**base, **extras, "ok": False, "status": st, "order_id": rep.get("order_id"),
                "order": rep.get("order"), "error": rep.get("error") or st}
    return {**base, **extras, "ok": False, "status": "unknown", "order_id": rep.get("order_id"),
            "order": rep.get("order"), "error": rep.get("error") or f"crossing status {st}"}


def _cancel(r, payload: dict, cmd_id: str, account: str) -> dict:
    order_id = _s(payload.get("order_id"))
    coid = _s(payload.get("client_order_id"))
    state = None
    if coid:
        state = get_order_state(r, client_order_id=coid)
    if state is None and order_id:
        state = get_order_state(r, order_id=order_id)
    if state:
        coid = coid or state.get("client_order_id")
        order_id = order_id or state.get("order_id")
    base = {"cmd_id": cmd_id, "order_id": order_id, "client_order_id": coid, "order": state}
    if not state and not order_id and not coid:
        return {**base, "ok": False, "status": "not_found", "error": "order_id or client_order_id required"}
    if runtime.worker_alive(r, "crossing") is None:
        return {**base, "ok": False, "status": "error", "error": "crossing not running"}
    fwd = {"kind": "cancel_order", "account": (state or {}).get("account") or account,
           "order_id": order_id, "client_order_id": coid, "source": payload.get("source")}
    rep = forward_to_cross(r, fwd)
    rep = rep if isinstance(rep, dict) else {"ok": False, "status": "error", "error": "bad reply"}
    st = rep.get("status")
    if st == "timeout":
        return {**base, "ok": False, "status": "unknown", "error": "crossing did not reply"}
    out = {**base, "ok": bool(rep.get("ok")), "status": st,
           "order_id": rep.get("order_id") or order_id,
           "client_order_id": rep.get("client_order_id") or coid, "error": rep.get("error")}
    if st == "cancel_requested":
        targets = []
        for oid in (out["order_id"], order_id, (state or {}).get("order_id")):
            if oid and oid not in targets:
                targets.append(oid)
        for oid in targets:
            try:
                okey = runtime.order_key(oid)
                h = runtime.redis_call(r.hgetall, okey) or {}
                if h and h.get("status") not in runtime.TERMINAL_STATUSES:
                    runtime.redis_call(r.hset, okey, mapping={"status": "pending_cancel",
                                                              "updated_at": runtime.now_iso()})
                    h["status"] = "pending_cancel"
                    emit_order(r, h)
                    out["order"] = _convert(h)
            except Exception:
                log.exception("pending_cancel update failed for %s", oid)
    return out


def _algo_reply(rep: dict, cmd_id: str, algo_id=None, timeout_err="algo_engine did not reply") -> dict:
    rep = rep if isinstance(rep, dict) else {"ok": False, "status": "error", "error": "bad reply"}
    if rep.get("status") == "timeout":
        return {"ok": False, "status": "unknown", "cmd_id": cmd_id, "algo_id": algo_id,
                "algo": None, "error": timeout_err}
    return {"ok": bool(rep.get("ok")), "status": rep.get("status"), "cmd_id": cmd_id,
            "algo_id": rep.get("algo_id") or algo_id, "algo": rep.get("algo"),
            "error": rep.get("error")}


def _algo_create(r, payload: dict, cmd_id: str, account: str) -> dict:
    base = {"cmd_id": cmd_id, "algo_id": None, "algo": None}
    strategy = payload.get("strategy")
    side = str(payload.get("side") or "").lower()
    total = _f(payload.get("total_qty"))
    if not isinstance(strategy, str) or not strategy:
        return {**base, "ok": False, "status": "rejected", "error": "strategy required"}
    if side not in ("buy", "sell"):
        return {**base, "ok": False, "status": "rejected", "error": "side must be buy or sell"}
    if total is None or total <= 0:
        return {**base, "ok": False, "status": "rejected", "error": "total_qty must be > 0"}
    if runtime.worker_alive(r, "algo_engine") is None:
        return {**base, "ok": False, "status": "error", "error": "algo_engine not running"}
    fwd = {"kind": "create", "strategy": strategy, "account": account,
           "book": payload.get("book") or runtime.DEFAULT_BOOK,
           "symbol": payload.get("symbol"), "side": side, "total_qty": total,
           "duration_s": payload.get("duration_s"), "n_slices": payload.get("n_slices"),
           "limit_cap": payload.get("limit_cap"), "params": payload.get("params"),
           "source": payload.get("source")}
    return _algo_reply(forward_to_algo(r, fwd), cmd_id)


def _algo_cancel(r, payload: dict, cmd_id: str) -> dict:
    algo_id = payload.get("algo_id")
    if not algo_id:
        return {"ok": False, "status": "rejected", "cmd_id": cmd_id, "algo_id": None,
                "algo": None, "error": "algo_id required"}
    if runtime.worker_alive(r, "algo_engine") is None:
        return {"ok": False, "status": "error", "cmd_id": cmd_id, "algo_id": algo_id,
                "algo": None, "error": "algo_engine not running"}
    fwd = {"kind": "cancel", "algo_id": algo_id, "source": payload.get("source")}
    return _algo_reply(forward_to_algo(r, fwd), cmd_id, algo_id)


def handle_command(r, payload: dict, stream_id: str | None = None) -> dict:
    payload = payload if isinstance(payload, dict) else {}
    cmd_id = payload.get("cmd_id") or payload.get("req_id") or runtime.new_id()
    try:
        stored = runtime.redis_call(r.get, runtime.cmd_key(cmd_id))
    except Exception:
        log.exception("replay lookup failed for %s", cmd_id)
        stored = None
    if stored:
        try:
            rep = json.loads(stored)
            if isinstance(rep, dict):
                runtime.send_reply(r, cmd_id, rep)
                log.info("REPLAY cmd_id=%s status=%s", cmd_id, rep.get("status"))
                return rep
        except (ValueError, TypeError):
            pass

    received_at = runtime.now_iso()
    kind = payload.get("kind")
    account = str(payload.get("account") or "paper").lower()
    reply: dict
    try:
        if kind not in KINDS:
            reply = {"ok": False, "status": "error", "cmd_id": cmd_id,
                     "error": f"unknown kind {kind!r}"}
        elif account not in ("paper", "live"):
            reply = {"ok": False, "status": "rejected", "cmd_id": cmd_id,
                     "error": "account must be paper or live"}
        elif kind == "submit":
            reply = _submit(r, payload, cmd_id, account)
        elif kind == "cancel":
            reply = _cancel(r, payload, cmd_id, account)
        elif kind == "algo_create":
            reply = _algo_create(r, payload, cmd_id, account)
        else:
            reply = _algo_cancel(r, payload, cmd_id)
    except Exception as e:
        log.exception("command %s (%s) failed", cmd_id, kind)
        reply = {"ok": False, "status": "error", "cmd_id": cmd_id,
                 "error": f"{type(e).__name__}: {e}"}
    reply["cmd_id"] = cmd_id

    record = {
        "cmd_id": cmd_id, "kind": kind, "account": account,
        "client_order_id": reply.get("client_order_id") or payload.get("client_order_id"),
        "order_id": reply.get("order_id") or payload.get("order_id"),
        "algo_id": reply.get("algo_id") or payload.get("algo_id"),
        "slice_idx": payload.get("slice_idx"), "source": payload.get("source"),
        "payload": payload, "status": reply.get("status"), "error": reply.get("error"),
        "received_at": received_at, "completed_at": runtime.now_iso(),
    }
    try:
        emit_command(r, record)
    except Exception:
        log.exception("emit_command failed for %s", cmd_id)
    try:
        runtime.redis_call(r.set, runtime.cmd_key(cmd_id), json.dumps(reply, default=str),
                           ex=runtime.STATE_TTL_S)
    except Exception:
        log.exception("storing reply failed for %s", cmd_id)
    try:
        runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
    except Exception:
        log.exception("send_reply failed for %s", cmd_id)
    log.info("CMD %s kind=%s source=%s status=%s coid=%s order_id=%s algo_id=%s err=%s",
             cmd_id, kind, payload.get("source"), reply.get("status"),
             record["client_order_id"], record["order_id"], record["algo_id"], reply.get("error"))
    return reply


# --------------------------------------------------------------------------- reconcile
def _reconcile_accounts():
    accts = ["paper"]
    if str(os.environ.get("OMS_RECONCILE_LIVE", "0")).strip() == "1":
        accts.append("live")
    return tuple(accts)


def reconcile(r, accounts=("paper",), since_iso=None) -> int:
    count = 0
    try:
        for acct in accounts:
            live = acct == "live"
            after = since_iso or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            orders = list(trader.open_orders(live) or [])
            orders += list(trader.list_orders("closed", after=after, live=live) or [])
            for o in orders:
                try:
                    if not isinstance(o, dict):
                        continue
                    oid = o.get("id") or o.get("order_id")
                    if not oid:
                        continue
                    old = runtime.redis_call(r.hgetall, runtime.order_key(oid)) or {}
                    merged = apply_order(r, o, account=acct)
                    if not old or changed(old, merged):
                        emit_order(r, merged)
                        count += 1
                except Exception:
                    log.exception("reconcile of order failed")
        runtime.redis_call(r.set, LAST_RECONCILE_KEY, runtime.now_iso())
        if count:
            log.info("RECONCILE updated %d order(s)", count)
        return count
    except Exception as e:
        log.warning("reconcile failed: %s: %s", type(e).__name__, e)
        return 0


# --------------------------------------------------------------------------- loops
def _parse_entry(fields: dict) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        return dict(fields or {})
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("payload is not an object")
    return d


def process_updates(r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        try:
            payload = _parse_entry(fields)
            handle_payload(r, eid, payload)
            reader.ack(eid)
        except Exception as e:
            log.exception("update %s failed", eid)
            try:
                reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
            except Exception:
                log.exception("dead_letter failed for %s", eid)
    return len(entries)


def process_commands(r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        payload = None
        try:
            payload = _parse_entry(fields)
            handle_command(r, payload, eid)
            reader.ack(eid)
        except Exception as e:
            log.exception("command entry %s failed", eid)
            try:
                reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
            except Exception:
                log.exception("dead_letter failed for %s", eid)
            cmd_id = (payload or {}).get("cmd_id") if isinstance(payload, dict) else None
            if cmd_id:
                try:
                    runtime.send_reply(r, cmd_id, {"ok": False, "status": "error", "cmd_id": cmd_id,
                                                   "error": f"{type(e).__name__}: {e}"})
                except Exception:
                    pass
    return len(entries)


def _reconcile_interval() -> float:
    try:
        return float(os.environ.get("OMS_RECONCILE_S", "30"))
    except ValueError:
        return 30.0


def body() -> None:
    r = runtime.get_redis()
    consumer = os.environ.get("OMS_CONSUMER", "oms-1")
    cmd_reader = runtime.GroupReader(r, runtime.STREAM_OMS_COMMANDS, "oms", consumer, block_ms=500)
    upd_reader = runtime.GroupReader(r, runtime.STREAM_TRADE_UPDATES, "oms", consumer, block_ms=500)
    interval = _reconcile_interval()
    accounts = _reconcile_accounts()
    log.info("oms started consumer=%s reconcile_s=%s accounts=%s", consumer, interval, accounts)
    reconcile(r, accounts)
    last = time.monotonic()
    while True:
        n_cmd = process_commands(r, cmd_reader)
        n_upd = process_updates(r, upd_reader)
        if time.monotonic() - last >= interval:
            last = time.monotonic()
            reconcile(r, accounts)
        if not n_cmd and not n_upd:
            time.sleep(0.05)


def main() -> None:
    runtime.run_forever(body, "oms")


if __name__ == "__main__":
    main()
