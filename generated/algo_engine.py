"""Algo engine worker: runs execution algorithms (TWAP; VWAP/IS stubs) as a standalone service.

Serves ``create`` / ``cancel`` requests forwarded by OMS on ``alpaca:algo:requests`` (group
``algo_engine``), keeps live algo state in Redis (``alpaca:algo:<id>``, ``alpaca:algo:<id>:slices``,
``alpaca:algos:active``), submits every child THROUGH OMS (``alpaca:oms:commands``, source "algo"),
tracks children via OMS's live order hashes and emits snapshots to ``alpaca:algo:events``.
Never calls Alpaca order endpoints; never writes SQLite (one read-only cold-start reload).
No I/O at import time.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import algos
import runtime
import trader

log = runtime.log("algo_engine")

ALGO_FIELDS = ("algo_id", "strategy", "account", "book", "symbol", "side", "total_qty", "n_slices", "start_ts",
               "end_ts", "limit_cap", "params_json", "source", "status", "filled_qty", "avg_px", "error",
               "created_at", "updated_at")
ALGO_FLOAT = ("total_qty", "start_ts", "end_ts", "limit_cap", "filled_qty", "avg_px")
SLICE_FLOAT = ("scheduled_ts", "qty", "limit_price", "filled_qty", "filled_avg_price", "submitted_ts")
ALGO_TERMINAL = frozenset({"completed", "partial", "cancelled", "failed"})
ALGO_ACTIVE = frozenset({"pending", "running"})
SLICE_TERMINAL = frozenset({"filled", "rolled", "cancelled", "failed"})
SLICE_LIVE = frozenset({"submitted", "working"})
ACK_TIMEOUT_S = 120.0
FINAL_GRACE_S = 5.0
EPS = 1e-9


# --------------------------------------------------------------------------- helpers
def _f(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ts_float(x):
    v = _f(x)
    if v is not None:
        return v
    if x is None or x == "":
        return None
    try:
        s = str(x).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _typed_algo(h: dict) -> dict:
    out = {}
    for k in ALGO_FIELDS:
        v = h.get(k)
        if v == "":
            v = None
        out[k] = v
    for k in ALGO_FLOAT:
        out[k] = _ts_float(out.get(k)) if k in ("start_ts", "end_ts") else _f(out.get(k))
    try:
        out["n_slices"] = int(float(out["n_slices"])) if out.get("n_slices") is not None else None
    except (TypeError, ValueError):
        out["n_slices"] = None
    if out.get("filled_qty") is None:
        out["filled_qty"] = 0.0
    if not out.get("book"):
        out["book"] = runtime.DEFAULT_BOOK
    return out


def _typed_slice(d: dict) -> dict:
    s = dict(d or {})
    for k in SLICE_FLOAT:
        if k in s:
            s[k] = _ts_float(s[k]) if k in ("scheduled_ts", "submitted_ts") else _f(s[k])
    try:
        s["slice_idx"] = int(float(s.get("slice_idx")))
    except (TypeError, ValueError):
        s["slice_idx"] = 0
    if s.get("filled_qty") is None:
        s["filled_qty"] = 0.0
    for k in ("order_id", "client_order_id", "error"):
        if s.get(k) in ("", "None"):
            s[k] = None
    return s


def _params(algo: dict) -> dict:
    try:
        p = json.loads(algo.get("params_json") or "{}")
        return p if isinstance(p, dict) else {}
    except (TypeError, ValueError):
        return {}


def _rules(r, symbol, account):
    try:
        return algos.rules_from_dict(trader.get_asset_rules(symbol, account == "live", r=r))
    except Exception as e:
        log.warning("asset rules for %s unavailable: %s", symbol, e)
        return algos.AssetRules()


# --------------------------------------------------------------------------- state
def load_algo(r, algo_id) -> dict | None:
    h = runtime.redis_call(r.hgetall, runtime.algo_key(algo_id))
    if not h:
        return None
    a = _typed_algo(h)
    a["algo_id"] = a.get("algo_id") or str(algo_id)
    return a


def load_slices(r, algo_id) -> list[dict]:
    h = runtime.redis_call(r.hgetall, runtime.algo_slices_key(algo_id)) or {}
    out = []
    for field, raw in h.items():
        try:
            d = json.loads(raw)
            if not isinstance(d, dict):
                continue
        except (TypeError, ValueError):
            continue
        d.setdefault("slice_idx", field)
        d.setdefault("algo_id", str(algo_id))
        out.append(_typed_slice(d))
    out.sort(key=lambda s: s["slice_idx"])
    return out


def _algo_view(algo: dict) -> dict:
    return {k: algo.get(k) for k in ALGO_FIELDS}


def save_algo(r, algo: dict, *, emit: bool = True, slices: list | None = None) -> None:
    algo["updated_at"] = runtime.now_iso()
    view = _algo_view(algo)
    aid = view["algo_id"]
    terminal = view.get("status") in ALGO_TERMINAL
    runtime.redis_call(runtime.hset_state, r, runtime.algo_key(aid), view,
                       ttl=runtime.STATE_TTL_S if terminal else None)
    if terminal:
        runtime.redis_call(r.expire, runtime.algo_slices_key(aid), runtime.STATE_TTL_S)
        runtime.redis_call(r.srem, runtime.SET_ACTIVE_ALGOS, aid)
    else:
        runtime.redis_call(r.sadd, runtime.SET_ACTIVE_ALGOS, aid)
    if emit:
        ev = {"type": "algo", "ts": runtime.now_iso(), "algo": view}
        if slices is not None:
            ev["slices"] = slices
        try:
            runtime.publish(r, runtime.STREAM_ALGO_EVENTS, ev)
        except Exception as e:
            log.error("publish algo event %s failed: %s", aid, e)


def _hset_slice(r, sl: dict) -> None:
    runtime.redis_call(r.hset, runtime.algo_slices_key(sl["algo_id"]), str(sl["slice_idx"]),
                       json.dumps(sl, default=str))


def save_slice(r, sl: dict, *, emit: bool = True) -> None:
    sl["updated_at"] = runtime.now_iso()
    _hset_slice(r, sl)
    if emit:
        try:
            runtime.publish(r, runtime.STREAM_ALGO_EVENTS, {"type": "slice", "ts": runtime.now_iso(),
                                                            "algo_id": sl["algo_id"], "slice": sl})
        except Exception as e:
            log.error("publish slice event failed: %s", e)


_INTERNAL_STATUS_MAP = {"accepted": "accepted", "partially_filled": "partially_filled",
                        "new": "accepted", "pending_new": "accepted",
                        "filled": "filled", "replaced": "accepted",
                        "canceled": "canceled", "rejected": "rejected", "expired": "expired"}


def child_state(r, client_order_id) -> dict | None:
    """Combined state of an algo child: internal (crossed) record + exchange record."""
    if not client_order_id:
        return None
    try:
        int_id = runtime.internal_order_id(client_order_id)
        ih = runtime.redis_call(r.hgetall, runtime.order_key(int_id)) or {}
        xh = {}
        xid = runtime.redis_call(r.get, runtime.coid_key(client_order_id))
        if xid and not str(xid).startswith("int-"):
            xh = runtime.redis_call(r.hgetall, runtime.order_key(xid)) or {}
        if not ih and not xh:
            return None
        ifq = _f(ih.get("filled_qty")) or 0.0 if ih else 0.0
        xfq = _f(xh.get("filled_qty")) or 0.0 if xh else 0.0
        ipx = _f(ih.get("filled_avg_price")) if ih else None
        xpx = _f(xh.get("filled_avg_price")) if xh else None
        notional = 0.0
        priced = 0.0
        if ifq > 0 and ipx is not None:
            notional += ifq * ipx
            priced += ifq
        if xfq > 0 and xpx is not None:
            notional += xfq * xpx
            priced += xfq
        avg = (notional / priced) if priced > 0 else None
        if xh:
            order_id = xh.get("order_id") or xid
            status = xh.get("status") or None
        else:
            order_id = ih.get("order_id") or int_id
            ist = ih.get("status") or None
            status = _INTERNAL_STATUS_MAP.get(ist, ist)
        return {"order_id": order_id, "status": status, "filled_qty": ifq + xfq,
                "filled_avg_price": avg, "internal_filled_qty": ifq, "external_filled_qty": xfq}
    except Exception as e:
        log.warning("child_state %s failed: %s", client_order_id, e)
        return None


# --------------------------------------------------------------------------- OMS calls
def submit_child(r, algo: dict, sl: dict, px, tif: str) -> dict:
    return runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
        "kind": "submit", "account": algo.get("account") or "paper",
        "client_order_id": sl["client_order_id"], "symbol": algo["symbol"], "side": algo["side"],
        "type": "limit", "qty": sl["qty"], "limit_price": px, "tif": tif, "source": "algo",
        "book": algo.get("book") or runtime.DEFAULT_BOOK,
        "algo_id": algo["algo_id"], "slice_idx": sl["slice_idx"]}, timeout=10)


def cancel_child(r, algo: dict, sl: dict) -> dict:
    return runtime.send_command(r, runtime.STREAM_OMS_COMMANDS, {
        "kind": "cancel", "account": algo.get("account") or "paper",
        "client_order_id": sl["client_order_id"], "source": "algo"}, timeout=10)


# --------------------------------------------------------------------------- requests
def _reply(ok, status, *, algo_id=None, algo=None, error=None) -> dict:
    return {"ok": bool(ok), "status": status, "algo_id": algo_id, "algo": algo, "error": error}


def _create(r, p: dict) -> dict:
    strategy = str(p.get("strategy") or "").strip().lower()
    cls = algos.REGISTRY.get(strategy)
    if cls is None:
        return _reply(False, "rejected", error=f"unknown strategy {strategy!r}; valid: {', '.join(algos.REGISTRY)}")
    if not cls.available:
        return _reply(False, "rejected", error=f"strategy {strategy} unavailable: {cls.note}")
    side = str(p.get("side") or "").lower()
    if side not in ("buy", "sell"):
        return _reply(False, "rejected", error="side must be buy or sell")
    total_qty = _f(p.get("total_qty"))
    if total_qty is None or total_qty <= 0:
        return _reply(False, "rejected", error="total_qty must be > 0")
    symbol = str(p.get("symbol") or "").strip()
    if not symbol:
        return _reply(False, "rejected", error="symbol required")
    ns = p.get("n_slices")
    try:
        n_slices = 5 if ns in (None, "") else int(ns)
        if float(ns if ns not in (None, "") else 5) != n_slices:
            raise ValueError
    except (TypeError, ValueError):
        return _reply(False, "rejected", error="n_slices must be an integer >= 1")
    if n_slices < 1:
        return _reply(False, "rejected", error="n_slices must be an integer >= 1")
    ds = p.get("duration_s")
    duration_s = 300.0 if ds in (None, "") else _f(ds)
    if duration_s is None or duration_s < 10 * n_slices:
        return _reply(False, "rejected", error=f"duration_s must be >= {10 * n_slices} for {n_slices} slices")
    raw_acct = p.get("account")
    account = "paper" if raw_acct in (None, "") else str(raw_acct).lower()
    if account not in ("paper", "live"):
        return _reply(False, "rejected", error="account must be paper or live")
    limit_cap = p.get("limit_cap")
    if limit_cap in (None, ""):
        limit_cap = None
    else:
        limit_cap = _f(limit_cap)
        if limit_cap is None or limit_cap <= 0:
            return _reply(False, "rejected", error="limit_cap must be a positive number")
    params = p.get("params") or {}
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except ValueError:
            params = {}
    if not isinstance(params, dict):
        params = {}

    rules = _rules(r, symbol, account)
    start = runtime.utc_ts() + 2
    end = start + duration_s
    try:
        sched = algos.build_schedule(strategy, total_qty, start, end, n_slices, rules, params)
    except (ValueError, NotImplementedError) as e:
        return _reply(False, "rejected", error=str(e))
    if not sched:
        return _reply(False, "rejected", error="empty schedule")

    algo_id = runtime.new_id()
    now = runtime.now_iso()
    book = str(p.get("book") or runtime.DEFAULT_BOOK)
    algo = {"algo_id": algo_id, "strategy": strategy, "account": account, "book": book,
            "symbol": symbol, "side": side,
            "total_qty": total_qty, "n_slices": len(sched), "start_ts": start, "end_ts": end,
            "limit_cap": limit_cap, "params_json": json.dumps(params, default=str),
            "source": p.get("source"), "status": "pending", "filled_qty": 0.0, "avg_px": None,
            "error": None, "created_at": now, "updated_at": now}
    slices = []
    for s in sched:
        slices.append({"algo_id": algo_id, "slice_idx": s.idx, "scheduled_ts": s.scheduled_ts,
                       "qty": s.qty, "client_order_id": f"algo-{algo_id[:12]}-{s.idx:03d}",
                       "order_id": None, "limit_price": None, "status": "scheduled",
                       "filled_qty": 0.0, "updated_at": now})
    for sl in slices:
        _hset_slice(r, sl)
    save_algo(r, algo, emit=True, slices=slices)
    log.info("algo %s created %s %s %s qty=%s slices=%d over %.0fs", algo_id, strategy, side, symbol,
             total_qty, len(slices), duration_s)
    return _reply(True, "pending", algo_id=algo_id, algo=_algo_view(algo))


def _cancel(r, p: dict) -> dict:
    algo_id = p.get("algo_id")
    algo = load_algo(r, algo_id) if algo_id else None
    if not algo:
        return _reply(False, "not_found", algo_id=algo_id, error="algo not found")
    if algo.get("status") in ALGO_TERMINAL:
        return _reply(True, algo["status"], algo_id=algo_id, algo=_algo_view(algo))
    slices = load_slices(r, algo_id)
    for sl in slices:
        st = sl.get("status")
        if st in SLICE_LIVE:
            try:
                rep = cancel_child(r, algo, sl) or {}
            except Exception as e:
                log.warning("cancel child %s failed: %s", sl.get("client_order_id"), e)
                rep = {}
            if rep.get("status") in ("cancel_requested", "not_found"):
                sl["status"] = "cancelled"
                save_slice(r, sl)
            else:
                log.warning("cancel child %s -> %s", sl.get("client_order_id"), rep.get("status"))
        elif st == "scheduled":
            sl["status"] = "cancelled"
            save_slice(r, sl)
    _aggregate(algo, slices)
    algo["status"] = "cancelled"
    save_algo(r, algo)
    log.info("algo %s cancelled", algo_id)
    return _reply(True, "cancelled", algo_id=algo_id, algo=_algo_view(algo))


def handle_request(r, payload: dict) -> dict:
    payload = payload if isinstance(payload, dict) else {}
    cmd_id = payload.get("cmd_id")
    kind = payload.get("kind")
    try:
        if kind == "create":
            reply = _create(r, payload)
        elif kind == "cancel":
            reply = _cancel(r, payload)
        else:
            reply = _reply(False, "error", algo_id=payload.get("algo_id"), error=f"unknown kind {kind!r}")
    except Exception as e:
        log.exception("algo request %s (%s) failed", cmd_id, kind)
        reply = _reply(False, "error", algo_id=payload.get("algo_id"), error=f"{type(e).__name__}: {e}")
    reply["cmd_id"] = cmd_id
    if r is not None and cmd_id:
        try:
            runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
        except Exception as e:
            log.error("send_reply %s failed: %s", cmd_id, e)
    log.info("request %s kind=%s -> %s%s", cmd_id, kind, reply.get("status"),
             f" ({reply.get('error')})" if reply.get("error") else "")
    return reply


# --------------------------------------------------------------------------- engine
def _aggregate(algo: dict, slices: list[dict]) -> None:
    total = 0.0
    notional = 0.0
    priced = 0.0
    for sl in slices:
        fq = _f(sl.get("filled_qty")) or 0.0
        total += fq
        px = _f(sl.get("filled_avg_price"))
        if fq > 0 and px is not None:
            notional += fq * px
            priced += fq
    algo["filled_qty"] = total
    algo["avg_px"] = (notional / priced) if priced > 0 else algo.get("avg_px")


def refresh(r, algo: dict, slices: list[dict], now_ts: float | None = None) -> None:
    now = runtime.utc_ts() if now_ts is None else now_ts
    for sl in slices:
        st0 = sl.get("status")
        if not sl.get("client_order_id") or st0 not in ("submitted", "working", "rolled"):
            continue
        before = (st0, sl.get("order_id"), sl.get("filled_qty"), sl.get("filled_avg_price"),
                  sl.get("error"))
        cs = child_state(r, sl["client_order_id"])
        if cs:
            if cs.get("order_id"):
                sl["order_id"] = cs["order_id"]
            fq = cs.get("filled_qty") or 0.0
            sl["filled_qty"] = max(_f(sl.get("filled_qty")) or 0.0, fq)
            if cs.get("filled_avg_price") is not None:
                sl["filled_avg_price"] = cs["filled_avg_price"]
            ost = cs.get("status")
            if st0 != "rolled":
                if ost == "filled":
                    sl["status"] = "filled"
                elif ost in runtime.ACTIVE_STATUSES:
                    sl["status"] = "working"
                elif ost in ("rejected", "expired"):
                    sl["status"] = "failed"
                    sl["error"] = f"child {ost}"
                elif ost in ("canceled", "replaced"):
                    sl["status"] = "cancelled"
        elif st0 == "submitted":
            sub = _f(sl.get("submitted_ts"))
            if sub is None:
                sl["submitted_ts"] = now
                _hset_slice(r, sl)
            elif now - sub > ACK_TIMEOUT_S:
                sl["status"] = "failed"
                sl["error"] = "child never acknowledged"
        after = (sl.get("status"), sl.get("order_id"), sl.get("filled_qty"), sl.get("filled_avg_price"),
                 sl.get("error"))
        if after != before:
            save_slice(r, sl)
    _aggregate(algo, slices)


def _finalise_status(algo: dict) -> str:
    total = _f(algo.get("total_qty")) or 0.0
    filled = _f(algo.get("filled_qty")) or 0.0
    if total > 0 and filled >= total * (1 - EPS):
        return "completed"
    if filled > 0:
        return "partial"
    return "failed"


def _process_algo(r, algo: dict, now: float, allow_submit: bool) -> None:
    aid = algo["algo_id"]
    snap = (algo.get("status"), algo.get("filled_qty"), algo.get("avg_px"), algo.get("error"))
    slices = load_slices(r, aid)
    refresh(r, algo, slices, now_ts=now)
    if not slices:
        algo["status"] = "failed"
        algo["error"] = "algo has no slices"
    last_idx = max((s["slice_idx"] for s in slices), default=-1)
    end_ts = _f(algo.get("end_ts")) or now

    if algo.get("status") == "pending":
        first = min((_f(s.get("scheduled_ts")) or 0.0 for s in slices), default=0.0)
        if now >= first:
            algo["status"] = "running"

    if algo.get("status") == "running" and allow_submit:
        due = [s for s in slices if s.get("status") == "scheduled" and (_f(s.get("scheduled_ts")) or 0) <= now]
        if due:
            rules = _rules(r, algo["symbol"], algo.get("account"))
            target = due[-1]
            # merge overdue scheduled slices (e.g. after downtime) into the latest due one
            for s in due[:-1]:
                target["qty"] = (_f(target.get("qty")) or 0.0) + (_f(s.get("qty")) or 0.0)
                s["status"] = "rolled"
                save_slice(r, s)
            # roll forward: cancel earlier resting children, add their remainder
            for s in slices:
                if s["slice_idx"] >= target["slice_idx"] or s.get("status") not in SLICE_LIVE:
                    continue
                try:
                    rep = cancel_child(r, algo, s) or {}
                except Exception as e:
                    rep = {"status": "error", "error": str(e)}
                if rep.get("status") not in ("cancel_requested", "not_found"):
                    log.warning("algo %s roll cancel %s -> %s", aid, s.get("client_order_id"),
                                rep.get("status"))
                cs = child_state(r, s.get("client_order_id"))
                if cs:
                    s["filled_qty"] = max(_f(s.get("filled_qty")) or 0.0, cs.get("filled_qty") or 0.0)
                    if cs.get("filled_avg_price") is not None:
                        s["filled_avg_price"] = cs["filled_avg_price"]
                    if cs.get("order_id"):
                        s["order_id"] = cs["order_id"]
                rem = max(0.0, (_f(s.get("qty")) or 0.0) - (_f(s.get("filled_qty")) or 0.0))
                if cs and cs.get("status") == "filled":
                    s["status"] = "filled"
                    rem = 0.0
                else:
                    s["status"] = "rolled"
                target["qty"] = (_f(target.get("qty")) or 0.0) + rem
                save_slice(r, s)
            _aggregate(algo, slices)
            # never exceed what is still needed
            total = _f(algo.get("total_qty")) or 0.0
            outstanding = sum((_f(s.get("qty")) or 0.0) - (_f(s.get("filled_qty")) or 0.0)
                              for s in slices if s.get("status") in SLICE_LIVE)
            need = max(0.0, total - (_f(algo.get("filled_qty")) or 0.0) - outstanding)
            qty = algos.round_qty(min(_f(target.get("qty")) or 0.0, need), rules.min_trade_increment)
            target["qty"] = qty
            final = target["slice_idx"] == last_idx
            if qty <= 0 or (rules.min_order_size and qty < rules.min_order_size and not final):
                if qty > 0 and not final:
                    # too small to send: roll into the next slice
                    nxt = next((s for s in slices if s["slice_idx"] > target["slice_idx"]
                                and s.get("status") == "scheduled"), None)
                    if nxt is not None:
                        nxt["qty"] = (_f(nxt.get("qty")) or 0.0) + qty
                        save_slice(r, nxt)
                        target["status"] = "rolled"
                    else:
                        target["status"] = "cancelled"
                else:
                    target["status"] = "cancelled"
                save_slice(r, target)
            elif rules.min_order_size and qty < rules.min_order_size:
                target["status"] = "cancelled"
                target["error"] = f"remaining qty {qty} below min_order_size"
                save_slice(r, target)
            else:
                px = None
                try:
                    quote = trader.latest_quote(algo["symbol"], r=r)
                    strat = algos.get_strategy(algo.get("strategy") or "twap", _params(algo))
                    px = strat.child_price(algo["side"], quote, _f(algo.get("limit_cap")), final, rules)
                except Exception as e:
                    log.warning("algo %s slice %s no price yet: %s", aid, target["slice_idx"], e)
                    px = None
                if px is None or px <= 0:
                    save_slice(r, target)  # persist merged qty; retry next tick
                    if now > end_ts + FINAL_GRACE_S:
                        target["status"] = "failed"
                        target["error"] = "no usable quote"
                        save_slice(r, target)
                else:
                    target["limit_price"] = px
                    rep = submit_child(r, algo, target, px, "ioc" if final else "gtc") or {}
                    st = rep.get("status")
                    if st in ("acked", "already_submitted"):
                        order = rep.get("order") if isinstance(rep.get("order"), dict) else {}
                        target["order_id"] = order.get("order_id") or order.get("id") or rep.get("order_id")
                        target["status"] = "submitted"
                        target["submitted_ts"] = now
                        save_slice(r, target)
                    elif st in ("rejected", "error"):
                        target["status"] = "failed"
                        target["error"] = rep.get("error") or st
                        save_slice(r, target)
                        algo["status"] = "failed"
                        algo["error"] = f"slice {target['slice_idx']} {st}: {rep.get('error')}"
                        for s in slices:
                            if s.get("status") in SLICE_LIVE:
                                try:
                                    cancel_child(r, algo, s)
                                except Exception as e:
                                    log.warning("cancel child failed: %s", e)
                            if s.get("status") == "scheduled":
                                s["status"] = "cancelled"
                                save_slice(r, s)
                    else:
                        target["status"] = "submitted"
                        target["submitted_ts"] = now
                        target["error"] = rep.get("error")
                        save_slice(r, target)
                    log.info("algo %s slice %d qty=%s px=%s tif=%s -> %s", aid, target["slice_idx"], qty,
                             px, "ioc" if final else "gtc", st)

    if algo.get("status") == "running":
        if slices and all(s.get("status") in SLICE_TERMINAL for s in slices):
            _aggregate(algo, slices)
            algo["status"] = _finalise_status(algo)
        else:
            final_sl = next((s for s in slices if s["slice_idx"] == last_idx), None)
            if final_sl is not None and final_sl.get("status") in SLICE_LIVE \
                    and now > end_ts + FINAL_GRACE_S \
                    and all(s.get("status") in SLICE_TERMINAL for s in slices if s is not final_sl):
                try:
                    cancel_child(r, algo, final_sl)
                except Exception as e:
                    log.warning("cancel final child failed: %s", e)
                cs = child_state(r, final_sl.get("client_order_id"))
                if cs:
                    final_sl["filled_qty"] = max(_f(final_sl.get("filled_qty")) or 0.0,
                                                 cs.get("filled_qty") or 0.0)
                    if cs.get("filled_avg_price") is not None:
                        final_sl["filled_avg_price"] = cs["filled_avg_price"]
                final_sl["status"] = "cancelled"
                save_slice(r, final_sl)
                _aggregate(algo, slices)
                algo["status"] = _finalise_status(algo)
                if algo["status"] == "completed":
                    algo["status"] = "partial" if (_f(algo.get("filled_qty")) or 0) > 0 else "failed"
                    if (_f(algo.get("filled_qty")) or 0) >= (_f(algo.get("total_qty")) or 0) * (1 - EPS):
                        algo["status"] = "completed"

    now_snap = (algo.get("status"), algo.get("filled_qty"), algo.get("avg_px"), algo.get("error"))
    if now_snap != snap:
        save_algo(r, algo)
        if algo.get("status") in ALGO_TERMINAL:
            log.info("algo %s finished %s filled=%s avg_px=%s", aid, algo["status"], algo.get("filled_qty"),
                     algo.get("avg_px"))


def tick(r, now_ts: float | None = None, allow_submit: bool = True) -> int:
    now = runtime.utc_ts() if now_ts is None else float(now_ts)
    try:
        ids = runtime.redis_call(r.smembers, runtime.SET_ACTIVE_ALGOS) or set()
    except Exception as e:
        log.warning("tick: cannot read active algos: %s", e)
        return 0
    touched = 0
    for aid in sorted(ids):
        try:
            algo = load_algo(r, aid)
            if not algo:
                runtime.redis_call(r.srem, runtime.SET_ACTIVE_ALGOS, aid)
                continue
            if algo.get("status") in ALGO_TERMINAL:
                runtime.redis_call(r.srem, runtime.SET_ACTIVE_ALGOS, aid)
                continue
            touched += 1
            _process_algo(r, algo, now, allow_submit)
        except Exception as e:
            log.exception("algo %s tick failed", aid)
            try:
                algo = load_algo(r, aid) or {"algo_id": aid}
                algo["algo_id"] = algo.get("algo_id") or aid
                algo["status"] = "failed"
                algo["error"] = f"{type(e).__name__}: {e}"[:500]
                save_algo(r, algo)
            except Exception as e2:
                log.error("marking algo %s failed did not work: %s", aid, e2)
    return touched


# --------------------------------------------------------------------------- cold start
def reload_from_db(r, db_path: str | None = None) -> int:
    conn = None
    try:
        if int(runtime.redis_call(r.scard, runtime.SET_ACTIVE_ALGOS) or 0) > 0:
            return 0
        path = db_path or runtime.DB_PATH
        if path != ":memory:" and not os.path.exists(path):
            return 0
        conn = runtime.sqlite_connect(path)
        rows = conn.execute("SELECT * FROM algo_orders WHERE status IN ('pending','running')").fetchall()
        n = 0
        for row in rows:
            d = dict(row)
            aid = d.get("algo_id")
            if not aid:
                continue
            if d.get("params_json") is None and d.get("params") is not None:
                d["params_json"] = d.get("params")
            algo = _typed_algo({k: d.get(k) for k in ALGO_FIELDS})
            algo["algo_id"] = aid
            srows = conn.execute("SELECT * FROM algo_slices WHERE algo_id = ? ORDER BY slice_idx",
                                 (aid,)).fetchall()
            for sr in srows:
                sd = dict(sr)
                sl = _typed_slice({k: sd.get(k) for k in ("algo_id", "slice_idx", "scheduled_ts", "qty",
                                                          "client_order_id", "order_id", "limit_price",
                                                          "status", "filled_qty", "updated_at")
                                   if k in sd})
                sl["algo_id"] = aid
                if not sl.get("client_order_id"):
                    sl["client_order_id"] = f"algo-{str(aid)[:12]}-{sl['slice_idx']:03d}"
                sl["status"] = sl.get("status") or "scheduled"
                _hset_slice(r, sl)
            runtime.redis_call(runtime.hset_state, r, runtime.algo_key(aid), _algo_view(algo))
            runtime.redis_call(r.sadd, runtime.SET_ACTIVE_ALGOS, aid)
            n += 1
        if n:
            log.info("reloaded %d active algos from %s", n, path)
        return n
    except Exception as e:
        log.warning("reload_from_db failed: %s: %s", type(e).__name__, e)
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# --------------------------------------------------------------------------- loops
def process_requests(r, reader) -> int:
    entries = reader.read()
    n = 0
    for entry_id, fields in entries:
        payload = None
        try:
            payload = json.loads(fields.get("json") or "{}")
            if not isinstance(payload, dict):
                raise ValueError("payload is not an object")
            handle_request(r, payload)
            reader.ack(entry_id)
        except Exception as e:
            log.exception("algo request entry %s failed", entry_id)
            try:
                reader.dead_letter(entry_id, fields, f"{type(e).__name__}: {e}")
            except Exception as e2:
                log.error("dead_letter %s failed: %s", entry_id, e2)
            cmd_id = payload.get("cmd_id") if isinstance(payload, dict) else None
            if cmd_id:
                try:
                    runtime.send_reply(r, cmd_id, {"ok": False, "status": "error", "cmd_id": cmd_id,
                                                   "algo_id": payload.get("algo_id"), "algo": None,
                                                   "error": f"{type(e).__name__}: {e}"})
                except Exception:
                    pass
        n += 1
    return n


def body():
    r = runtime.get_redis()
    reload_from_db(r)
    consumer = os.environ.get("ALGO_CONSUMER", "algo-1")
    reader = runtime.GroupReader(r, runtime.STREAM_ALGO_REQUESTS, "algo_engine", consumer, block_ms=500)
    log.info("algo_engine started consumer=%s", consumer)
    last_warn = 0.0
    while True:
        t0 = time.monotonic()
        process_requests(r, reader)
        oms_up = runtime.worker_alive(r, "oms") is not None
        if not oms_up and time.monotonic() - last_warn >= 30:
            log.warning("OMS heartbeat missing; child submissions paused")
            last_warn = time.monotonic()
        tick(r, allow_submit=oms_up)
        elapsed = time.monotonic() - t0
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)


def main():
    runtime.run_forever(body, "algo_engine")


if __name__ == "__main__":
    main()
