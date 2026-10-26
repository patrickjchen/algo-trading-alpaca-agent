"""Crossing service: the CENTRAL LIQUIDITY BOOK between OMS and EMS.

OMS forwards every submit / cancel_order on alpaca:cross:requests. Offsetting internal interest
(same symbol + account, opposite sides, crossing prices) is matched at the midpoint BEFORE any
routing; resting limit orders wait a short hold window for a counterparty; only the residual is
routed to EMS (this module is the ONLY writer of alpaca:ems:requests).

Never calls Alpaca order endpoints, never touches SQLite, never writes OMS order hashes: it
publishes internal_* events on alpaca:trade_updates and OMS/persister apply them.
"""
from __future__ import annotations

import json
import os
import re
import time

import algos
import runtime
import trader

log = runtime.log("crossing")

COID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
MARKET_BUY_SCORE = 1e12
MARKET_SELL_SCORE = 0.0
STATUS_MAP = {"resting": "accepted", "filled": "filled", "routed": "replaced",
              "canceled": "canceled", "rejected": "rejected"}

_spill = runtime.SpillBuffer()
_last_ems_warn = 0.0


# --------------------------------------------------------------------------- config
def _cross_enabled() -> bool:
    return str(os.environ.get("CROSS_ENABLED", "1")).strip().lower() not in ("0", "false", "no", "off", "")


def _hold_s_default() -> float:
    try:
        return float(os.environ.get("CROSS_HOLD_S", "1.0"))
    except ValueError:
        return 1.0


def _fee_bps() -> float:
    try:
        return float(os.environ.get("FEE_BPS", "25"))
    except ValueError:
        return 25.0


# --------------------------------------------------------------------------- helpers
def _f(x, default=None):
    if x is None or x == "":
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _s(x):
    if x is None or x == "" or x == "None":
        return None
    return str(x)


def _get_hash(r, coid: str) -> dict:
    try:
        return runtime.redis_call(r.hgetall, runtime.book_order_key(coid)) or {}
    except Exception:
        log.exception("hgetall failed for %s", coid)
        return {}


def _save(r, coid: str, mapping: dict, terminal: bool = False) -> None:
    runtime.hset_state(r, runtime.book_order_key(coid), mapping,
                       ttl=runtime.STATE_TTL_S if terminal else None)


def _remove_from_book(r, h: dict) -> None:
    coid = h.get("client_order_id")
    sym, side = h.get("symbol"), h.get("side")
    try:
        pipe = r.pipeline()
        if sym and side:
            pipe.zrem(runtime.book_key(sym, side), coid)
        pipe.srem(runtime.SET_RESTING, coid)
        pipe.execute()
    except Exception:
        log.exception("remove_from_book failed for %s", coid)


def _stats(r, symbol: str | None, incs: dict) -> None:
    keys = [runtime.CROSS_STATS_KEY]
    if symbol:
        keys.append(runtime.cross_stats_key(symbol))
    for k in keys:
        for field, amt in incs.items():
            try:
                runtime.redis_call(r.hincrbyfloat, k, field, float(amt), buffer=_spill)
            except Exception:
                log.exception("stats update failed %s %s", k, field)


def _score(side: str, otype: str, limit_price) -> float:
    if otype == "market" or limit_price is None:
        return MARKET_BUY_SCORE if side == "buy" else MARKET_SELL_SCORE
    return float(limit_price)


# --------------------------------------------------------------------------- order views / events
def as_order(h: dict) -> dict:
    h = h or {}
    crossed = _f(h.get("crossed_qty"), 0.0) or 0.0
    notional = _f(h.get("crossed_notional"), 0.0) or 0.0
    st = h.get("status") or "resting"
    status = STATUS_MAP.get(st, st)
    if st == "resting" and crossed > 0:
        status = "partially_filled"
    return {
        "id": h.get("order_id") or (runtime.internal_order_id(h["client_order_id"])
                                    if h.get("client_order_id") else None),
        "client_order_id": h.get("client_order_id"),
        "symbol": h.get("symbol"),
        "side": h.get("side"),
        "type": h.get("type"),
        "time_in_force": h.get("time_in_force"),
        "qty": _f(h.get("qty")),
        "limit_price": _f(h.get("limit_price")),
        "filled_qty": crossed,
        "filled_avg_price": (notional / crossed) if crossed > 0 else None,
        "status": status,
        "submitted_at": h.get("received_at") or h.get("received_ts"),
        "updated_at": runtime.now_iso(),
        "venue": "internal",
        "book": h.get("book"),
        "routed_order_id": _s(h.get("routed_order_id")),
        "error": _s(h.get("error")),
    }


def _slice_idx(h: dict):
    v = h.get("slice_idx")
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def emit(r, event: str, h: dict, **extra) -> None:
    try:
        h = h or {}
        payload = {
            "event": event,
            "execution_id": extra.get("execution_id"),
            "order_id": h.get("order_id"),
            "client_order_id": h.get("client_order_id"),
            "symbol": h.get("symbol"),
            "side": h.get("side"),
            "price": extra.get("price"),
            "qty": extra.get("qty"),
            "position_qty": None,
            "ts": runtime.now_iso(),
            "account": h.get("account"),
            "order": as_order(h),
            "source": _s(h.get("source")),
            "algo_id": _s(h.get("algo_id")),
            "slice_idx": _slice_idx(h),
            "venue": "internal",
            "book": h.get("book"),
        }
        payload.update(extra)
        runtime.publish(r, runtime.STREAM_TRADE_UPDATES, payload, buffer=_spill)
    except Exception:
        log.exception("emit %s failed", event)


# --------------------------------------------------------------------------- pricing
def cross_price(buy: dict, sell: dict, quote: dict | None, rules=None) -> float | None:
    b_type = str(buy.get("type") or "market").lower()
    s_type = str(sell.get("type") or "market").lower()
    b_lim = _f(buy.get("limit_price")) if b_type == "limit" else None
    s_lim = _f(sell.get("limit_price")) if s_type == "limit" else None
    if b_lim is not None and s_lim is not None:
        if b_lim < s_lim:
            return None
        px = (b_lim + s_lim) / 2.0
    elif b_lim is not None:
        px = b_lim
    elif s_lim is not None:
        px = s_lim
    else:
        q = quote or {}
        bid, ask = _f(q.get("bid_price")), _f(q.get("ask_price"))
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        px = (bid + ask) / 2.0
    inc = None
    if rules is not None:
        inc = rules.get("price_increment") if isinstance(rules, dict) else getattr(rules, "price_increment", None)
    if inc:
        try:
            px = algos.round_price(px, inc, side="buy")
        except Exception:
            pass
    if s_lim is not None and px < s_lim:
        px = s_lim
    if b_lim is not None and px > b_lim:
        px = b_lim
    if px is None or px <= 0:
        return None
    return float(px)


# --------------------------------------------------------------------------- matching
def match(r, coid: str, quote: dict | None, rules=None) -> list:
    h = _get_hash(r, coid)
    if not h or h.get("status") != "resting":
        return []
    remaining = _f(h.get("remaining"), 0.0) or 0.0
    if remaining <= 0 or h.get("cross") == "deny":
        return []
    side, sym = h.get("side"), h.get("symbol")
    otype = h.get("type")
    lim = _f(h.get("limit_price"))
    opp = "sell" if side == "buy" else "buy"
    zkey = runtime.book_key(sym, opp)
    try:
        if side == "buy":
            mx = "+inf" if (otype == "market" or lim is None) else lim
            cands = runtime.redis_call(r.zrangebyscore, zkey, "-inf", mx, withscores=True) or []
        else:
            mn = "-inf" if (otype == "market" or lim is None) else lim
            cands = runtime.redis_call(r.zrevrangebyscore, zkey, "+inf", mn, withscores=True) or []
    except Exception:
        log.exception("candidate lookup failed for %s", coid)
        return []

    hashes = {}
    for member, _ in cands:
        if member == coid:
            continue
        hashes[member] = _get_hash(r, member)
    sign = 1.0 if side == "buy" else -1.0
    ordered = sorted(
        [(m, s) for m, s in cands if m in hashes],
        key=lambda ms: (sign * float(ms[1]), _f(hashes[ms[0]].get("received_ts"), 0.0) or 0.0))

    fee_bps = _fee_bps()
    q = quote or {}
    bid, ask = _f(q.get("bid_price")), _f(q.get("ask_price"))
    out = []
    for member, _score_v in ordered:
        if remaining <= 0:
            break
        c = hashes.get(member) or {}
        if not c:
            try:
                runtime.redis_call(r.zrem, zkey, member)
            except Exception:
                pass
            continue
        if c.get("status") != "resting":
            continue
        if (c.get("account") or "paper") != (h.get("account") or "paper"):
            continue
        if c.get("cross") == "deny":
            continue
        c_rem = _f(c.get("remaining"), 0.0) or 0.0
        if c_rem <= 0:
            continue
        buy, sell = (h, c) if side == "buy" else (c, h)
        px = cross_price(buy, sell, quote, rules)
        if px is None:
            continue
        qty = min(remaining, c_rem)
        if qty <= 0:
            continue
        cross_id = runtime.new_id()[:12]
        ts = runtime.now_iso()
        notional = qty * px
        for leg in (h, c):
            leg_rem = max(0.0, (_f(leg.get("remaining"), 0.0) or 0.0) - qty)
            if leg_rem < 1e-12:
                leg_rem = 0.0
            leg["remaining"] = leg_rem
            leg["crossed_qty"] = (_f(leg.get("crossed_qty"), 0.0) or 0.0) + qty
            leg["crossed_notional"] = (_f(leg.get("crossed_notional"), 0.0) or 0.0) + notional
            terminal = leg_rem <= 0
            if terminal:
                leg["status"] = "filled"
                _remove_from_book(r, leg)
            _save(r, leg["client_order_id"], {
                "remaining": leg["remaining"], "crossed_qty": leg["crossed_qty"],
                "crossed_notional": leg["crossed_notional"], "status": leg["status"],
            }, terminal=terminal)
        for leg, other in ((h, c), (c, h)):
            emit(r, "internal_fill", leg,
                 execution_id=f"x-{cross_id}-{(leg.get('side') or 'x')[0]}",
                 cross_id=cross_id, counter_client_order_id=other.get("client_order_id"),
                 counter_book=other.get("book"), price=px, qty=qty)
        fee_saved = notional * fee_bps / 1e4 * 2
        spread_saved = (ask - bid) * qty if (bid is not None and ask is not None) else 0.0
        _stats(r, sym, {"crosses": 1, "crossed_qty": qty, "crossed_notional": notional,
                        "est_fee_saved": fee_saved, "est_spread_saved": spread_saved})
        rec = {"cross_id": cross_id, "symbol": sym, "price": px, "qty": qty,
               "buy_coid": buy.get("client_order_id"), "sell_coid": sell.get("client_order_id"),
               "buy_book": buy.get("book"), "sell_book": sell.get("book"), "ts": ts}
        out.append(rec)
        log.info("CROSS %s qty=%s px=%s %s(%s) x %s(%s) saved≈$%.4f", sym, qty, px,
                 rec["buy_coid"], rec["buy_book"], rec["sell_coid"], rec["sell_book"],
                 fee_saved + spread_saved)
        remaining = _f(h.get("remaining"), 0.0) or 0.0
    return out


# --------------------------------------------------------------------------- routing
def forward_to_ems(r, payload: dict, timeout: float = 10.0) -> dict:
    p = dict(payload or {})
    p["cmd_id"] = runtime.new_id()
    return runtime.send_command(r, runtime.STREAM_EMS_REQUESTS, p, timeout)


def route(r, coid: str) -> dict:
    h = _get_hash(r, coid)
    if not h:
        return {"ok": False, "status": "not_found", "error": "no such order"}
    remaining = _f(h.get("remaining"), 0.0) or 0.0
    if h.get("status") != "resting" or remaining <= 0:
        return {"ok": False, "status": "error", "error": f"order is {h.get('status')} remaining={remaining}"}
    payload = {"kind": "submit", "account": h.get("account") or "paper", "client_order_id": coid,
               "symbol": h.get("symbol"), "side": h.get("side"), "type": h.get("type"),
               "qty": remaining, "limit_price": _f(h.get("limit_price")),
               "tif": h.get("time_in_force") or "gtc", "source": _s(h.get("source")),
               "algo_id": _s(h.get("algo_id")), "slice_idx": _slice_idx(h), "book": h.get("book")}
    try:
        rep = forward_to_ems(r, payload)
    except Exception as e:
        log.exception("forward_to_ems failed for %s", coid)
        rep = {"ok": False, "status": "unknown", "error": f"{type(e).__name__}: {e}"}
    rep = rep if isinstance(rep, dict) else {"ok": False, "status": "unknown", "error": "bad reply"}
    st = rep.get("status")
    if st in ("acked", "already_submitted"):
        roid = rep.get("order_id")
        h.update({"status": "routed", "routed_order_id": roid})
        _remove_from_book(r, h)
        _save(r, coid, {"status": "routed", "routed_order_id": roid}, terminal=True)
        emit(r, "internal_routed", h, routed_order_id=roid)
        _stats(r, h.get("symbol"), {"routed_orders": 1, "routed_qty": remaining})
        log.info("ROUTED %s %s %s qty=%s -> %s (%s)", coid, h.get("symbol"), h.get("side"),
                 remaining, roid, st)
    elif st in ("rejected", "error"):
        err = rep.get("error") or st
        h.update({"status": "rejected", "error": err})
        _remove_from_book(r, h)
        _save(r, coid, {"status": "rejected", "error": err}, terminal=True)
        emit(r, "internal_rejected", h, error=err)
        log.warning("ROUTE REJECTED %s: %s", coid, err)
    else:
        h.update({"status": "routed"})
        _remove_from_book(r, h)
        _save(r, coid, {"status": "routed", "error": rep.get("error")}, terminal=True)
        emit(r, "internal_routed", h, routed_order_id=None)
        log.warning("ROUTE %s status=%s (%s); OMS reconciliation will resolve", coid, st,
                    rep.get("error"))
    return rep


# --------------------------------------------------------------------------- accept / cancel
def _validate(p: dict):
    side = str(p.get("side") or "").lower()
    otype = str(p.get("type") or "market").lower()
    tif = str(p.get("tif") or p.get("time_in_force") or "gtc").lower()
    coid = p.get("client_order_id")
    account = str(p.get("account") or "paper").lower()
    if side not in ("buy", "sell"):
        return None, "side must be buy or sell"
    if otype not in ("market", "limit"):
        return None, "type must be market or limit"
    qty = _f(p.get("qty"))
    if qty is None or qty <= 0:
        return None, "qty must be > 0"
    lim = _f(p.get("limit_price"))
    if otype == "limit" and (lim is None or lim <= 0):
        return None, "limit order requires limit_price > 0"
    if otype == "market":
        lim = None
    if tif not in ("gtc", "ioc"):
        return None, "tif must be gtc or ioc"
    if not isinstance(coid, str) or not COID_RE.match(coid):
        return None, "client_order_id must match ^[A-Za-z0-9_-]{1,48}$"
    if account not in ("paper", "live"):
        return None, "account must be paper or live"
    symbol = p.get("symbol")
    if not symbol or not isinstance(symbol, str):
        return None, "symbol required"
    return {"side": side, "type": otype, "tif": tif, "qty": qty, "limit_price": lim,
            "client_order_id": coid, "account": account, "symbol": symbol}, None


def accept(r, p: dict) -> dict:
    p = p or {}
    v, err = _validate(p)
    if err:
        return {"ok": False, "status": "rejected", "order_id": None,
                "client_order_id": p.get("client_order_id"), "order": None, "error": err}
    coid = v["client_order_id"]
    oid = runtime.internal_order_id(coid)
    existing = _get_hash(r, coid)
    if existing:
        return {"ok": True, "status": "already_submitted", "order_id": oid, "client_order_id": coid,
                "order": as_order(existing), "venue": "internal", "error": None,
                "routed_order_id": _s(existing.get("routed_order_id"))}
    try:
        hold_s = float(p.get("hold_s")) if p.get("hold_s") is not None else _hold_s_default()
    except (TypeError, ValueError):
        hold_s = _hold_s_default()
    cross = str(p.get("cross") or "allow").lower()
    now = runtime.utc_ts()
    h = {
        "client_order_id": coid, "order_id": oid, "account": v["account"],
        "book": p.get("book") or runtime.DEFAULT_BOOK, "symbol": v["symbol"], "side": v["side"],
        "type": v["type"], "time_in_force": v["tif"], "qty": v["qty"], "remaining": v["qty"],
        "crossed_qty": 0.0, "crossed_notional": 0.0, "limit_price": v["limit_price"],
        "source": p.get("source"), "algo_id": p.get("algo_id"), "slice_idx": p.get("slice_idx"),
        "status": "resting", "received_ts": now, "received_at": runtime.now_iso(),
        "route_at": now + max(0.0, hold_s), "hold_s": hold_s, "cross": cross,
    }
    _save(r, coid, h)
    pipe = r.pipeline()
    pipe.sadd(runtime.SET_RESTING, coid)
    pipe.zadd(runtime.book_key(v["symbol"], v["side"]),
              {coid: _score(v["side"], v["type"], v["limit_price"])})
    pipe.execute()
    _stats(r, v["symbol"], {"accepted_orders": 1})
    h = _get_hash(r, coid) or {k: (str(x) if x is not None else None) for k, x in h.items()}
    emit(r, "internal_accepted", h)

    crosses = []
    if _cross_enabled() and cross != "deny":
        quote = None
        rules = None
        try:
            quote = trader.latest_quote(v["symbol"], r=r)
        except Exception as e:
            log.debug("quote unavailable for %s: %s", v["symbol"], e)
        try:
            rules = algos.rules_from_dict(trader.get_asset_rules(v["symbol"], v["account"] == "live", r=r))
        except Exception as e:
            log.debug("asset rules unavailable for %s: %s", v["symbol"], e)
        try:
            crosses = match(r, coid, quote, rules)
        except Exception:
            log.exception("match failed for %s", coid)

    h = _get_hash(r, coid)
    remaining = _f(h.get("remaining"), 0.0) or 0.0
    crossed_qty = _f(h.get("crossed_qty"), 0.0) or 0.0
    rep = None
    if h.get("status") == "resting" and remaining > 0 and (
            v["type"] == "market" or v["tif"] == "ioc" or hold_s <= 0):
        if runtime.worker_alive(r, "ems"):
            rep = route(r, coid)
        else:
            log.warning("ems not running; %s stays resting and will route when ems is back", coid)
            _save(r, coid, {"route_at": runtime.utc_ts()})
        h = _get_hash(r, coid)
    ok, status, error = True, "acked", None
    if rep is not None and rep.get("status") in ("rejected", "error") and crossed_qty <= 0:
        ok, status, error = False, "rejected", rep.get("error")
    log.info("ACCEPT %s %s %s %s qty=%s px=%s book=%s crossed=%s remaining=%s status=%s",
             coid, v["symbol"], v["side"], v["type"], v["qty"], v["limit_price"], h.get("book"),
             crossed_qty, _f(h.get("remaining")), h.get("status"))
    return {"ok": ok, "status": status, "order_id": oid, "client_order_id": coid,
            "order": as_order(h), "venue": "internal", "crossed_qty": crossed_qty,
            "remaining": _f(h.get("remaining"), 0.0), "crosses": [c["cross_id"] for c in crosses],
            "routed_order_id": _s(h.get("routed_order_id")), "error": error}


def cancel(r, p: dict) -> dict:
    p = p or {}
    coid = p.get("client_order_id")
    order_id = p.get("order_id")
    if not coid and order_id and str(order_id).startswith("int-"):
        coid = str(order_id)[4:]
    h = _get_hash(r, coid) if coid else {}
    account = p.get("account") or (h.get("account") if h else None) or "paper"
    if h:
        st = h.get("status")
        oid = h.get("order_id") or runtime.internal_order_id(coid)
        if st == "resting":
            h["status"] = "canceled"
            _remove_from_book(r, h)
            _save(r, coid, {"status": "canceled"}, terminal=True)
            emit(r, "internal_canceled", h)
            log.info("CANCEL %s (resting)", coid)
            return {"ok": True, "status": "cancel_requested", "order_id": oid,
                    "client_order_id": coid, "error": None}
        roid = _s(h.get("routed_order_id"))
        if st == "routed":
            payload = {"kind": "cancel_order", "account": h.get("account") or account,
                       "order_id": roid, "client_order_id": coid, "source": p.get("source")}
            try:
                rep = forward_to_ems(r, payload)
            except Exception as e:
                rep = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
            rep = dict(rep or {})
            rep.setdefault("order_id", roid)
            rep.setdefault("client_order_id", coid)
            return rep
        return {"ok": False, "status": "not_found", "order_id": oid, "client_order_id": coid,
                "error": f"order is {st}"}
    if order_id and not str(order_id).startswith("int-"):
        payload = {"kind": "cancel_order", "account": account, "order_id": order_id,
                   "client_order_id": coid, "source": p.get("source")}
        try:
            rep = forward_to_ems(r, payload)
        except Exception as e:
            rep = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
        rep = dict(rep or {})
        rep.setdefault("order_id", order_id)
        return rep
    if coid and not order_id:
        # maybe an exchange-only order known by coid; let EMS resolve it
        payload = {"kind": "cancel_order", "account": account, "order_id": None,
                   "client_order_id": coid, "source": p.get("source")}
        if runtime.worker_alive(r, "ems"):
            try:
                rep = forward_to_ems(r, payload)
            except Exception as e:
                rep = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
            return dict(rep or {})
    return {"ok": False, "status": "not_found", "order_id": order_id, "client_order_id": coid,
            "error": "order not found"}


# --------------------------------------------------------------------------- dispatch
def handle_request(r, payload: dict) -> dict:
    payload = payload or {}
    cmd_id = payload.get("cmd_id") or payload.get("req_id")
    kind = payload.get("kind")
    try:
        if kind == "submit":
            reply = accept(r, payload)
        elif kind in ("cancel_order", "cancel"):
            reply = cancel(r, payload)
        else:
            reply = {"ok": False, "status": "error", "error": f"unknown kind {kind!r}"}
    except Exception as e:
        log.exception("handle_request failed")
        reply = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
    reply["cmd_id"] = cmd_id
    if r is not None and cmd_id:
        try:
            runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
        except Exception:
            log.exception("send_reply failed cmd_id=%s", cmd_id)
    return reply


def tick(r, now_ts: float | None = None) -> int:
    global _last_ems_warn
    now = runtime.utc_ts() if now_ts is None else float(now_ts)
    try:
        members = runtime.redis_call(r.smembers, runtime.SET_RESTING) or set()
    except Exception:
        log.exception("smembers resting failed")
        return 0
    routed = 0
    ems_ok = None
    for coid in sorted(members):
        try:
            h = _get_hash(r, coid)
            if not h:
                runtime.redis_call(r.srem, runtime.SET_RESTING, coid)
                continue
            if h.get("status") != "resting":
                _remove_from_book(r, h)
                continue
            remaining = _f(h.get("remaining"), 0.0) or 0.0
            if remaining <= 0:
                continue
            if (_f(h.get("route_at"), 0.0) or 0.0) > now:
                continue
            if ems_ok is None:
                ems_ok = bool(runtime.worker_alive(r, "ems"))
            if not ems_ok:
                if time.monotonic() - _last_ems_warn > 30:
                    _last_ems_warn = time.monotonic()
                    log.warning("ems not running; %d resting order(s) waiting to route", len(members))
                break
            rep = route(r, coid)
            if rep.get("status") in ("acked", "already_submitted"):
                routed += 1
        except Exception:
            log.exception("tick failed for %s", coid)
    return routed


def book_snapshot(r, symbol: str) -> dict:
    out = {"symbol": symbol, "bids": [], "asks": []}
    now = runtime.utc_ts()
    for side, key, rev in (("buy", "bids", True), ("sell", "asks", False)):
        try:
            zk = runtime.book_key(symbol, side)
            items = (r.zrevrange(zk, 0, -1, withscores=True) if rev
                     else r.zrange(zk, 0, -1, withscores=True)) or []
        except Exception:
            items = []
        rows = []
        for member, score in items:
            h = _get_hash(r, member)
            if not h:
                continue
            rts = _f(h.get("received_ts"), now) or now
            px = _f(h.get("limit_price"))
            rows.append({"price": px, "qty": _f(h.get("remaining"), 0.0), "book": h.get("book"),
                         "client_order_id": member, "type": h.get("type"),
                         "age_s": round(max(0.0, now - rts), 3), "_score": float(score), "_ts": rts})
        sign = -1.0 if rev else 1.0
        rows.sort(key=lambda d: (sign * d["_score"], d["_ts"]))
        for d in rows:
            d.pop("_score", None)
            d.pop("_ts", None)
        out[key] = rows
    return out


def _parse_entry(fields: dict) -> dict:
    raw = (fields or {}).get("json")
    if raw is None:
        return dict(fields or {})
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError("payload is not an object")
    return d


def process_requests(r, reader) -> int:
    entries = reader.read()
    for eid, fields in entries:
        payload = None
        try:
            payload = _parse_entry(fields)
            handle_request(r, payload)
            reader.ack(eid)
        except Exception as e:
            log.exception("request %s failed", eid)
            try:
                reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
            except Exception:
                log.exception("dead_letter failed for %s", eid)
            cmd_id = (payload or {}).get("cmd_id")
            if cmd_id:
                try:
                    runtime.send_reply(r, cmd_id, {"ok": False, "status": "error", "cmd_id": cmd_id,
                                                   "error": f"{type(e).__name__}: {e}"})
                except Exception:
                    pass
    return len(entries)


def body() -> None:
    r = runtime.get_redis()
    consumer = os.environ.get("CROSS_CONSUMER", "cross-1")
    reader = runtime.GroupReader(r, runtime.STREAM_CROSS_REQUESTS, "crossing", consumer, block_ms=100)
    log.info("crossing started consumer=%s enabled=%s hold_s=%s fee_bps=%s", consumer,
             _cross_enabled(), _hold_s_default(), _fee_bps())
    while True:
        n = process_requests(r, reader)
        routed = tick(r)
        if not n and not routed:
            time.sleep(0.02)


def main() -> None:
    runtime.run_forever(body, "crossing")


if __name__ == "__main__":
    main()
