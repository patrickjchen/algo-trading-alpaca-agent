"""EMS worker: the exchange gateway. The ONLY process calling Alpaca order endpoints (via trader.py).

Serves submit / cancel_order requests from crossing.py on alpaca:ems:requests and publishes an
execution report to alpaca:trade_updates for each successful exchange interaction.
No algo logic, no SQLite.
"""
from __future__ import annotations

import json
import os
import re
import time

from alpaca.common.exceptions import APIError

import runtime
import trader

log = runtime.log("ems")

COID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
RETRY_DELAYS = (0.5, 1.0, 2.0)

_spill = runtime.SpillBuffer()


def _sleep(s: float) -> None:
    time.sleep(s)


def _status_code(exc):
    try:
        return exc.status_code
    except Exception:
        return None


def _err_text(exc) -> str:
    try:
        info = trader.error_info(exc)
        msg = info.get("message") or str(exc)
    except Exception:
        msg = str(exc)
    return str(msg)[:500]


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- reports
def publish_report(r, event: str, order: dict | None, *, account, client_order_id=None, order_id=None,
                   symbol=None, side=None, qty=None, source=None, algo_id=None, slice_idx=None,
                   book=None) -> None:
    try:
        if r is None:
            return
        o = order or {}
        norm = runtime.normalize_order(o) if o else {}
        payload = {
            "event": event,
            "execution_id": None,
            "order_id": order_id or norm.get("order_id"),
            "client_order_id": client_order_id or norm.get("client_order_id"),
            "symbol": symbol or norm.get("symbol"),
            "side": side or norm.get("side"),
            "price": None,
            "qty": qty if qty is not None else norm.get("qty"),
            "position_qty": None,
            "ts": runtime.now_iso(),
            "account": account,
            "order": order,
            "source": source,
            "algo_id": algo_id,
            "slice_idx": slice_idx,
            "book": book,
            "venue": "alpaca",
        }
        runtime.publish(r, runtime.STREAM_TRADE_UPDATES, payload, buffer=_spill)
    except Exception:
        log.exception("publish_report failed event=%s", event)


# --------------------------------------------------------------------------- submit
def _validate_submit(p: dict):
    side = str(p.get("side") or "").lower()
    otype = str(p.get("type") or "market").lower()
    tif = str(p.get("tif") or "gtc").lower()
    coid = p.get("client_order_id")
    if side not in ("buy", "sell"):
        return None, "side must be buy or sell"
    if otype not in ("market", "limit"):
        return None, "type must be market or limit"
    qty = _f(p.get("qty"))
    if qty is None or qty <= 0:
        return None, "qty must be > 0"
    limit_price = _f(p.get("limit_price"))
    if otype == "limit" and (limit_price is None or limit_price <= 0):
        return None, "limit order requires limit_price > 0"
    if tif not in ("gtc", "ioc"):
        return None, "tif must be gtc or ioc"
    if not isinstance(coid, str) or not COID_RE.match(coid):
        return None, "client_order_id must match ^[A-Za-z0-9_-]{1,48}$"
    symbol = p.get("symbol")
    if not symbol or not isinstance(symbol, str):
        return None, "symbol required"
    return {"side": side, "type": otype, "tif": tif, "qty": qty, "limit_price": limit_price,
            "client_order_id": coid, "symbol": symbol}, None


def execute_submit(r, p: dict) -> dict:
    p = p or {}
    account = "live" if str(p.get("account") or "paper").lower() == "live" else "paper"
    live = account == "live"
    v, err = _validate_submit(p)
    coid = p.get("client_order_id")
    if err:
        return {"ok": False, "status": "rejected", "order_id": None, "client_order_id": coid,
                "order": None, "error": err}
    coid = v["client_order_id"]
    rep_kw = dict(account=account, source=p.get("source"), algo_id=p.get("algo_id"),
                  slice_idx=p.get("slice_idx"), book=p.get("book"))

    def _ok(order, status):
        order = trader._to_dict(order) if not isinstance(order, dict) else order
        norm = runtime.normalize_order(order or {})
        publish_report(r, "ems_submitted", order, client_order_id=coid, **rep_kw)
        return {"ok": True, "status": status, "order_id": norm.get("order_id"),
                "client_order_id": coid, "order": order, "error": None}

    def _fail(status, error):
        return {"ok": False, "status": status, "order_id": None, "client_order_id": coid,
                "order": None, "error": error}

    last_exc = None
    attempts = len(RETRY_DELAYS) + 1
    for attempt in range(attempts):
        try:
            if v["type"] == "limit":
                res = trader.submit_limit(v["symbol"], v["qty"], v["side"], v["limit_price"], coid,
                                          live=live, tif=v["tif"])
            else:
                res = trader.submit_market(v["symbol"], v["qty"], v["side"], coid, live=live,
                                           tif=v["tif"])
            return _ok(res, "acked")
        except APIError as e:
            sc = _status_code(e)
            text = _err_text(e)
            if sc == 422 and "client_order_id" in (text + " " + str(e)).lower():
                try:
                    existing = trader.get_order_by_client_id(coid, live)
                except Exception as e2:
                    return _fail("rejected", f"{text}; lookup failed: {type(e2).__name__}: {e2}")
                if existing:
                    return _ok(existing, "already_submitted")
                return _fail("rejected", text)
            if trader.is_transient(e):
                last_exc = e
            else:
                return _fail("rejected", text)
        except Exception as e:
            if isinstance(e, RuntimeError) or not trader.is_transient(e):
                return _fail("error", f"{type(e).__name__}: {e}")
            last_exc = e
        if attempt < len(RETRY_DELAYS):
            log.warning("submit coid=%s transient error (attempt %d): %s", coid, attempt + 1, last_exc)
            _sleep(RETRY_DELAYS[attempt])

    try:
        existing = trader.get_order_by_client_id(coid, live)
    except Exception as e2:
        existing = None
        log.warning("lookup coid=%s after retries failed: %s", coid, e2)
    if existing:
        return _ok(existing, "acked")
    return _fail("unknown", f"{type(last_exc).__name__}: {last_exc}" if last_exc else "unknown")


# --------------------------------------------------------------------------- cancel
def execute_cancel(r, p: dict) -> dict:
    p = p or {}
    account = "live" if str(p.get("account") or "paper").lower() == "live" else "paper"
    live = account == "live"
    order_id = p.get("order_id")
    coid = p.get("client_order_id")
    try:
        if not order_id and coid and r is not None:
            try:
                order_id = runtime.redis_call(r.get, runtime.coid_key(coid))
            except Exception as e:
                log.warning("coid lookup in redis failed: %s", e)
                order_id = None
            if order_id and str(order_id).startswith("int-"):
                order_id = None
        if not order_id and coid:
            existing = trader.get_order_by_client_id(coid, live)
            if existing:
                order_id = existing.get("id") or existing.get("order_id")
    except Exception as e:
        return {"ok": False, "status": "error", "order_id": None, "client_order_id": coid,
                "error": f"{type(e).__name__}: {e}"}
    if not order_id:
        return {"ok": False, "status": "not_found", "order_id": None, "client_order_id": coid,
                "error": "order not found"}
    order_id = str(order_id)

    last_exc = None
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            trader.cancel(order_id, live)
            publish_report(r, "ems_cancel_requested", None, account=account, order_id=order_id,
                           client_order_id=coid, source=p.get("source"))
            return {"ok": True, "status": "cancel_requested", "order_id": order_id,
                    "client_order_id": coid, "error": None}
        except APIError as e:
            sc = _status_code(e)
            if sc in (404, 422):
                return {"ok": False, "status": "not_found", "order_id": order_id,
                        "client_order_id": coid, "error": _err_text(e)}
            if not trader.is_transient(e):
                return {"ok": False, "status": "error", "order_id": order_id,
                        "client_order_id": coid, "error": _err_text(e)}
            last_exc = e
        except Exception as e:
            if isinstance(e, RuntimeError) or not trader.is_transient(e):
                return {"ok": False, "status": "error", "order_id": order_id,
                        "client_order_id": coid, "error": f"{type(e).__name__}: {e}"}
            last_exc = e
        if attempt < len(RETRY_DELAYS):
            log.warning("cancel %s transient error (attempt %d): %s", order_id, attempt + 1, last_exc)
            _sleep(RETRY_DELAYS[attempt])
    return {"ok": False, "status": "error", "order_id": order_id, "client_order_id": coid,
            "error": f"{type(last_exc).__name__}: {last_exc}" if last_exc else "cancel failed"}


# --------------------------------------------------------------------------- dispatch
def handle_request(r, payload: dict) -> dict:
    payload = payload or {}
    cmd_id = payload.get("cmd_id") or payload.get("req_id")
    kind = payload.get("kind")
    try:
        if kind == "submit":
            reply = execute_submit(r, payload)
            log.info("submit coid=%s %s %s qty=%s px=%s -> %s%s", payload.get("client_order_id"),
                     payload.get("symbol"), payload.get("side"), payload.get("qty"),
                     payload.get("limit_price"), reply.get("status"),
                     f" ({reply.get('error')})" if reply.get("error") else "")
        elif kind == "cancel_order":
            reply = execute_cancel(r, payload)
            log.info("cancel order_id=%s coid=%s -> %s%s", payload.get("order_id"),
                     payload.get("client_order_id"), reply.get("status"),
                     f" ({reply.get('error')})" if reply.get("error") else "")
        else:
            reply = {"ok": False, "status": "error", "error": "unknown kind"}
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
    n = 0
    for eid, fields in entries:
        payload = None
        try:
            payload = _parse_entry(fields)
            handle_request(r, payload)
            reader.ack(eid)
            n += 1
        except Exception as e:
            log.exception("request %s failed", eid)
            try:
                reader.dead_letter(eid, fields, f"{type(e).__name__}: {e}")
            except Exception:
                log.exception("dead_letter failed for %s", eid)
            cmd_id = (payload or {}).get("cmd_id") or (payload or {}).get("req_id")
            if cmd_id:
                try:
                    runtime.send_reply(r, cmd_id, {"ok": False, "status": "error", "cmd_id": cmd_id,
                                                   "error": f"{type(e).__name__}: {e}"})
                except Exception:
                    pass
    return len(entries)


def body() -> None:
    r = runtime.get_redis()
    consumer = os.environ.get("EMS_CONSUMER", "ems-1")
    reader = runtime.GroupReader(r, runtime.STREAM_EMS_REQUESTS, "ems", consumer, block_ms=500)
    log.info("ems started consumer=%s stream=%s", consumer, runtime.STREAM_EMS_REQUESTS)
    while True:
        n = process_requests(r, reader)
        if not n:
            time.sleep(0.02)


def main() -> None:
    runtime.run_forever(body, "ems")


if __name__ == "__main__":
    main()
