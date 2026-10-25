"""EMS worker: the exchange gateway. The ONLY process calling Alpaca order endpoints (via trader.py).

Serves submit / cancel_order requests from OMS on ``alpaca:ems:requests`` (group ``ems``), replies on
``alpaca:reply:<cmd_id>`` and publishes an EXECUTION REPORT to ``alpaca:trade_updates`` for each
successful exchange action. No algo logic, no SQLite. No I/O at import time.
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
_sleep = time.sleep


# --------------------------------------------------------------------------- helpers
def _status_code(exc):
    try:
        return exc.status_code
    except Exception:
        return None


def _err_text(exc) -> str:
    msg = None
    if isinstance(exc, APIError):
        try:
            msg = exc.message
        except Exception:
            msg = None
    return str(msg or exc)[:500]


def _float(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _is_transient(exc) -> bool:
    if isinstance(exc, RuntimeError):
        return False
    try:
        return bool(trader.is_transient(exc))
    except Exception:
        return False


def _order_dict(res):
    if res is None:
        return None
    if isinstance(res, dict):
        return res
    try:
        d = trader._to_dict(res)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


# --------------------------------------------------------------------------- reports
def publish_report(r, event: str, order: dict | None, *, account, client_order_id=None, order_id=None,
                   symbol=None, side=None, qty=None, source=None, algo_id=None, slice_idx=None) -> None:
    try:
        o = order or {}
        if order_id is None:
            order_id = o.get("id") or o.get("order_id")
        if client_order_id is None:
            client_order_id = o.get("client_order_id")
        if symbol is None:
            symbol = o.get("symbol")
        if side is None:
            side = runtime._enum_str(o.get("side"))
        if qty is None:
            qty = _float(o.get("qty"))
        payload = {
            "event": event, "execution_id": None,
            "order_id": str(order_id) if order_id is not None else None,
            "client_order_id": client_order_id, "symbol": symbol, "side": side,
            "price": None, "qty": qty, "position_qty": None, "ts": runtime.now_iso(),
            "account": account, "order": order, "source": source, "algo_id": algo_id,
            "slice_idx": slice_idx,
        }
        if r is None:
            return
        runtime.publish(r, runtime.STREAM_TRADE_UPDATES, payload)
    except Exception as e:  # never raise
        log.error("publish_report %s failed: %s", event, e)


# --------------------------------------------------------------------------- submit
def _validate_submit(p: dict):
    side = str(p.get("side") or "").lower()
    typ = str(p.get("type") or "").lower()
    tif = str(p.get("tif") or p.get("time_in_force") or "gtc").lower()
    coid = p.get("client_order_id")
    qty = _float(p.get("qty"))
    lp = _float(p.get("limit_price"))
    if side not in ("buy", "sell"):
        return "side must be buy or sell", None
    if typ not in ("market", "limit"):
        return "type must be market or limit", None
    if qty is None or qty <= 0:
        return "qty must be > 0", None
    if typ == "limit" and (lp is None or lp <= 0):
        return "limit order requires limit_price > 0", None
    if tif not in ("gtc", "ioc"):
        return "tif must be gtc or ioc", None
    if not isinstance(coid, str) or not COID_RE.match(coid):
        return "client_order_id must match ^[A-Za-z0-9_-]{1,48}$", None
    if not p.get("symbol"):
        return "symbol required", None
    return None, {"side": side, "type": typ, "tif": tif, "coid": coid, "qty": qty,
                  "limit_price": lp if typ == "limit" else None, "symbol": str(p["symbol"])}


def execute_submit(r, p: dict) -> dict:
    p = p or {}
    coid = p.get("client_order_id")
    err, v = _validate_submit(p)
    if err:
        return {"ok": False, "status": "rejected", "order_id": None, "client_order_id": coid,
                "order": None, "error": err}
    account = "live" if p.get("account") == "live" else "paper"
    live = account == "live"
    source, algo_id, slice_idx = p.get("source"), p.get("algo_id"), p.get("slice_idx")
    coid = v["coid"]

    def _ok(status, order):
        publish_report(r, "ems_submitted", order, account=account, source=source,
                       algo_id=algo_id, slice_idx=slice_idx)
        return {"ok": True, "status": status, "order_id": str(order.get("id") or order.get("order_id")),
                "client_order_id": order.get("client_order_id") or coid, "order": order, "error": None}

    def _fail(status, error):
        return {"ok": False, "status": status, "order_id": None, "client_order_id": coid,
                "order": None, "error": error}

    def _lookup():
        return _order_dict(trader.get_order_by_client_id(coid, live))

    attempt = 0
    while True:
        try:
            if v["type"] == "limit":
                res = trader.submit_limit(v["symbol"], v["qty"], v["side"], v["limit_price"], coid,
                                          live=live, tif=v["tif"])
            else:
                res = trader.submit_market(v["symbol"], v["qty"], v["side"], coid, live=live, tif=v["tif"])
            order = _order_dict(res) or {}
            return _ok("acked", order)
        except Exception as e:
            sc = _status_code(e) if isinstance(e, APIError) else None
            if isinstance(e, APIError) and sc == 422 and "client_order_id" in str(e).lower():
                try:
                    found = _lookup()
                except Exception as e2:
                    return _fail("rejected", f"{_err_text(e)}; lookup failed: {e2}")
                if found:
                    return _ok("already_submitted", found)
                return _fail("rejected", _err_text(e))
            if _is_transient(e):
                if attempt < len(RETRY_DELAYS):
                    log.warning("submit coid=%s transient error (attempt %d): %s", coid, attempt + 1, e)
                    _sleep(RETRY_DELAYS[attempt])
                    attempt += 1
                    continue
                try:
                    found = _lookup()
                except Exception as e2:
                    found = None
                    log.warning("submit coid=%s lookup after retries failed: %s", coid, e2)
                if found:
                    return _ok("acked", found)
                return _fail("unknown", f"{type(e).__name__}: {_err_text(e)}")
            if isinstance(e, APIError) and sc is not None and 400 <= sc < 500:
                return _fail("rejected", _err_text(e))
            return _fail("error", f"{type(e).__name__}: {e}")


# --------------------------------------------------------------------------- cancel
def execute_cancel(r, p: dict) -> dict:
    p = p or {}
    account = "live" if p.get("account") == "live" else "paper"
    live = account == "live"
    coid = p.get("client_order_id")
    source = p.get("source")
    order_id = p.get("order_id")
    try:
        if not order_id and coid and r is not None:
            try:
                order_id = runtime.redis_call(r.get, runtime.coid_key(coid))
            except Exception as e:
                log.warning("coid lookup in redis failed: %s", e)
        if not order_id and coid:
            found = _order_dict(trader.get_order_by_client_id(coid, live))
            if found:
                order_id = found.get("id") or found.get("order_id")
    except Exception as e:
        return {"ok": False, "status": "error", "order_id": None, "error": f"{type(e).__name__}: {e}"}
    if not order_id:
        return {"ok": False, "status": "not_found", "order_id": None, "error": "order not found"}
    order_id = str(order_id)

    attempt = 0
    while True:
        try:
            trader.cancel(order_id, live)
            publish_report(r, "ems_cancel_requested", None, account=account, order_id=order_id,
                           client_order_id=coid, source=source)
            return {"ok": True, "status": "cancel_requested", "order_id": order_id, "error": None}
        except Exception as e:
            sc = _status_code(e) if isinstance(e, APIError) else None
            if isinstance(e, APIError) and sc in (404, 422):
                return {"ok": False, "status": "not_found", "order_id": order_id, "error": _err_text(e)}
            if _is_transient(e) and attempt < len(RETRY_DELAYS):
                log.warning("cancel %s transient error (attempt %d): %s", order_id, attempt + 1, e)
                _sleep(RETRY_DELAYS[attempt])
                attempt += 1
                continue
            return {"ok": False, "status": "error", "order_id": order_id,
                    "error": f"{type(e).__name__}: {_err_text(e)}"}


# --------------------------------------------------------------------------- dispatch
def handle_request(r, payload: dict) -> dict:
    payload = payload or {}
    cmd_id = payload.get("cmd_id") or payload.get("req_id")
    kind = payload.get("kind")
    try:
        if kind == "submit":
            reply = execute_submit(r, payload)
            log.info("submit coid=%s %s %s qty=%s px=%s -> %s", payload.get("client_order_id"),
                     payload.get("symbol"), payload.get("side"), payload.get("qty"),
                     payload.get("limit_price"), reply.get("status"))
        elif kind == "cancel_order":
            reply = execute_cancel(r, payload)
            log.info("cancel order_id=%s coid=%s -> %s", payload.get("order_id"),
                     payload.get("client_order_id"), reply.get("status"))
        else:
            reply = {"ok": False, "status": "error", "error": "unknown kind"}
    except Exception as e:
        log.exception("handle_request failed")
        reply = {"ok": False, "status": "error", "error": f"{type(e).__name__}: {e}"}
    reply["cmd_id"] = cmd_id
    if r is not None and cmd_id:
        try:
            runtime.redis_call(runtime.send_reply, r, cmd_id, reply)
        except Exception as e:
            log.error("send_reply %s failed: %s", cmd_id, e)
    return reply


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
            log.exception("request %s failed", entry_id)
            try:
                reader.dead_letter(entry_id, fields, f"{type(e).__name__}: {e}")
            except Exception as e2:
                log.error("dead_letter %s failed: %s", entry_id, e2)
            cmd_id = (payload or {}).get("cmd_id") or (payload or {}).get("req_id") \
                if isinstance(payload, dict) else None
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
    consumer = os.environ.get("EMS_CONSUMER", "ems-1")
    reader = runtime.GroupReader(r, runtime.STREAM_EMS_REQUESTS, "ems", consumer, block_ms=500)
    log.info("ems started consumer=%s", consumer)
    while True:
        n = process_requests(r, reader)
        if not n:
            time.sleep(0.02)


def main():
    runtime.run_forever(body, "ems")


if __name__ == "__main__":
    main()
