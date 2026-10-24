"""Trading websocket -> Redis Stream ``alpaca:trade_updates``.

One TradingStream connection per account (Alpaca allows only one). Every update is
XADDed to the stream (consumed by OMS) and LPUSHed to a short recent list for the GUI.
Handlers never raise; Redis writes go through runtime.redis_call with a SpillBuffer.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

from alpaca.trading.stream import TradingStream

import runtime
import trader

NAME = "trade_stream"
RECENT_KEY = "alpaca:recent:trade_updates"
STREAM_MAXLEN = 100000
RECENT_MAX = 100

log = runtime.log(NAME)

# Per-incarnation context (filled by body(); empty at import time).
_CTX: dict = {}


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _val(x):
    if x is None:
        return None
    v = getattr(x, "value", x)
    return str(v)


def _get(obj, name, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def build_payload(update, account: str = "paper") -> dict:
    """Turn a TradeUpdate (model or dict) into the stream payload dict."""
    order = _get(update, "order")
    order_d = trader._to_dict(order) if order is not None else {}
    if not isinstance(order_d, dict):
        order_d = {}
    ts = _get(update, "timestamp")
    if isinstance(ts, datetime):
        ts = ts.isoformat()
    elif ts:
        ts = str(ts)
    else:
        ts = runtime.now_iso()
    exec_id = _get(update, "execution_id")
    oid = _get(order, "id") if order is not None else None
    if oid is None:
        oid = order_d.get("id")
    return {
        "event": _val(_get(update, "event")),
        "execution_id": str(exec_id) if exec_id is not None else None,
        "order_id": str(oid) if oid is not None else None,
        "client_order_id": order_d.get("client_order_id"),
        "symbol": order_d.get("symbol"),
        "side": _val(order_d.get("side")),
        "price": _f(_get(update, "price")),
        "qty": _f(_get(update, "qty")),
        "position_qty": _f(_get(update, "position_qty")),
        "ts": ts,
        "account": account,
        "order": order_d,
    }


def publish(r, payload: dict, buf=None):
    data = json.dumps(payload, default=str)
    pipe = r.pipeline()
    pipe.xadd(runtime.STREAM_TRADE_UPDATES, {"json": data}, maxlen=STREAM_MAXLEN, approximate=True)
    pipe.lpush(RECENT_KEY, data)
    pipe.ltrim(RECENT_KEY, 0, RECENT_MAX - 1)
    return runtime.redis_call(pipe.execute, buffer=buf)


async def on_trade_update(update) -> None:
    try:
        account = _CTX.get("account", "paper")
        payload = build_payload(update, account)
        r = _CTX.get("r")
        if r is None:
            log.error("no redis client; dropping %s", payload.get("event"))
            return
        try:
            publish(r, payload, _CTX.get("buf"))
        except Exception as e:
            log.error("redis write failed for %s order=%s: %s", payload.get("event"),
                      payload.get("order_id"), e)
        log.info("%s order=%s %s %s qty=%s px=%s", payload.get("event"),
                 (payload.get("order_id") or "")[:8], payload.get("symbol"),
                 payload.get("side"), payload.get("qty"), payload.get("price"))
    except Exception:
        log.exception("trade update handler failed")


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="Alpaca trade_updates -> Redis stream")
    p.add_argument("--live", action="store_true", help="use the LIVE account (default paper)")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = _parse_args(argv)
    live = bool(args.live) or os.environ.get("ALPACA_LIVE", "") == "1"
    key = os.environ.get("ALPACA_API_KEY_ID")
    secret = os.environ.get("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        print("ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY not set", file=sys.stderr)
        sys.exit(2)
    account = "live" if live else "paper"
    if live:
        log.warning("LIVE account trade stream")

    async def body():
        r = runtime.get_redis()
        buf = runtime.SpillBuffer()
        _CTX.update({"r": r, "buf": buf, "account": account})
        stream = TradingStream(key, secret, paper=not live)
        stream.subscribe_trade_updates(on_trade_update)
        log.info("trade stream starting account=%s", account)
        try:
            await stream._run_forever()
        finally:
            try:
                await stream.close()
            except Exception:
                log.exception("stream close failed")
            if len(buf):
                try:
                    buf.flush()
                except Exception:
                    pass

    runtime.run_forever(body, NAME)


if __name__ == "__main__":
    main()
