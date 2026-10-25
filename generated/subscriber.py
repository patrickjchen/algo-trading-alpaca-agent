"""Market data subscriber: Alpaca CryptoDataStream -> Redis market keys.

Keys written:
  alpaca:latest:<SYM>            hash  last trade price/size + top-of-book quote
  alpaca:recent:<SYM>:trade      list  last 100 trades (JSON)
  alpaca:recent:<SYM>:quote      list  last 100 quotes (JSON)
  alpaca:ob:bids:<SYM>           hash  price -> size (hybrid snapshot/delta L2)
  alpaca:ob:asks:<SYM>           hash  price -> size
  alpaca:orderbook:<SYM>         hash  book metadata
  alpaca:symbols                 set   symbols seen
"""
from __future__ import annotations

import asyncio
import collections
import json
import os
import sys
from datetime import datetime

from alpaca.data.live.crypto import CryptoDataStream

import runtime

NAME = "subscriber"
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
RECENT_MAX = 100
STATS_INTERVAL_S = 30.0

log = runtime.log(NAME)


def _iso(ts) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    return str(ts)


def _clean(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _fmt_price(p) -> str:
    return f"{float(p):.10g}"


def _num(x):
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def parse_symbols(argv: list[str] | None = None) -> list[str]:
    argv = list(sys.argv[1:] if argv is None else argv)
    syms: list[str] = []
    for a in argv:
        if a.startswith("-"):
            continue
        syms.extend(s.strip().upper() for s in a.split(",") if s.strip())
    if not syms:
        env = os.environ.get("SYMBOLS", "")
        syms = [s.strip().upper() for s in env.split(",") if s.strip()]
    if not syms:
        syms = list(DEFAULT_SYMBOLS)
    seen, out = set(), []
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


class Stats:
    def __init__(self):
        self.trades = collections.Counter()
        self.quotes = collections.Counter()
        self.books = collections.Counter()
        self.errors = 0

    def summary(self, buf: runtime.SpillBuffer | None) -> str:
        syms = sorted(set(self.trades) | set(self.quotes) | set(self.books))
        parts = [f"{s}:t={self.trades[s]},q={self.quotes[s]},b={self.books[s]}" for s in syms]
        blen = len(buf) if buf is not None else 0
        dropped = buf.dropped if buf is not None else 0
        return (f"stats {' '.join(parts) or 'no-ticks'} errors={self.errors} "
                f"buffer={blen} dropped={dropped}")


def make_handlers(r, buf: runtime.SpillBuffer, stats: Stats):
    """Build the three async handlers bound to a Redis client and spill buffer."""

    async def on_trade(t) -> None:
        try:
            sym = t.symbol
            ts = _iso(getattr(t, "timestamp", None))
            price = _num(getattr(t, "price", None))
            size = _num(getattr(t, "size", None))
            exch = getattr(t, "exchange", None)
            conds = getattr(t, "conditions", None) or []
            if isinstance(conds, str):
                conds = [conds]
            entry = {"price": price, "size": size, "timestamp": ts,
                     "exchange": str(exch) if exch is not None else None,
                     "conditions": list(conds), "id": getattr(t, "id", None)}
            pipe = r.pipeline(transaction=False)
            latest = _clean({"price": price, "size": size, "timestamp": ts,
                             "trade_timestamp": ts, "symbol": sym})
            if latest:
                pipe.hset(f"alpaca:latest:{sym}", mapping=latest)
            key = f"alpaca:recent:{sym}:trade"
            pipe.lpush(key, json.dumps(entry, default=str))
            pipe.ltrim(key, 0, RECENT_MAX - 1)
            pipe.sadd("alpaca:symbols", sym)
            runtime.redis_call(pipe.execute, buffer=buf)
            stats.trades[sym] += 1
        except Exception:
            stats.errors += 1
            log.exception("trade handler error")

    async def on_quote(q) -> None:
        try:
            sym = q.symbol
            ts = _iso(getattr(q, "timestamp", None))
            entry = {"bid_price": _num(getattr(q, "bid_price", None)),
                     "bid_size": _num(getattr(q, "bid_size", None)),
                     "ask_price": _num(getattr(q, "ask_price", None)),
                     "ask_size": _num(getattr(q, "ask_size", None)),
                     "timestamp": ts}
            pipe = r.pipeline(transaction=False)
            latest = _clean({**entry, "quote_timestamp": ts, "symbol": sym})
            if latest:
                pipe.hset(f"alpaca:latest:{sym}", mapping=latest)
            key = f"alpaca:recent:{sym}:quote"
            pipe.lpush(key, json.dumps(entry, default=str))
            pipe.ltrim(key, 0, RECENT_MAX - 1)
            pipe.sadd("alpaca:symbols", sym)
            runtime.redis_call(pipe.execute, buffer=buf)
            stats.quotes[sym] += 1
        except Exception:
            stats.errors += 1
            log.exception("quote handler error")

    async def on_orderbook(book) -> None:
        try:
            sym = book.symbol
            ts = _iso(getattr(book, "timestamp", None))
            reset = bool(getattr(book, "reset", False))
            bids = getattr(book, "bids", None) or []
            asks = getattr(book, "asks", None) or []
            bkey, akey = f"alpaca:ob:bids:{sym}", f"alpaca:ob:asks:{sym}"
            pipe = r.pipeline(transaction=True)
            if reset:
                pipe.delete(bkey, akey)
            for key, levels in ((bkey, bids), (akey, asks)):
                upserts: dict = {}
                removes: list = []
                for lvl in levels:
                    p = getattr(lvl, "price", None)
                    s = getattr(lvl, "size", None)
                    if p is None:
                        continue
                    field = _fmt_price(p)
                    sz = _num(s) or 0.0
                    if sz > 0:
                        upserts[field] = f"{sz:.10g}"
                        if field in removes:
                            removes.remove(field)
                    else:
                        upserts.pop(field, None)
                        if not reset:
                            removes.append(field)
                if upserts:
                    pipe.hset(key, mapping=upserts)
                if removes:
                    pipe.hdel(key, *removes)
            meta = _clean({"symbol": sym, "timestamp": ts, "levels_bids": len(bids),
                           "levels_asks": len(asks), "reset": int(reset)})
            pipe.hset(f"alpaca:orderbook:{sym}", mapping=meta)
            pipe.sadd("alpaca:symbols", sym)
            runtime.redis_call(pipe.execute, buffer=buf)
            stats.books[sym] += 1
        except Exception:
            stats.errors += 1
            log.exception("orderbook handler error")

    return on_trade, on_quote, on_orderbook


async def _stats_loop(stats: Stats, buf: runtime.SpillBuffer) -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_S)
        try:
            if len(buf):
                try:
                    buf.flush()
                except Exception:
                    pass
            log.info(stats.summary(buf))
        except Exception:
            log.exception("stats loop error")


def make_body(symbols: list[str], key: str, secret: str):
    async def body() -> None:
        r = runtime.get_redis()
        buf = runtime.SpillBuffer()
        stats = Stats()
        stream = CryptoDataStream(key, secret)
        on_trade, on_quote, on_book = make_handlers(r, buf, stats)
        stream.subscribe_trades(on_trade, *symbols)
        stream.subscribe_quotes(on_quote, *symbols)
        stream.subscribe_orderbooks(on_book, *symbols)
        log.info("subscribing trades/quotes/orderbooks for %s", ",".join(symbols))
        stats_task = asyncio.create_task(_stats_loop(stats, buf))
        try:
            await stream._run_forever()
        finally:
            stats_task.cancel()
            try:
                await stream.close()
            except Exception:
                log.exception("stream close failed")
            log.info(stats.summary(buf))
    return body


def main() -> None:
    symbols = parse_symbols()
    key = os.environ.get("ALPACA_API_KEY_ID")
    secret = os.environ.get("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        sys.stderr.write("subscriber: ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY must be set\n")
        sys.exit(2)
    log.info("subscriber starting symbols=%s", symbols)
    runtime.run_forever(make_body(symbols, key, secret), NAME)


if __name__ == "__main__":
    main()
