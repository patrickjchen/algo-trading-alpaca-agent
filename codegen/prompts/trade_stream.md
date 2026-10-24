You are generating: trade_stream.py — trading websocket → Redis Stream `alpaca:trade_updates`.

REQUIREMENTS
- `from alpaca.trading.stream import TradingStream`; `import runtime`, `import trader` (for `_to_dict`).
- `main()`: parse `--live` (also env `ALPACA_LIVE=1`); read creds (exit 2 if missing); `runtime.run_forever(body, "trade_stream")`. `body` is `async def`: fresh `r`, `SpillBuffer`, `TradingStream(key, secret, paper=not live)`, `stream.subscribe_trade_updates(on_trade_update)`, `await stream._run_forever()` in try/finally `await stream.close()`.
- `on_trade_update(update)` is `async def`, never raises. Build payload:
  `{"event": str(update.event value), "execution_id": str or None, "order_id": str(order.id), "client_order_id", "symbol", "side", "price": float|None, "qty": float|None, "position_qty": float|None, "ts": ISO (update.timestamp or now), "account": "paper"|"live", "order": trader._to_dict(update.order)}`.
  Use `.get()`/`getattr` defensively; numeric strings → float.
- Write via a pipeline: `XADD alpaca:trade_updates {"json": json.dumps(payload, default=str)} maxlen=100000 approximate=True`; `LPUSH alpaca:recent:trade_updates` + `LTRIM 0 99`; executed with `runtime.redis_call(pipe.execute, buffer=buf)`. Log one line per event: `event order=<id[:8]> <symbol> <side> qty=<qty> px=<price>`.
- Module-level: only constants and function defs. `if __name__ == "__main__": main()`. Call `mcp__verify__verify_stage(stage="trade_stream")`.
