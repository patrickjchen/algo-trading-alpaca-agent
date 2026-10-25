You are generating: subscriber.py — market data websocket → Redis.

REQUIREMENTS
- `from alpaca.data.live.crypto import CryptoDataStream`; `import runtime`.
- `main()` parses argv symbols (default the SYMBOLS given in the task; also honour env `SYMBOLS` comma-separated), reads `ALPACA_API_KEY_ID`/`ALPACA_API_SECRET_KEY` (exit 2 with a clear message if missing), then `runtime.run_forever(body, "subscriber")` where `body` is an `async def` that creates a fresh `r = runtime.get_redis()`, a `SpillBuffer`, the stream (with creds), subscribes trades+quotes+orderbooks for all symbols and awaits `stream._run_forever()` in try/finally `await stream.close()`.
- Handlers (`async def`, never raise, log exceptions):
  * trade → `HSET alpaca:latest:<SYM>` {price, size, timestamp} merged; `LPUSH alpaca:recent:<SYM>:trade json` + `LTRIM 0 99`, entry `{price, size, timestamp, exchange, conditions, id}`; `SADD alpaca:symbols`.
  * quote → `HSET alpaca:latest:<SYM>` {bid_price, bid_size, ask_price, ask_size, timestamp}; `LPUSH alpaca:recent:<SYM>:quote` + LTRIM 99.
  * orderbook → hybrid reset/delta into `alpaca:ob:bids:<SYM>` / `alpaca:ob:asks:<SYM>` exactly per the durable rules, plus metadata `HSET alpaca:orderbook:<SYM> {symbol, timestamp, levels_bids, levels_asks, reset}`; `SADD alpaca:symbols`.
  All writes go through a pipeline executed via `runtime.redis_call(pipe.execute, buffer=buf)`.
- Also keep a tick counter and log a one-line stats summary every 30s (trades/quotes/books per symbol, buffer length). Datetimes → ISO strings; `json.dumps(default=str)`; `_clean()` Nones before HSET.
- `if __name__ == "__main__": main()`. No top-level side effects. Call `mcp__verify__verify_stage(stage="subscriber")`.
