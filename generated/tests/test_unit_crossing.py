"""crossing.py: central liquidity book (fakeredis; EMS forwarder captured)."""
import json

import pytest

import crossing
import runtime

SYM = "BTC/USD"
QUOTE = {"bid_price": 100.0, "ask_price": 101.0}


@pytest.fixture
def fwd(monkeypatch, fake_redis):
    monkeypatch.setenv("CROSS_HOLD_S", "60")
    monkeypatch.delenv("FEE_BPS", raising=False)
    monkeypatch.delenv("CROSS_ENABLED", raising=False)
    calls = []
    state = {"submit": None, "cancel_order": None, "quote": dict(QUOTE)}

    def fake_forward(r, payload, timeout=10.0):
        calls.append(dict(payload))
        rep = state.get(payload.get("kind"))
        if rep is not None:
            return dict(rep)
        if payload.get("kind") == "submit":
            coid = payload["client_order_id"]
            return {"ok": True, "status": "acked", "order_id": "x-" + coid, "client_order_id": coid,
                    "order": {"id": "x-" + coid, "client_order_id": coid, "status": "accepted"}}
        return {"ok": True, "status": "cancel_requested", "order_id": payload.get("order_id")}

    def quote(sym, r=None):
        if state["quote"] is None:
            raise RuntimeError("no quote")
        return dict(state["quote"])

    def boom(*a, **k):
        raise AssertionError("crossing must never call trader order endpoints")

    monkeypatch.setattr(crossing, "forward_to_ems", fake_forward)
    monkeypatch.setattr(crossing.trader, "latest_quote", quote)
    monkeypatch.setattr(crossing.trader, "get_asset_rules",
                        lambda sym, live=False, r=None: {"symbol": sym, "min_order_size": 0.0001,
                                                         "min_trade_increment": 0.0001,
                                                         "price_increment": 0.01, "tradable": True})
    for name in ("submit_limit", "submit_market", "cancel", "cancel_all"):
        monkeypatch.setattr(crossing.trader, name, boom)
    return calls, state


_n = [0]


def _accept(fake, side="buy", qty=1.0, px=100.0, typ="limit", book="A", coid=None, **kw):
    _n[0] += 1
    p = {"cmd_id": f"c{_n[0]}", "kind": "submit", "account": "paper",
         "client_order_id": coid or f"tc-{side}-{_n[0]}", "symbol": SYM, "side": side, "type": typ,
         "qty": qty, "limit_price": px if typ == "limit" else None, "tif": "gtc", "source": "test",
         "book": book, "hold_s": 60}
    p.update(kw)
    return crossing.accept(fake, p)


def _events(fake, event=None):
    out = [json.loads(f["json"]) for _, f in fake.xrange(runtime.STREAM_TRADE_UPDATES)]
    return [e for e in out if event is None or e["event"] == event]


def _h(fake, coid):
    return fake.hgetall(runtime.book_order_key(coid))


def test_module_has_no_sqlite():
    assert not hasattr(crossing, "sqlite3")


def test_accept_rests(fake_redis, fwd):
    calls, _ = fwd
    rep = _accept(fake_redis, coid="tc-b1")
    assert rep["ok"] is True and rep["status"] == "acked"
    assert rep["venue"] == "internal" and rep["crossed_qty"] == 0
    assert rep["order_id"] == "int-tc-b1"
    h = _h(fake_redis, "tc-b1")
    assert h["status"] == "resting"
    assert fake_redis.zscore(runtime.book_key(SYM, "buy"), "tc-b1") == 100.0
    assert fake_redis.sismember(runtime.SET_RESTING, "tc-b1")
    ev = _events(fake_redis, "internal_accepted")
    assert len(ev) == 1 and ev[0]["order"]["id"] == "int-tc-b1" and ev[0]["venue"] == "internal"
    assert calls == []


def test_cross_at_midpoint(fake_redis, fwd):
    calls, _ = fwd
    _accept(fake_redis, coid="tc-b1", qty=1.0, px=100.0, book="A")
    rep = _accept(fake_redis, side="sell", coid="tc-s1", qty=0.4, px=99.0, book="B")
    assert rep["crossed_qty"] == pytest.approx(0.4) and len(rep["crosses"]) == 1
    fills = _events(fake_redis, "internal_fill")
    assert len(fills) == 2
    assert fills[0]["cross_id"] == fills[1]["cross_id"]
    assert {f["side"] for f in fills} == {"buy", "sell"}
    for f in fills:
        assert f["price"] == pytest.approx(99.5) and f["qty"] == pytest.approx(0.4)
        assert f["execution_id"].endswith("-b" if f["side"] == "buy" else "-s")
        assert f["counter_client_order_id"] == ("tc-s1" if f["side"] == "buy" else "tc-b1")
    s = _h(fake_redis, "tc-s1")
    assert s["status"] == "filled"
    assert fake_redis.zscore(runtime.book_key(SYM, "sell"), "tc-s1") is None
    assert not fake_redis.sismember(runtime.SET_RESTING, "tc-s1")
    assert fake_redis.ttl(runtime.book_order_key("tc-s1")) > 0
    b = _h(fake_redis, "tc-b1")
    assert b["status"] == "resting" and float(b["remaining"]) == pytest.approx(0.6)
    assert fake_redis.sismember(runtime.SET_RESTING, "tc-b1")
    st = fake_redis.hgetall(runtime.CROSS_STATS_KEY)
    notional = 0.4 * 99.5
    assert float(st["crosses"]) == 1
    assert float(st["crossed_qty"]) == pytest.approx(0.4)
    assert float(st["est_fee_saved"]) == pytest.approx(notional * 25 / 1e4 * 2)
    assert float(st["est_spread_saved"]) == pytest.approx(1.0 * 0.4)
    assert float(fake_redis.hget(runtime.cross_stats_key(SYM), "crosses")) == 1
    assert calls == []


def test_price_time_priority(fake_redis, fwd):
    _accept(fake_redis, side="sell", coid="tc-s99", px=99.0)
    _accept(fake_redis, side="sell", coid="tc-s98", px=98.0)
    rep = _accept(fake_redis, side="buy", coid="tc-bx", px=100.0, qty=1.0)
    assert rep["crossed_qty"] == pytest.approx(1.0)
    assert _h(fake_redis, "tc-s98")["status"] == "filled"
    assert _h(fake_redis, "tc-s99")["status"] == "resting"
    fills = _events(fake_redis, "internal_fill")
    assert fills[0]["price"] == pytest.approx(99.0)


def test_no_cross_when_prices_dont_meet(fake_redis, fwd):
    _accept(fake_redis, coid="tc-b1", px=100.0)
    rep = _accept(fake_redis, side="sell", coid="tc-s1", px=101.0)
    assert rep["crossed_qty"] == 0
    assert _h(fake_redis, "tc-s1")["status"] == "resting"
    assert _events(fake_redis, "internal_fill") == []


def test_account_mismatch(fake_redis, fwd):
    _accept(fake_redis, coid="tc-b1", px=100.0)
    rep = _accept(fake_redis, side="sell", coid="tc-s1", px=99.0, account="live")
    assert rep["crossed_qty"] == 0
    assert _events(fake_redis, "internal_fill") == []


def test_cross_deny(fake_redis, fwd):
    _accept(fake_redis, coid="tc-b1", px=100.0)
    rep = _accept(fake_redis, side="sell", coid="tc-s1", px=99.0, cross="deny")
    assert rep["crossed_qty"] == 0
    # and a resting deny order is never picked as counterparty
    rep = _accept(fake_redis, side="buy", coid="tc-b2", px=100.0)
    assert rep["crossed_qty"] == 0


def test_market_vs_limit_at_limit(fake_redis, fwd):
    calls, _ = fwd
    _accept(fake_redis, side="sell", coid="tc-s1", px=99.0, qty=1.0)
    rep = _accept(fake_redis, side="buy", coid="tc-m1", typ="market", qty=1.0)
    assert rep["crossed_qty"] == pytest.approx(1.0)
    assert _events(fake_redis, "internal_fill")[0]["price"] == pytest.approx(99.0)
    assert calls == []


def test_market_vs_market_quote_mid(fake_redis, fwd):
    # no EMS heartbeat -> market orders keep resting instead of routing
    _accept(fake_redis, side="sell", coid="tc-ms", typ="market", qty=1.0)
    rep = _accept(fake_redis, side="buy", coid="tc-mb", typ="market", qty=1.0)
    assert rep["crossed_qty"] == pytest.approx(1.0)
    assert _events(fake_redis, "internal_fill")[0]["price"] == pytest.approx(100.5)


def test_market_vs_market_no_quote(fake_redis, fwd):
    _, state = fwd
    state["quote"] = None
    _accept(fake_redis, side="sell", coid="tc-ms", typ="market", qty=1.0)
    rep = _accept(fake_redis, side="buy", coid="tc-mb", typ="market", qty=1.0)
    assert rep["crossed_qty"] == 0
    assert _events(fake_redis, "internal_fill") == []


@pytest.mark.parametrize("kw", [{"typ": "market"}, {"tif": "ioc"}, {"hold_s": 0}])
def test_immediate_route(fake_redis, fwd, kw):
    calls, _ = fwd
    runtime.Heartbeat(fake_redis, "ems").beat()
    rep = _accept(fake_redis, coid="tc-r1", qty=0.5, **kw)
    assert rep["ok"] is True
    assert len(calls) == 1
    assert calls[0]["kind"] == "submit" and calls[0]["qty"] == pytest.approx(0.5)
    assert calls[0]["client_order_id"] == "tc-r1"
    h = _h(fake_redis, "tc-r1")
    assert h["status"] == "routed" and h["routed_order_id"] == "x-tc-r1"
    assert rep["routed_order_id"] == "x-tc-r1"
    ev = _events(fake_redis, "internal_routed")
    assert len(ev) == 1 and ev[0]["order"]["status"] == "replaced"
    assert not fake_redis.sismember(runtime.SET_RESTING, "tc-r1")


def test_partial_cross_then_route_residual(fake_redis, fwd):
    calls, _ = fwd
    runtime.Heartbeat(fake_redis, "ems").beat()
    _accept(fake_redis, side="sell", coid="tc-s1", px=99.0, qty=0.3)
    rep = _accept(fake_redis, side="buy", coid="tc-m1", typ="market", qty=1.0)
    assert rep["crossed_qty"] == pytest.approx(0.3)
    assert len(calls) == 1 and calls[0]["qty"] == pytest.approx(0.7)


def test_tick_routes_after_hold(fake_redis, fwd):
    calls, _ = fwd
    runtime.Heartbeat(fake_redis, "ems").beat()
    _accept(fake_redis, coid="tc-h1")
    route_at = float(_h(fake_redis, "tc-h1")["route_at"])
    assert crossing.tick(fake_redis, now_ts=route_at - 1) == 0
    assert calls == []
    assert crossing.tick(fake_redis, now_ts=route_at + 1) == 1
    assert len(calls) == 1 and calls[0]["client_order_id"] == "tc-h1"
    assert _h(fake_redis, "tc-h1")["status"] == "routed"


def test_tick_ems_down(fake_redis, fwd):
    calls, _ = fwd
    _accept(fake_redis, coid="tc-h1")
    route_at = float(_h(fake_redis, "tc-h1")["route_at"])
    assert crossing.tick(fake_redis, now_ts=route_at + 1) == 0
    assert calls == []
    assert _h(fake_redis, "tc-h1")["status"] == "resting"


def test_route_rejected(fake_redis, fwd):
    _, state = fwd
    runtime.Heartbeat(fake_redis, "ems").beat()
    state["submit"] = {"ok": False, "status": "rejected", "error": "insufficient balance"}
    rep = _accept(fake_redis, coid="tc-rj", typ="market")
    assert rep["ok"] is False and rep["status"] == "rejected"
    assert _h(fake_redis, "tc-rj")["status"] == "rejected"
    assert len(_events(fake_redis, "internal_rejected")) == 1


def test_cancel_resting(fake_redis, fwd):
    calls, _ = fwd
    _accept(fake_redis, coid="tc-c1")
    rep = crossing.cancel(fake_redis, {"kind": "cancel_order", "client_order_id": "tc-c1"})
    assert rep["ok"] is True and rep["status"] == "cancel_requested"
    assert _h(fake_redis, "tc-c1")["status"] == "canceled"
    assert not fake_redis.sismember(runtime.SET_RESTING, "tc-c1")
    assert fake_redis.zscore(runtime.book_key(SYM, "buy"), "tc-c1") is None
    assert len(_events(fake_redis, "internal_canceled")) == 1
    assert calls == []


def test_cancel_routed_forwards(fake_redis, fwd):
    calls, _ = fwd
    runtime.Heartbeat(fake_redis, "ems").beat()
    _accept(fake_redis, coid="tc-c2", typ="market")
    calls.clear()
    rep = crossing.cancel(fake_redis, {"kind": "cancel_order", "client_order_id": "tc-c2"})
    assert rep["status"] == "cancel_requested"
    assert len(calls) == 1
    assert calls[0]["kind"] == "cancel_order" and calls[0]["order_id"] == "x-tc-c2"


def test_cancel_filled_not_found(fake_redis, fwd):
    _accept(fake_redis, coid="tc-b1", qty=1.0)
    _accept(fake_redis, side="sell", coid="tc-s1", px=99.0, qty=1.0)
    rep = crossing.cancel(fake_redis, {"kind": "cancel_order", "client_order_id": "tc-s1"})
    assert rep["status"] == "not_found" and rep["ok"] is False


def test_duplicate_accept(fake_redis, fwd):
    _accept(fake_redis, coid="tc-d1")
    rep = _accept(fake_redis, coid="tc-d1")
    assert rep["status"] == "already_submitted" and rep["ok"] is True
    assert len(_events(fake_redis, "internal_accepted")) == 1


def test_invalid_rejected(fake_redis, fwd):
    rep = _accept(fake_redis, coid="bad id!")
    assert rep["ok"] is False and rep["status"] == "rejected"


def test_handle_request_replies(fake_redis, fwd):
    rep = crossing.handle_request(fake_redis, {"cmd_id": "hr1", "kind": "submit", "account": "paper",
                                               "client_order_id": "tc-hr", "symbol": SYM, "side": "buy",
                                               "type": "limit", "qty": 1, "limit_price": 100, "hold_s": 60})
    assert rep["status"] == "acked"
    assert json.loads(fake_redis.lpop(runtime.reply_key("hr1")))["cmd_id"] == "hr1"
    assert crossing.handle_request(fake_redis, {"cmd_id": "hr2", "kind": "zzz"})["ok"] is False


def test_book_snapshot(fake_redis, fwd):
    _accept(fake_redis, coid="tc-b100", px=100.0)
    _accept(fake_redis, coid="tc-b101", px=101.0)
    _accept(fake_redis, side="sell", coid="tc-s103", px=103.0)
    _accept(fake_redis, side="sell", coid="tc-s102", px=102.0)
    snap = crossing.book_snapshot(fake_redis, SYM)
    assert [b["price"] for b in snap["bids"]] == [101.0, 100.0]
    assert [a["price"] for a in snap["asks"]] == [102.0, 103.0]
    assert snap["bids"][0]["book"] == "A"
