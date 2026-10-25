import json
from types import SimpleNamespace

import pytest
from alpaca.common.exceptions import APIError

import ems
import runtime


def _api_error(code, msg="x"):
    return APIError(json.dumps({"code": code * 100000, "message": msg}),
                    SimpleNamespace(response=SimpleNamespace(status_code=code), request=None))


ORDER = {"id": "o1", "client_order_id": "algo-abc-001", "symbol": "BTC/USD", "side": "buy",
         "type": "limit", "qty": "0.01", "limit_price": "100", "status": "accepted",
         "time_in_force": "gtc"}


def _submit(**kw):
    p = {"cmd_id": "c1", "kind": "submit", "account": "paper", "client_order_id": "algo-abc-001",
         "symbol": "BTC/USD", "side": "buy", "type": "limit", "qty": 0.01, "limit_price": 100.0,
         "tif": "gtc", "source": "algo", "algo_id": "abc", "slice_idx": 1}
    p.update(kw)
    return p


def _reports(fake):
    return [json.loads(f["json"]) for _, f in fake.xrange(runtime.STREAM_TRADE_UPDATES)]


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(ems, "_sleep", lambda s: None)


def test_no_sqlite():
    assert not hasattr(ems, "sqlite3")


def test_submit_limit_acked(fake_redis, monkeypatch):
    seen = []

    def fake_limit(symbol, qty, side, px, coid, live=False, tif="gtc"):
        seen.append((symbol, qty, side, px, coid, live, tif))
        return dict(ORDER)

    monkeypatch.setattr(ems.trader, "submit_limit", fake_limit)
    rep = ems.handle_request(fake_redis, _submit())
    assert rep["ok"] is True and rep["status"] == "acked" and rep["order_id"] == "o1"
    assert rep["order"]["id"] == "o1"
    assert seen == [("BTC/USD", 0.01, "buy", 100.0, "algo-abc-001", False, "gtc")]
    reps = _reports(fake_redis)
    assert len(reps) == 1
    r0 = reps[0]
    assert r0["event"] == "ems_submitted" and r0["client_order_id"] == "algo-abc-001"
    assert r0["source"] == "algo" and r0["algo_id"] == "abc" and r0["slice_idx"] == 1
    assert r0["order"]["id"] == "o1" and r0["execution_id"] is None
    assert json.loads(fake_redis.lindex(runtime.reply_key("c1"), 0))["status"] == "acked"


def test_submit_duplicate_coid(fake_redis, monkeypatch):
    def fake_limit(*a, **k):
        raise _api_error(422, "client_order_id must be unique")

    monkeypatch.setattr(ems.trader, "submit_limit", fake_limit)
    monkeypatch.setattr(ems.trader, "get_order_by_client_id", lambda cid, live=False: dict(ORDER))
    rep = ems.handle_request(fake_redis, _submit())
    assert rep["ok"] is True and rep["status"] == "already_submitted"


def test_submit_transient_then_unknown(fake_redis, monkeypatch):
    n = {"calls": 0}

    def fake_limit(*a, **k):
        n["calls"] += 1
        raise ConnectionError("net down")

    monkeypatch.setattr(ems.trader, "submit_limit", fake_limit)
    monkeypatch.setattr(ems.trader, "is_transient", lambda e: True)
    monkeypatch.setattr(ems.trader, "get_order_by_client_id", lambda cid, live=False: None)
    rep = ems.handle_request(fake_redis, _submit())
    assert rep["ok"] is False and rep["status"] == "unknown"
    assert n["calls"] == 4
    assert _reports(fake_redis) == []


def test_submit_no_creds_error(fake_redis, monkeypatch):
    def fake_market(*a, **k):
        raise RuntimeError("ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY not set")

    monkeypatch.setattr(ems.trader, "submit_market", fake_market)
    rep = ems.handle_request(fake_redis, _submit(type="market", limit_price=None))
    assert rep["ok"] is False and rep["status"] == "error"


def test_submit_invalid(fake_redis, monkeypatch):
    monkeypatch.setattr(ems.trader, "submit_limit", lambda *a, **k: pytest.fail("called"))
    rep = ems.handle_request(fake_redis, _submit(qty=0))
    assert rep["ok"] is False and rep["status"] == "rejected"
    rep = ems.handle_request(fake_redis, _submit(tif="day"))
    assert rep["status"] == "rejected"


def test_cancel_by_coid(fake_redis, monkeypatch):
    fake_redis.set(runtime.coid_key("tc-1"), "o9")
    cancelled = []
    monkeypatch.setattr(ems.trader, "cancel", lambda oid, live=False: cancelled.append(oid))
    rep = ems.handle_request(fake_redis, {"cmd_id": "k1", "kind": "cancel_order", "account": "paper",
                                          "client_order_id": "tc-1", "source": "test"})
    assert rep["ok"] is True and rep["status"] == "cancel_requested" and rep["order_id"] == "o9"
    assert cancelled == ["o9"]
    reps = _reports(fake_redis)
    assert len(reps) == 1 and reps[0]["event"] == "ems_cancel_requested"
    assert reps[0]["order_id"] == "o9"


def test_cancel_not_found(fake_redis, monkeypatch):
    monkeypatch.setattr(ems.trader, "cancel", lambda *a, **k: pytest.fail("called"))
    monkeypatch.setattr(ems.trader, "get_order_by_client_id", lambda cid, live=False: None)
    rep = ems.handle_request(fake_redis, {"cmd_id": "k2", "kind": "cancel_order",
                                          "client_order_id": "tc-missing"})
    assert rep["ok"] is False and rep["status"] == "not_found"


def test_unknown_kind(fake_redis):
    rep = ems.handle_request(fake_redis, {"cmd_id": "k3", "kind": "explode"})
    assert rep["ok"] is False
