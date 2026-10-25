import pytest

import runtime
import webgui


@pytest.fixture
def client(monkeypatch, fake_redis, tmp_path):
    monkeypatch.setattr(webgui, "_r", lambda: fake_redis)
    db = str(tmp_path / "missing" / "none.db")
    monkeypatch.setenv("ALPACA_DB_PATH", db)
    monkeypatch.setattr(runtime, "DB_PATH", db)
    webgui.app.config["TESTING"] = True
    return webgui.app.test_client()


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    d = r.get_json()
    assert all(w["alive"] is False for w in d["workers"].values())
    assert d["db"] is False


def test_symbols_empty(client):
    r = client.get("/api/symbols")
    assert r.status_code == 200 and r.get_json() == []


def test_orderbook_no_data(client):
    r = client.get("/api/orderbook/BTC%2FUSD")
    assert r.status_code == 200
    d = r.get_json()
    assert d["fallback"] == "no_data" and d["symbol"] == "BTC/USD"


def test_orderbook_ladder(client, fake_redis):
    fake_redis.hset("alpaca:ob:bids:BTC/USD", mapping={f"{100 + i:.10g}": "1" for i in range(15)})
    fake_redis.hset("alpaca:ob:asks:BTC/USD", mapping={f"{200 + i:.10g}": "2" for i in range(15)})
    d = client.get("/api/orderbook/BTC/USD").get_json()
    bids = [x["price"] for x in d["bids"]]
    asks = [x["price"] for x in d["asks"]]
    assert len(bids) == 10 and len(asks) == 10
    assert bids == sorted(bids, reverse=True) and bids[0] == 114
    assert asks == sorted(asks) and asks[0] == 200
    assert "fallback" not in d


def test_orders_empty(client):
    r = client.get("/api/orders")
    assert r.status_code == 200 and r.get_json() == []
    assert client.get("/api/fills").get_json() == []
    assert client.get("/api/algos").get_json() == []


def test_post_order_bad_body(client):
    assert client.post("/api/order", json={"symbol": "BTC/USD"}).status_code == 400
    assert client.post("/api/order", data="nope").status_code == 400


GOOD = {"symbol": "BTC/USD", "qty": 0.001, "side": "buy", "type": "market"}


def test_post_order_oms_down(client):
    r = client.post("/api/order", json=GOOD)
    assert r.status_code == 503
    assert r.get_json()["ok"] is False


def test_post_order_ok(client, fake_redis, monkeypatch):
    runtime.Heartbeat(fake_redis, "oms").beat()
    sent = []

    def fake_send(r, stream, payload, timeout=10.0):
        sent.append((stream, payload))
        return {"ok": True, "status": "acked", "order_id": "o1"}

    monkeypatch.setattr(webgui.runtime, "send_command", fake_send)
    r = client.post("/api/order", json=GOOD)
    assert r.status_code == 200 and r.get_json()["status"] == "acked"
    assert sent[0][0] == runtime.STREAM_OMS_COMMANDS
    assert sent[0][1]["account"] == "paper" and sent[0][1]["kind"] == "submit"


def test_strategies(client):
    d = client.get("/api/algos/strategies").get_json()
    by = {x["id"]: x for x in d}
    assert by["twap"]["available"] is True
    assert by["vwap"]["available"] is False


def test_post_algo_ems_down(client):
    r = client.post("/api/algo", json={"strategy": "twap", "symbol": "BTC/USD", "side": "buy",
                                       "total_qty": 0.01, "duration_s": 60, "n_slices": 3})
    assert r.status_code == 503


ALGO = {"strategy": "twap", "symbol": "BTC/USD", "side": "buy", "total_qty": 0.01,
        "duration_s": 60, "n_slices": 3}


def _capture(monkeypatch, reply):
    sent = []

    def fake_send(r, stream, payload, timeout=10.0):
        sent.append((stream, dict(payload)))
        return reply

    monkeypatch.setattr(webgui.runtime, "send_command", fake_send)
    return sent


def test_post_algo_oms_up(client, fake_redis, monkeypatch):
    runtime.Heartbeat(fake_redis, "oms").beat()
    sent = _capture(monkeypatch, {"ok": True, "status": "pending", "algo_id": "a1"})
    r = client.post("/api/algo", json=ALGO)
    assert r.status_code == 200 and r.get_json()["algo_id"] == "a1"
    assert len(sent) == 1
    stream, payload = sent[0]
    assert stream == runtime.STREAM_OMS_COMMANDS and stream != runtime.STREAM_EMS_REQUESTS
    assert payload["kind"] == "algo_create" and payload["strategy"] == "twap"


def test_delete_algo(client, fake_redis, monkeypatch):
    runtime.Heartbeat(fake_redis, "oms").beat()
    sent = _capture(monkeypatch, {"ok": True, "status": "cancelled", "algo_id": "a1"})
    r = client.delete("/api/algo/a1")
    assert r.status_code == 200
    assert sent[0][0] == runtime.STREAM_OMS_COMMANDS
    assert sent[0][1]["kind"] == "algo_cancel" and sent[0][1]["algo_id"] == "a1"


def test_delete_algo_oms_down(client):
    assert client.delete("/api/algo/a1").status_code == 503


def test_algo_not_found(client):
    assert client.get("/api/algo/nope").status_code == 404
