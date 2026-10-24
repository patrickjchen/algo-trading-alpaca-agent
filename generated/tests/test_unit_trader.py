import enum
import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from alpaca.common.exceptions import APIError

import trader


def _api_error(code):
    return APIError(json.dumps({"code": code, "message": "x"}),
                    SimpleNamespace(response=SimpleNamespace(status_code=code), request=None))


def test_tif():
    with pytest.raises(ValueError):
        trader._tif("day")
    assert trader._tif("GTC").value == "gtc"
    assert trader._tif("ioc").value == "ioc"


def test_side():
    assert trader._side("BUY").value == "buy"
    with pytest.raises(ValueError):
        trader._side("short")


class Color(str, enum.Enum):
    RED = "red"


class FakeModel:
    def model_dump(self, mode="python"):
        return {"side": Color.RED, "legs": [Color.RED, 1], "id": "abc"}


def test_to_dict():
    assert trader._to_dict(Color.RED) == "red"
    assert trader._to_dict(FakeModel()) == {"side": "red", "legs": ["red", 1], "id": "abc"}
    u = uuid.uuid4()
    dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert trader._to_dict([u, dt, None]) == [str(u), dt.isoformat(), None]
    ns = SimpleNamespace(a=1, _hidden=2, b=Color.RED)
    assert trader._to_dict(ns) == {"a": 1, "b": "red"}


def test_is_transient():
    assert trader.is_transient(_api_error(429)) is True
    assert trader.is_transient(_api_error(500)) is True
    assert trader.is_transient(_api_error(422)) is False
    assert trader.is_transient(_api_error(404)) is False
    assert trader.is_transient(ConnectionError("x")) is True
    assert trader.is_transient(ValueError("x")) is False


def test_error_info_non_json():
    e = APIError("<html>bad gateway</html>",
                 SimpleNamespace(response=SimpleNamespace(status_code=502), request=None))
    info = trader.error_info(e)
    assert info["status_code"] == 502 and info["code"] is None


def test_get_client_without_creds(no_creds, monkeypatch):
    monkeypatch.setattr(trader, "_clients", {})
    with pytest.raises(RuntimeError):
        trader.get_client(False)
