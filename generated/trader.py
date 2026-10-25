"""Thin synchronous wrapper around alpaca-py trading REST. The ONLY module importing alpaca.trading.

No network / env access at import time; clients are created lazily.
"""
from __future__ import annotations

import enum
import os
import threading
import uuid
from datetime import date, datetime, timezone

from alpaca.common.exceptions import APIError
from alpaca.data import CryptoHistoricalDataClient, CryptoLatestQuoteRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, MarketOrderRequest

import runtime

log = runtime.log("trader")

_clients: dict = {}
_clients_lock = threading.Lock()
_data_client = None

QUOTE_MAX_AGE_S = 30.0
ASSET_CACHE_TTL = 3600


# --------------------------------------------------------------------------- clients
def get_client(live: bool = False) -> TradingClient:
    live = bool(live)
    with _clients_lock:
        c = _clients.get(live)
        if c is not None:
            return c
        key = os.environ.get("ALPACA_API_KEY_ID")
        secret = os.environ.get("ALPACA_API_SECRET_KEY")
        if not key or not secret:
            raise RuntimeError("ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY not set")
        c = TradingClient(key, secret, paper=not live)
        _clients[live] = c
        return c


def _get_data_client() -> CryptoHistoricalDataClient:
    global _data_client
    if _data_client is None:
        _data_client = CryptoHistoricalDataClient()
    return _data_client


# --------------------------------------------------------------------------- serialisation
def _to_dict(obj):
    if obj is None or isinstance(obj, (bool, int, float, str)):
        if isinstance(obj, enum.Enum):
            return obj.value
        return obj
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, (uuid.UUID,)):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (list, tuple, set)):
        return [_to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _to_dict(v) for k, v in obj.items()}
    if hasattr(obj, "model_dump"):
        try:
            return _to_dict(obj.model_dump(mode="json"))
        except Exception:
            pass
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        try:
            d = obj.dict()
            if isinstance(d, dict):
                return _to_dict(d)
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        return {k: _to_dict(v) for k, v in vars(obj).items() if not k.startswith("_")}
    return str(obj)


def _side(s) -> OrderSide:
    v = str(getattr(s, "value", s) or "").strip().lower()
    if v == "buy":
        return OrderSide.BUY
    if v == "sell":
        return OrderSide.SELL
    raise ValueError(f"side must be buy or sell, got {s!r}")


def _tif(s) -> TimeInForce:
    v = str(getattr(s, "value", s) or "").strip().lower()
    if v == "gtc":
        return TimeInForce.GTC
    if v == "ioc":
        return TimeInForce.IOC
    raise ValueError("crypto supports gtc or ioc")


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- errors
def error_info(exc) -> dict:
    info = {"type": type(exc).__name__, "status_code": None, "code": None, "message": str(exc)[:500]}
    if isinstance(exc, APIError):
        try:
            info["status_code"] = exc.status_code
        except Exception:
            pass
        try:
            info["code"] = exc.code
        except Exception:
            pass
        try:
            m = exc.message
            if m:
                info["message"] = str(m)[:500]
        except Exception:
            pass
    return info


def _is_not_found(exc) -> bool:
    if not isinstance(exc, APIError):
        return False
    info = error_info(exc)
    if info["status_code"] == 404:
        return True
    if str(info["code"]) == "40410000":
        return True
    return "not found" in str(info["message"]).lower() or "not found" in str(exc).lower()


def is_transient(exc) -> bool:
    if isinstance(exc, APIError):
        try:
            sc = exc.status_code
        except Exception:
            sc = None
        if sc is None:
            return False
        return sc in (408, 429) or sc >= 500
    try:
        import requests  # alpaca-py dependency
        if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            return True
        if isinstance(exc, requests.exceptions.ChunkedEncodingError):
            return True
    except Exception:
        pass
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    return False


# --------------------------------------------------------------------------- account / orders
def account(live: bool = False) -> dict:
    return _to_dict(get_client(live).get_account())


def positions(live: bool = False) -> list:
    return [_to_dict(p) for p in get_client(live).get_all_positions()]


def open_orders(live: bool = False, symbols=None) -> list:
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500,
                           symbols=list(symbols) if symbols else None)
    return [_to_dict(o) for o in get_client(live).get_orders(filter=req)]


def _parse_dt(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, (int, float)):
        dt = datetime.fromtimestamp(float(v), tz=timezone.utc)
    else:
        s = str(v).strip()
        try:
            dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
        except ValueError:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def list_orders(status: str = "all", after=None, limit: int = 500, live: bool = False) -> list:
    st = str(status or "all").lower()
    mapping = {"open": QueryOrderStatus.OPEN, "closed": QueryOrderStatus.CLOSED,
               "all": QueryOrderStatus.ALL}
    if st not in mapping:
        raise ValueError("status must be open, closed or all")
    kw = {"status": mapping[st], "limit": max(1, min(int(limit or 500), 500))}
    after_dt = _parse_dt(after)
    if after_dt is not None:
        kw["after"] = after_dt
        kw["direction"] = "asc"
    req = GetOrdersRequest(**kw)
    return [_to_dict(o) for o in get_client(live).get_orders(filter=req)]


def get_order(order_id, live: bool = False) -> dict:
    return _to_dict(get_client(live).get_order_by_id(str(order_id)))


def get_order_by_client_id(client_order_id: str, live: bool = False):
    try:
        return _to_dict(get_client(live).get_order_by_client_id(client_order_id))
    except APIError as e:
        if _is_not_found(e):
            return None
        raise


def submit_market(symbol: str, qty, side, client_order_id: str, live: bool = False,
                  tif: str = "gtc") -> dict:
    req = MarketOrderRequest(symbol=symbol, qty=float(qty), side=_side(side),
                             time_in_force=_tif(tif), client_order_id=client_order_id)
    return _to_dict(get_client(live).submit_order(req))


def submit_limit(symbol: str, qty, side, limit_price, client_order_id: str, live: bool = False,
                 tif: str = "gtc") -> dict:
    req = LimitOrderRequest(symbol=symbol, qty=float(qty), side=_side(side),
                            limit_price=float(limit_price), time_in_force=_tif(tif),
                            client_order_id=client_order_id)
    return _to_dict(get_client(live).submit_order(req))


def cancel(order_id, live: bool = False) -> None:
    get_client(live).cancel_order_by_id(str(order_id))
    return None


def cancel_all(live: bool = False) -> list:
    res = get_client(live).cancel_orders()
    return [_to_dict(x) for x in (res or [])]


# --------------------------------------------------------------------------- asset rules
def _asset_key(symbol: str) -> str:
    return f"alpaca:asset:{symbol.upper()}"


def get_asset_rules(symbol: str, live: bool = False, r=None) -> dict:
    sym = str(symbol).upper()
    if r is not None:
        try:
            h = runtime.redis_call(r.hgetall, _asset_key(sym))
            if h and "symbol" in h:
                return {
                    "symbol": h.get("symbol") or sym,
                    "min_order_size": _f(h.get("min_order_size")),
                    "min_trade_increment": _f(h.get("min_trade_increment")),
                    "price_increment": _f(h.get("price_increment")),
                    "tradable": str(h.get("tradable", "1")).lower() in ("1", "true", "yes"),
                }
        except Exception as e:
            log.warning("asset cache read failed for %s: %s", sym, e)
    client = get_client(live)
    asset = None
    candidates = [sym.replace("/", ""), sym] if "/" in sym else [sym]
    last_exc = None
    for cand in candidates:
        try:
            asset = client.get_asset(cand)
            break
        except APIError as e:
            last_exc = e
            if not _is_not_found(e):
                raise
    if asset is None:
        raise last_exc if last_exc else RuntimeError(f"asset {sym} not found")
    rules = {
        "symbol": sym,
        "min_order_size": _f(getattr(asset, "min_order_size", None)),
        "min_trade_increment": _f(getattr(asset, "min_trade_increment", None)),
        "price_increment": _f(getattr(asset, "price_increment", None)),
        "tradable": bool(getattr(asset, "tradable", False)),
    }
    if r is not None:
        try:
            mapping = {k: v for k, v in rules.items() if v is not None}
            mapping["tradable"] = "1" if rules["tradable"] else "0"
            key = _asset_key(sym)
            runtime.redis_call(r.hset, key, mapping=mapping)
            runtime.redis_call(r.expire, key, ASSET_CACHE_TTL)
        except Exception as e:
            log.warning("asset cache write failed for %s: %s", sym, e)
    return rules


# --------------------------------------------------------------------------- quotes
def latest_quote(symbol: str, r=None) -> dict:
    sym = str(symbol).upper()
    if r is not None:
        try:
            h = runtime.redis_call(r.hgetall, f"alpaca:latest:{sym}")
            if h:
                bp, ap = _f(h.get("bid_price")), _f(h.get("ask_price"))
                ts_raw = h.get("timestamp") or h.get("ts")
                ts = None
                try:
                    ts = _parse_dt(ts_raw)
                except Exception:
                    ts = None
                if bp is not None and ap is not None and ts is not None:
                    age = (datetime.now(timezone.utc) - ts).total_seconds()
                    if age < QUOTE_MAX_AGE_S:
                        return {"bid_price": bp, "ask_price": ap,
                                "bid_size": _f(h.get("bid_size")),
                                "ask_size": _f(h.get("ask_size")),
                                "timestamp": ts.isoformat(), "source": "redis"}
        except Exception as e:
            log.warning("redis quote read failed for %s: %s", sym, e)
    resp = _get_data_client().get_crypto_latest_quote(CryptoLatestQuoteRequest(symbol_or_symbols=sym))
    q = resp[sym]
    ts = getattr(q, "timestamp", None)
    return {
        "bid_price": _f(getattr(q, "bid_price", None)),
        "ask_price": _f(getattr(q, "ask_price", None)),
        "bid_size": _f(getattr(q, "bid_size", None)),
        "ask_size": _f(getattr(q, "ask_size", None)),
        "timestamp": ts.isoformat() if isinstance(ts, datetime) else (str(ts) if ts else None),
        "source": "rest",
    }
