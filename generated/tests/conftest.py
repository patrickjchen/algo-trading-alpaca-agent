"""Shared fixtures for the offline (fakeredis / temp SQLite) and live test suites."""
from __future__ import annotations

import math
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fakeredis  # noqa: E402
import pytest  # noqa: E402

import runtime  # noqa: E402

GEN_DIR = Path(__file__).resolve().parent.parent


def pytest_addoption(parser):
    parser.addoption("--run-twap", action="store_true", default=False,
                     help="run the live TWAP test (places real paper orders)")


def live_env() -> str | None:
    """Return None if live tests can run, else a reason string."""
    missing = [k for k in ("ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY") if not os.environ.get(k)]
    if missing:
        return f"missing env: {', '.join(missing)}"
    url = os.environ.get("REDIS_URL", runtime.REDIS_URL)
    try:
        import redis
        redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2).ping()
    except Exception as e:  # noqa: BLE001
        return f"redis not reachable at {url}: {e}"
    return None


def pytest_collection_modifyitems(config, items):
    live_items = [it for it in items if "live" in it.keywords]
    if not live_items:
        return
    reason = live_env()
    if reason is None:
        return
    skip = pytest.mark.skip(reason=f"live test skipped: {reason}")
    for it in live_items:
        it.add_marker(skip)


def _make_fake():
    fake = fakeredis.FakeRedis(decode_responses=True)
    # Some fakeredis versions lack XAUTOCLAIM; GroupReader calls it periodically. Patch a
    # no-op returning an empty claim result ([next_id, [], []]) if it is missing/broken.
    try:
        fake.xautoclaim("__probe__", "__g__", "__c__", min_idle_time=0)
    except Exception as e:  # noqa: BLE001
        if "NOGROUP" not in str(e).upper() and "no such key" not in str(e).lower():
            fake.xautoclaim = lambda *a, **k: ["0-0", [], []]
    return fake


@pytest.fixture
def fake_redis():
    return _make_fake()


@pytest.fixture(autouse=True)
def _isolate(request, monkeypatch, tmp_path):
    if "live" in request.keywords:
        yield
        return
    fake = request.getfixturevalue("fake_redis")
    db = str(tmp_path / "alpaca_test.db")
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    monkeypatch.setenv("ALPACA_DB_PATH", db)
    monkeypatch.setattr(runtime, "DB_PATH", db)
    monkeypatch.setattr(runtime, "get_redis", lambda url=None: fake)
    yield


@pytest.fixture
def no_creds(monkeypatch):
    for k in list(os.environ):
        if k.startswith("ALPACA_") and k != "ALPACA_DB_PATH":
            monkeypatch.delenv(k, raising=False)


@pytest.fixture
def db_conn(tmp_path):
    import persister  # persister owns the schema
    conn = runtime.sqlite_connect(str(tmp_path / "t.db"))
    persister.setup_db(conn)
    yield conn
    conn.close()


# =========================================================================== live helpers
# Processes started by the live tests (name -> Popen). Anything here is ours to stop.
STARTED: dict = {}


def paper_only() -> None:
    """Hard safety gate: live tests only ever run against the paper account."""
    if os.environ.get("ALPACA_LIVE"):
        pytest.fail("ALPACA_LIVE is set: live integration tests run on the PAPER account only")


def redis_client():
    return runtime.get_redis()


def db():
    """SQLite connection to the shared DB (tests only read from it)."""
    return runtime.sqlite_connect(runtime.DB_PATH)


def wait_until(fn, timeout: float, every: float = 0.5):
    """Poll ``fn`` until it returns a truthy value (returned) or ``timeout`` elapses (None)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            v = fn()
        except Exception:  # noqa: BLE001
            v = None
        if v:
            return v
        if time.monotonic() >= deadline:
            return None
        time.sleep(every)


def started_by_tests(name: str) -> bool:
    p = STARTED.get(name)
    return p is not None and p.poll() is None


def ensure_worker(name: str, timeout: float = 20.0):
    """Make sure worker ``name`` is alive. Returns the Popen if we started it, else None."""
    r = redis_client()
    if runtime.worker_alive(r, name):
        return None
    p = STARTED.get(name)
    if p is not None and p.poll() is None:
        if wait_until(lambda: runtime.worker_alive(r, name), timeout):
            return p
        pytest.fail(f"worker {name} (pid {p.pid}) started by tests has no heartbeat")
    errf = tempfile.NamedTemporaryFile(prefix=f"live-{name}-", suffix=".log", delete=False)
    p = subprocess.Popen([sys.executable, f"{name}.py"], cwd=str(GEN_DIR), env=os.environ.copy(),
                         stdout=subprocess.DEVNULL, stderr=errf)
    STARTED[name] = p
    if not wait_until(lambda: runtime.worker_alive(r, name) or p.poll() is not None, timeout) \
            or not runtime.worker_alive(r, name):
        stop_worker(name)
        pytest.fail(f"worker {name} did not heartbeat within {timeout}s (log: {errf.name})")
    return p


def stop_worker(name: str, timeout: float = 15.0) -> None:
    """Stop a worker this test session started (never touches the user's workers)."""
    p = STARTED.pop(name, None)
    if p is None:
        return
    if p.poll() is None:
        try:
            p.send_signal(signal.SIGTERM)
            p.wait(timeout=timeout)
        except Exception:  # noqa: BLE001
            try:
                p.kill()
                p.wait(timeout=5)
            except Exception:  # noqa: BLE001
                pass
    keys = [runtime.heartbeat_key(name)]
    if name == "oms_ems":
        keys += [runtime.heartbeat_key("oms"), runtime.heartbeat_key("ems")]
    try:
        redis_client().delete(*keys)
    except Exception:  # noqa: BLE001
        pass


def ensure_oms_ems(timeout: float = 20.0) -> list:
    """Make sure both OMS and EMS heartbeats exist.

    Neither alive -> start ``oms_ems.py`` once and wait for BOTH heartbeats; exactly one alive
    -> start only the missing standalone module. Returns the names of processes started."""
    r = redis_client()
    oms_up = runtime.worker_alive(r, "oms") is not None
    ems_up = runtime.worker_alive(r, "ems") is not None
    if oms_up and ems_up:
        return []
    if not oms_up and not ems_up:
        p = STARTED.get("oms_ems")
        if p is None or p.poll() is not None:
            errf = tempfile.NamedTemporaryFile(prefix="live-oms_ems-", suffix=".log", delete=False)
            p = subprocess.Popen([sys.executable, "oms_ems.py"], cwd=str(GEN_DIR),
                                 env=os.environ.copy(), stdout=subprocess.DEVNULL, stderr=errf)
            STARTED["oms_ems"] = p
            logname = errf.name
        else:
            logname = "?"

        def both():
            return (runtime.worker_alive(r, "oms") and runtime.worker_alive(r, "ems")) \
                or p.poll() is not None

        if not wait_until(both, timeout) or p.poll() is not None:
            stop_worker("oms_ems")
            pytest.fail(f"oms_ems did not heartbeat oms+ems within {timeout}s (log: {logname})")
        return ["oms_ems"]
    missing = "ems" if oms_up else "oms"
    ensure_worker(missing, timeout)
    return [missing]


def oms_host_started_by_tests() -> str | None:
    """Name of the test-started process hosting OMS ('oms' or 'oms_ems'), else None."""
    for name in ("oms", "oms_ems"):
        if started_by_tests(name):
            return name
    return None


def stop_all_started() -> None:
    for name in list(STARTED):
        stop_worker(name)


def round_down(x: float, inc) -> float:
    if not inc:
        return float(x)
    return round(math.floor(float(x) / inc + 1e-9) * inc, 12)


def round_up(x: float, inc) -> float:
    if not inc:
        return float(x)
    return round(math.ceil(float(x) / inc - 1e-9) * inc, 12)


def safe_limit_buy(symbol: str = "BTC/USD") -> dict:
    """Return {symbol, qty, limit_price, rules, quote} for a non-marketable limit BUY:
    priced 20% below the bid, qty = max(min_order_size, 2/price) rounded up to the increment."""
    import trader
    rules = trader.get_asset_rules(symbol)
    quote = trader.latest_quote(symbol)
    bid = float(quote["bid_price"])
    assert bid > 0
    px = round_down(bid * 0.8, rules.get("price_increment"))
    min_size = rules.get("min_order_size") or 0.0
    qty = round_up(max(min_size, 2.0 / px), rules.get("min_trade_increment"))
    return {"symbol": symbol, "qty": qty, "limit_price": px, "rules": rules, "quote": quote}


@pytest.fixture(scope="session")
def live_workers():
    """Session-wide teardown for any worker processes the live tests started."""
    yield STARTED
    stop_all_started()


@pytest.fixture
def live_redis(live_workers):
    paper_only()
    r = redis_client()
    try:
        yield r
    finally:
        try:
            r.close()
        except Exception:  # noqa: BLE001
            pass
