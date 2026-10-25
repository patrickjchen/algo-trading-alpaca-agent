import threading
import time

import runtime

import oms_ems


def test_start_workers_with_fake_targets():
    seen = {}
    lock = threading.Lock()
    stop = threading.Event()

    def make(name):
        def fn(ev):
            with lock:
                seen[name] = (ev, threading.current_thread().name,
                              threading.current_thread().daemon)
            ev.wait(5)
        return fn

    threads = oms_ems.start_workers(stop, targets={"oms": make("oms"), "ems": make("ems")})
    try:
        assert sorted(t.name for t in threads) == ["worker-ems", "worker-oms"]
        assert all(t.daemon for t in threads)
        deadline = time.monotonic() + 3
        while len(seen) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert set(seen) == {"oms", "ems"}
        assert seen["oms"][0] is stop and seen["ems"][0] is stop
        assert seen["oms"][1] == "worker-oms" and seen["ems"][1] == "worker-ems"
        assert seen["oms"][2] and seen["ems"][2]
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert not any(t.is_alive() for t in threads)


def test_both_heartbeats_written(fake_redis):
    stop = threading.Event()

    def make(name):
        def body():
            stop.wait(10)

        return lambda ev: runtime.run_forever(body, name, stop_event=ev)

    threads = oms_ems.start_workers(stop, targets={"oms": make("oms"), "ems": make("ems")})
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if runtime.worker_alive(fake_redis, "oms") and runtime.worker_alive(fake_redis, "ems"):
                break
            time.sleep(0.05)
        assert runtime.worker_alive(fake_redis, "oms") is not None
        assert runtime.worker_alive(fake_redis, "ems") is not None
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert not any(t.is_alive() for t in threads)


def test_main_returns_1_when_worker_dies(monkeypatch):
    started = {}

    def fake_start(stop_event, *, targets=None):
        dead = threading.Thread(target=lambda: None, name="worker-oms", daemon=True)
        waiting = threading.Thread(target=lambda: stop_event.wait(5), name="worker-ems",
                                   daemon=True)
        dead.start()
        waiting.start()
        started["threads"] = [dead, waiting]
        started["stop"] = stop_event
        return [dead, waiting]

    monkeypatch.setattr(oms_ems, "start_workers", fake_start)
    t0 = time.monotonic()
    assert oms_ems.main([]) == 1
    assert time.monotonic() - t0 < 5
    assert started["stop"].is_set()
    for t in started["threads"]:
        t.join(timeout=2)
        assert not t.is_alive()
