"""Combined trading-side worker: runs the OMS and the EMS bodies in one process.

Each body runs in its own daemon thread under its own ``runtime.run_forever`` supervisor,
so restart/backoff and the ``alpaca:heartbeat:oms`` / ``alpaca:heartbeat:ems`` heartbeats
behave exactly as when the workers run standalone. If either supervisor thread dies the
whole process exits with code 1 so an outer supervisor can restart it.
No I/O at import time.
"""
from __future__ import annotations

import os
import signal
import threading
import time

import ems
import oms
import runtime

NAME = "oms_ems"
WORKER_NAMES = ("oms", "ems")
POLL_S = 0.5
JOIN_GRACE_S = 5.0

log = runtime.log(NAME)


def run_oms(stop_event: threading.Event) -> None:
    runtime.run_forever(oms.body, "oms", stop_event=stop_event)


def run_ems(stop_event: threading.Event) -> None:
    runtime.run_forever(ems.body, "ems", stop_event=stop_event)


_DEFAULT_TARGETS = {"oms": run_oms, "ems": run_ems}


def start_workers(stop_event: threading.Event, *, targets=None) -> list:
    targets = targets if targets is not None else _DEFAULT_TARGETS
    threads = []
    for name in WORKER_NAMES:
        fn = targets.get(name)
        if fn is None:
            continue
        t = threading.Thread(target=fn, args=(stop_event,), name=f"worker-{name}", daemon=True)
        t.start()
        threads.append(t)
    return threads


def main(argv=None) -> int:
    stop = threading.Event()
    prev = {}

    def _handler(signum, frame):
        log.info("signal %s received; stopping", signum)
        stop.set()

    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                prev[sig] = signal.signal(sig, _handler)
            except (ValueError, OSError):
                pass

    exit_code = 0
    try:
        threads = start_workers(stop)
        log.info("oms_ems started pid=%s workers=%s", os.getpid(), ",".join(WORKER_NAMES))
        try:
            while not stop.is_set():
                for t in threads:
                    if not t.is_alive() and not stop.is_set():
                        log.error("worker thread %s died; stopping process", t.name)
                        exit_code = 1
                        stop.set()
                        break
                if stop.is_set():
                    break
                stop.wait(POLL_S)
        except KeyboardInterrupt:
            stop.set()
        stop.set()
        deadline = time.monotonic() + JOIN_GRACE_S
        for t in threads:
            t.join(timeout=max(0.0, deadline - time.monotonic()))
        alive = [t.name for t in threads if t.is_alive()]
        if alive:
            log.warning("worker threads still running after %.0fs grace: %s (daemon; exiting)",
                        JOIN_GRACE_S, ",".join(alive))
        log.info("oms_ems exiting code=%d", exit_code)
        return exit_code
    finally:
        for sig, h in prev.items():
            try:
                signal.signal(sig, h)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
