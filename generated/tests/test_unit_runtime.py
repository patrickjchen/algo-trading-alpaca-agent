import json

import redis

import runtime


def test_redis_call_spills_then_flush_fifo():
    calls = []
    state = {"up": False}

    def fn(x):
        if not state["up"]:
            raise redis.exceptions.ConnectionError("down")
        calls.append(x)
        return x

    buf = runtime.SpillBuffer()
    assert runtime.redis_call(fn, 1, retries=2, backoff=0, buffer=buf) is None
    assert runtime.redis_call(fn, 2, retries=1, backoff=0, buffer=buf) is None
    assert len(buf) == 2
    state["up"] = True
    assert buf.flush() == 2
    assert calls == [1, 2]
    assert len(buf) == 0


def test_redis_call_raises_without_buffer():
    def fn():
        raise redis.exceptions.ConnectionError("down")
    try:
        runtime.redis_call(fn, retries=1, backoff=0)
    except redis.exceptions.ConnectionError:
        pass
    else:
        raise AssertionError("expected ConnectionError")


def test_spill_buffer_bounded():
    buf = runtime.SpillBuffer(maxlen=2)
    for i in range(3):
        buf.add(print, i)
    assert len(buf) == 2 and buf.dropped == 1


def test_heartbeat_and_worker_alive(fake_redis):
    hb = runtime.Heartbeat(fake_redis, "oms")
    assert hb.beat()
    key = runtime.heartbeat_key("oms")
    assert fake_redis.ttl(key) > 0
    d = json.loads(fake_redis.get(key))
    assert "pid" in d
    alive = runtime.worker_alive(fake_redis, "oms")
    assert isinstance(alive, dict) and alive["pid"] == d["pid"]
    assert runtime.worker_alive(fake_redis, "ems") is None


def test_group_reader_flow(fake_redis):
    runtime.ensure_group(fake_redis, "s", "g")
    runtime.ensure_group(fake_redis, "s", "g")  # BUSYGROUP swallowed
    fake_redis.xadd("s", {"json": "1"})
    fake_redis.xadd("s", {"json": "2"})
    rd = runtime.GroupReader(fake_redis, "s", "g", "c1", block_ms=10)
    entries = rd.read()
    if not entries:
        entries = rd.read()
    assert [f["json"] for _, f in entries] == ["1", "2"]

    # not acked -> new reader with same consumer re-delivers via id "0"
    rd2 = runtime.GroupReader(fake_redis, "s", "g", "c1", block_ms=10)
    again = rd2.read()
    assert [eid for eid, _ in again] == [eid for eid, _ in entries]

    rd2.ack(again[0][0])
    rd2.dead_letter(again[1][0], again[1][1], "boom")
    assert fake_redis.xlen("s:dlq") == 1
    dlq = fake_redis.xrange("s:dlq")
    assert dlq[0][1]["error"] == "boom"
    assert fake_redis.xpending("s", "g")["pending"] == 0


def test_send_command_round_trip(fake_redis):
    runtime.send_reply(fake_redis, "c1", {"ok": True, "status": "acked"})
    assert fake_redis.ttl(runtime.reply_key("c1")) > 0
    rep = runtime.send_command(fake_redis, "cmds", {"cmd_id": "c1", "kind": "submit"}, timeout=1)
    assert rep == {"ok": True, "status": "acked"}
    entries = fake_redis.xrange("cmds")
    assert len(entries) == 1
    assert json.loads(entries[0][1]["json"])["cmd_id"] == "c1"


def test_wait_reply_timeout(fake_redis):
    rep = runtime.wait_reply(fake_redis, "nobody", timeout=1)
    assert rep["ok"] is False and rep["status"] == "timeout"


def test_sqlite_wal(tmp_path):
    conn = runtime.sqlite_connect(str(tmp_path / "x.db"))
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    finally:
        conn.close()


def test_terminal_statuses():
    assert runtime.TERMINAL_STATUSES == {"filled", "canceled", "expired", "rejected", "replaced"}
    assert not (runtime.TERMINAL_STATUSES & runtime.ACTIVE_STATUSES)


def test_publish_xadds_json(fake_redis):
    runtime.publish(fake_redis, "pub", {"a": 1, "b": None})
    entries = fake_redis.xrange("pub")
    assert len(entries) == 1
    assert json.loads(entries[0][1]["json"]) == {"a": 1, "b": None}


def test_hset_state(fake_redis):
    runtime.hset_state(fake_redis, "h", {"a": 1, "b": None, "c": True, "d": {"x": 1}}, ttl=100)
    h = fake_redis.hgetall("h")
    assert h == {"a": "1", "c": "1", "d": json.dumps({"x": 1})}
    assert 0 < fake_redis.ttl("h") <= 100
    runtime.hset_state(fake_redis, "h", {"a": 2})
    assert fake_redis.ttl("h") == -1 and fake_redis.hget("h", "a") == "2"


def test_key_helpers():
    assert runtime.order_key("o1") == "alpaca:order:o1"
    assert runtime.coid_key("c1") == "alpaca:coid:c1"
    assert runtime.cmd_key("x") == "alpaca:cmd:x"
    assert runtime.algo_key("a") == "alpaca:algo:a"
    assert runtime.algo_slices_key("a") == "alpaca:algo:a:slices"
    assert runtime.reply_key("x") == "alpaca:reply:x"
    assert runtime.heartbeat_key("oms") == "alpaca:heartbeat:oms"


def test_normalize_order():
    o = runtime.normalize_order({"id": "abc", "side": "OrderSide.BUY", "qty": "0.5",
                                 "status": "OrderStatus.FILLED", "time_in_force": "gtc",
                                 "filled_qty": "0.25", "order_type": "limit"})
    assert o["order_id"] == "abc" and o["side"] == "buy" and o["status"] == "filled"
    assert o["qty"] == 0.5 and o["filled_qty"] == 0.25 and o["type"] == "limit"
    assert o["limit_price"] is None and o["client_order_id"] is None
    empty = runtime.normalize_order({})
    assert empty["order_id"] is None and set(empty) == set(o)


def test_ids():
    cid = runtime.new_client_order_id("tc")
    assert cid.startswith("tc-") and len(cid) <= 48
    assert runtime.new_id() != runtime.new_id()
