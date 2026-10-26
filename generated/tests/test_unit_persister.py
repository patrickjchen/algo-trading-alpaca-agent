import json

import persister
import runtime

TU = runtime.STREAM_TRADE_UPDATES


def _reader(fake, stream, consumer="p1"):
    return runtime.GroupReader(fake, stream, persister.GROUP, consumer, block_ms=10)


def _xadd(fake, stream, payload):
    return fake.xadd(stream, {"json": json.dumps(payload, default=str)})


def _order(status="new", filled="0", oid="o1", coid="tc-1"):
    return {"id": oid, "client_order_id": coid, "symbol": "BTC/USD", "side": "buy", "type": "limit",
            "time_in_force": "gtc", "qty": "0.01", "limit_price": "100", "filled_qty": filled,
            "status": status}


def _tu(event, status, filled="0", exec_id=None):
    return {"event": event, "execution_id": exec_id, "order_id": "o1", "client_order_id": "tc-1",
            "symbol": "BTC/USD", "side": "buy", "price": "100" if exec_id else None,
            "qty": "0.01" if exec_id else None, "ts": runtime.now_iso(), "account": "paper",
            "order": _order(status, filled)}


def _count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_setup_db_tables(db_conn):
    names = {r[0] for r in db_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"orders", "fills", "trade_events", "commands", "algo_orders", "algo_slices"} <= names


def test_new_trade_update(fake_redis, db_conn):
    rd = _reader(fake_redis, TU)
    _xadd(fake_redis, TU, _tu("new", "new"))
    assert persister.process_batch(db_conn, rd, persister.handle_trade_update) == 1
    row = db_conn.execute("SELECT status, qty FROM orders WHERE order_id='o1'").fetchone()
    assert row["status"] == "new" and row["qty"] == 0.01
    assert _count(db_conn, "trade_events") == 1
    assert _count(db_conn, "fills") == 0


def test_fill_and_replay_idempotent(fake_redis, db_conn):
    rd = _reader(fake_redis, TU)
    _xadd(fake_redis, TU, _tu("fill", "filled", "0.01", exec_id="e1"))
    entries = rd.read()
    assert len(entries) == 1
    for eid, f in entries:  # processed but not acked (simulated crash before ack)
        persister.handle_trade_update(db_conn, eid, json.loads(f["json"]))
    db_conn.commit()
    rd2 = _reader(fake_redis, TU)
    assert persister.process_batch(db_conn, rd2, persister.handle_trade_update) == 1
    assert _count(db_conn, "fills") == 1
    assert _count(db_conn, "trade_events") == 1
    assert fake_redis.xpending(TU, persister.GROUP)["pending"] == 0


def test_terminal_status_not_regressed(fake_redis, db_conn):
    rd = _reader(fake_redis, TU)
    _xadd(fake_redis, TU, _tu("fill", "filled", "0.01", exec_id="e1"))
    _xadd(fake_redis, TU, _tu("new", "new"))
    persister.process_batch(db_conn, rd, persister.handle_trade_update)
    row = db_conn.execute("SELECT status, filled_qty FROM orders WHERE order_id='o1'").fetchone()
    assert row["status"] == "filled" and row["filled_qty"] == 0.01


def test_ems_report_no_fill_row(fake_redis, db_conn):
    rd = _reader(fake_redis, TU)
    p = _tu("ems_submitted", "accepted")
    p.update(source="algo", algo_id="abc", slice_idx=3)
    _xadd(fake_redis, TU, p)
    persister.process_batch(db_conn, rd, persister.handle_trade_update)
    row = db_conn.execute("SELECT source, algo_id, slice_idx FROM orders").fetchone()
    assert (row["source"], row["algo_id"], row["slice_idx"]) == ("algo", "abc", 3)
    assert _count(db_conn, "fills") == 0


def test_oms_command_and_order(fake_redis, db_conn):
    st = runtime.STREAM_OMS_EVENTS
    rd = _reader(fake_redis, st)
    _xadd(fake_redis, st, {"type": "command", "ts": runtime.now_iso(), "command": {
        "cmd_id": "c1", "kind": "submit", "account": "paper", "client_order_id": "tc-1",
        "order_id": "o1", "algo_id": None, "slice_idx": None, "source": "webgui",
        "payload": {"kind": "submit"}, "status": "acked", "error": None,
        "received_at": runtime.now_iso(), "completed_at": runtime.now_iso()}})
    _xadd(fake_redis, st, {"type": "order", "ts": runtime.now_iso(), "account": "paper",
                           "source": "webgui", "algo_id": None, "slice_idx": None,
                           "order": runtime.normalize_order(_order("accepted"))})
    assert persister.process_batch(db_conn, rd, persister.handle_oms_event) == 2
    c = db_conn.execute("SELECT status, kind, source FROM commands WHERE cmd_id='c1'").fetchone()
    assert (c["status"], c["kind"], c["source"]) == ("acked", "submit", "webgui")
    o = db_conn.execute("SELECT source, status FROM orders WHERE order_id='o1'").fetchone()
    assert o["source"] == "webgui" and o["status"] == "accepted"


def _slice(aid, i, status="scheduled", filled=0.0):
    return {"algo_id": aid, "slice_idx": i, "scheduled_ts": 1000.0 + i * 10, "qty": 0.01,
            "client_order_id": f"algo-{aid[:12]}-{i:03d}", "order_id": None, "limit_price": None,
            "status": status, "filled_qty": filled, "updated_at": runtime.now_iso()}


def test_algo_events(fake_redis, db_conn):
    st = runtime.STREAM_ALGO_EVENTS
    rd = _reader(fake_redis, st)
    aid = "a" * 32
    algo = {"algo_id": aid, "strategy": "twap", "account": "paper", "symbol": "BTC/USD", "side": "buy",
            "total_qty": 0.03, "n_slices": 3, "start_ts": 1000.0, "end_ts": 1030.0, "limit_cap": None,
            "params_json": "{}", "source": "test", "status": "pending", "filled_qty": 0.0,
            "avg_px": None, "error": None, "created_at": runtime.now_iso(), "updated_at": runtime.now_iso()}
    _xadd(fake_redis, st, {"type": "algo", "ts": runtime.now_iso(), "algo": algo,
                           "slices": [_slice(aid, i) for i in range(3)]})
    _xadd(fake_redis, st, {"type": "slice", "ts": runtime.now_iso(), "algo_id": aid,
                           "slice": _slice(aid, 1, "filled", 0.01)})
    assert persister.process_batch(db_conn, rd, persister.handle_algo_event) == 2
    assert db_conn.execute("SELECT status FROM algo_orders WHERE algo_id=?", (aid,)).fetchone()[0] == "pending"
    assert _count(db_conn, "algo_slices") == 3
    s1 = db_conn.execute("SELECT status, filled_qty FROM algo_slices WHERE algo_id=? AND slice_idx=1",
                         (aid,)).fetchone()
    assert s1["status"] == "filled" and s1["filled_qty"] == 0.01
    s0 = db_conn.execute("SELECT status FROM algo_slices WHERE algo_id=? AND slice_idx=0", (aid,)).fetchone()
    assert s0["status"] == "scheduled"


def test_malformed_dead_lettered(fake_redis, db_conn):
    rd = _reader(fake_redis, TU)
    _xadd(fake_redis, TU, _tu("new", "new"))
    bad = fake_redis.xadd(TU, {"json": "{not json"})
    p2 = _tu("new", "new")
    p2["order"] = _order(oid="o2", coid="tc-2")
    p2["order_id"] = "o2"
    _xadd(fake_redis, TU, p2)
    assert persister.process_batch(db_conn, rd, persister.handle_trade_update) == 3
    assert _count(db_conn, "orders") == 2
    dlq = fake_redis.xrange(f"{TU}:dlq")
    assert len(dlq) == 1 and dlq[0][1]["src_id"] == bad
    assert fake_redis.xpending(TU, persister.GROUP)["pending"] == 0
