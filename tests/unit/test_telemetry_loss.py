"""Loss must reach health reports without counting acknowledgements as drops."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.heartbeat import Heartbeat
from z4j_core.transport.frames import parse_frame


def batch(count: int) -> bytes:
    return json.dumps({"type": "event_batch", "payload": {"events": [{}] * count}}).encode()


@pytest.fixture
def store(tmp_path):
    buffer = BufferStore(path=tmp_path / "loss.sqlite", max_entries=2)
    yield buffer
    buffer.close()


@pytest.mark.asyncio
async def test_real_capacity_loss_reaches_both_reports_without_ack_loss(store):
    store.append("event_batch", batch(7))
    store.append("command_result", b"result")
    store.append("heartbeat", b"heartbeat")
    store.append("agent_status", b"status")
    store.confirm([entry.id for entry in store.drain(10)])
    hb = Heartbeat(
        buffer=store,
        stop_event=asyncio.Event(),
        status_provider=lambda: {"buffer_depth": 0},
        engines={"celery": SimpleNamespace(dropped_event_count=3)},
    )
    await hb._enqueue_heartbeat()
    await hb._enqueue_agent_status()
    frames = [parse_frame(entry.payload) for entry in store.drain(10)]
    assert frames[0].payload.dropped_events == 10
    for frame in frames:
        loss = frame.payload.telemetry_loss
        assert loss.event_records == 7
        assert loss.command_results == 1
        assert loss.capacity_evicted_frames == 2
        assert loss.other_frames == 0
        assert loss.adapter_events == {"celery": 3}
    assert frames[0].payload.telemetry_loss == frames[1].payload.telemetry_loss


def test_rejection_is_exact_and_malformed_batches_are_unknown(store):
    first = store.append("event_batch", b"broken json")
    second = store.append("event_batch", batch(4))
    store.increment_content_rejects([first])
    assert store.evict_if_exhausted([first, second], 1) == 1
    assert store.evict_if_exhausted([first, second], 1) == 0
    assert [entry.id for entry in store.drain(10)] == [second]
    assert store.loss_snapshot()["content_rejected_frames"] == 1
    assert store.loss_snapshot()["unclassified_frames"] == 1
    assert store.loss_snapshot()["event_records"] == 0


def test_rolled_back_projection_eviction_does_not_report_loss(store):
    store.append("event_batch", batch(5))
    before = store.loss_snapshot()
    with store._lock:
        store._conn.execute("BEGIN IMMEDIATE")
        assert store._drop_oldest_locked()
        store._conn.execute("ROLLBACK")
        # Projection callers reconcile their cached sizes on rollback.
        store._cached_count = 1
        store._cached_bytes = len(batch(5))
    assert store.loss_snapshot() == before
    assert len(store.drain(10)) == 1


def test_counter_write_failure_cannot_delete_the_entry(store):
    store.append("event_batch", batch(2))
    store._conn.execute("""CREATE TRIGGER reject_loss BEFORE INSERT ON _meta
        WHEN NEW.key = 'telemetry_loss_v1' BEGIN SELECT RAISE(ABORT, 'injected'); END""")
    with store._lock, pytest.raises(sqlite3.IntegrityError, match="injected"):
        store._drop_oldest_locked()
    assert len(store.drain(10)) == store.size() == 1
    assert store.loss_snapshot()["capacity_evicted_frames"] == 0


def test_counters_survive_reopen_of_retained_buffer(store):
    store.append("event_batch", batch(6))
    store.append("heartbeat", b"one")
    store.append("heartbeat", b"two")
    before = store.loss_snapshot()
    path = store._path
    store.close()
    reopened = BufferStore(path=path, max_entries=2)
    try:
        assert reopened.loss_snapshot() == before
    finally:
        reopened.close()


def test_concurrent_capacity_loss_counts_records_exactly(store):
    def append_and_read(_):
        store.append("event_batch", batch(2))
        return store.loss_snapshot()

    with ThreadPoolExecutor(max_workers=8) as pool:
        snapshots = list(pool.map(append_and_read, range(100)))
    assert store.loss_snapshot()["capacity_evicted_frames"] == 98
    assert store.loss_snapshot()["event_records"] == 196
    assert all(s["event_records"] == 2 * s["capacity_evicted_frames"] for s in snapshots)


@pytest.mark.asyncio
async def test_health_failure_does_not_hide_loss(store):
    store.append("heartbeat", b"one")
    store.append("heartbeat", b"two")
    store.append("heartbeat", b"three")
    store.confirm([entry.id for entry in store.drain(10)])

    def failing_health():
        raise ConnectionError("broker unavailable")

    hb = Heartbeat(buffer=store, stop_event=asyncio.Event(), health_provider=failing_health)
    await hb._enqueue_heartbeat()
    frame = parse_frame(store.drain(1)[0].payload)
    assert frame.payload.dropped_events == 0
    assert frame.payload.telemetry_loss.other_frames == 1
    assert frame.payload.adapter_health["error"] == "provider raised"
