"""Frames the agent discards without delivering must be reported as loss.

Capacity eviction and an exhausted event_batch rejection budget reached the
telemetry-loss counters. Two other discards deleted frames through the buffer's
delivery path (``confirm``), so heartbeat and status reports showed no loss:

* long-poll: an isolated control frame the brain rejects (413 / 415 / 422);
* both transports: a frame refused locally (unparseable, not a signed frame
  type, or larger than the brain accepts), reported through ``UndeliverableFrameError``.

Each is now counted exactly once, by kind, under ``content_rejected_frames``,
and reaches the heartbeat and agent_status reports. The frames are still
discarded; only the accounting changes.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr
from z4j_bare.buffer import BufferStore
from z4j_bare.heartbeat import Heartbeat
from z4j_bare.runtime import AgentRuntime
from z4j_bare.transport.longpoll import PayloadTooLargeError, UploadContentRejectedError
from z4j_bare.transport.websocket import UndeliverableFrameError, WebSocketTransport
from z4j_core.models import Config
from z4j_core.transport.frames import (
    EventBatchFrame,
    EventBatchPayload,
    HeartbeatFrame,
    HeartbeatPayload,
    parse_frame,
    serialize_frame,
)
from z4j_core.transport.framing import FrameSigner

#: Valid JSON with an ``id`` that fails full frame validation (schema drift).
UNREADABLE = b'{"id": "cmd_drift", "type": "from_the_future", "payload": {}}'


def batch(count: int) -> bytes:
    return json.dumps({"type": "event_batch", "payload": {"events": [{}] * count}}).encode()


def heartbeat_frame() -> bytes:
    return serialize_frame(HeartbeatFrame(id="hb_delivered", payload=HeartbeatPayload()))


def oversize_event_batch() -> bytes:
    """One event whose blob signs well past the 2 KiB cap used below."""
    frame = EventBatchFrame(
        id="e" * 32,
        ts=datetime.now(UTC),
        payload=EventBatchPayload(
            events=[
                {
                    "id": "evt_oversize",
                    "kind": "received",
                    "engine": "celery",
                    "task_id": "task-1",
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "data": {"blob": "x" * 4096},
                },
            ],
        ),
    )
    return serialize_frame(frame)


def _projection_payload(sequence: int, adapter_instance_id: str) -> bytes:
    return json.dumps(
        {"sequence": sequence, "adapter_instance_id": adapter_instance_id},
        sort_keys=True,
    ).encode()


class _FakeFramework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover
        pass


class _RecordingWs:
    """Stands in for the websockets connection; records what reached the wire."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    async def send(self, data: bytes) -> None:
        self.sent.append(data)


class _RejectingLongPoll:
    """Long-poll shaped transport whose brain rejects every POST's content."""

    confirm_on_send = True
    heartbeat_interval = 10

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        self.calls += 1
        raise self.error


def _runtime(tmp_path: Path, store: BufferStore) -> AgentRuntime:
    config = Config(
        brain_url="https://brain.example.com",
        token=SecretStr("test-token-12345678901234567890"),
        project_id="test",
        buffer_path=tmp_path / "unused.sqlite",
        dev_mode=True,
        autostart=False,
        hmac_secret=SecretStr(secrets.token_hex(32)),
    )
    runtime = AgentRuntime(
        config=config,
        framework=_FakeFramework(),  # type: ignore[arg-type]
        engines=[],
    )
    runtime._stop_event = asyncio.Event()
    runtime._buffer = store
    runtime._heartbeat = None
    return runtime


def _websocket(max_frame_bytes: int) -> tuple[WebSocketTransport, _RecordingWs]:
    secret = secrets.token_bytes(32)
    transport = WebSocketTransport(
        brain_url="http://brain.local",
        token="tok",
        project_id="11111111-1111-1111-1111-111111111111",
        framework_name="celery",
        engines=["celery"],
        schedulers=[],
        capabilities={},
        hmac_secret=secret,
    )
    transport._signer = FrameSigner(
        secret=secret,
        agent_id="22222222-2222-2222-2222-222222222222",
        project_id="11111111-1111-1111-1111-111111111111",
        session_id="33333333-3333-3333-3333-333333333333",
    )
    ws = _RecordingWs()
    transport._ws = ws  # type: ignore[assignment]
    transport._outbound_max_frame_bytes = max_frame_bytes
    return transport, ws


async def _drive_send_loop(runtime: AgentRuntime, done: Callable[[], bool]) -> None:
    task = asyncio.create_task(runtime._run_send_loop())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    try:
        while loop.time() < deadline:
            if done():
                break
            await asyncio.sleep(0.02)
    finally:
        runtime._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)


async def _reports(store: BufferStore) -> list:
    """Enqueue one heartbeat and one agent_status through the real path."""
    heartbeat = Heartbeat(
        buffer=store,
        stop_event=asyncio.Event(),
        status_provider=lambda: {"buffer_depth": 0},
    )
    await heartbeat._enqueue_heartbeat()
    await heartbeat._enqueue_agent_status()
    return [parse_frame(entry.payload) for entry in store.drain(10)]


def test_discard_counts_each_entry_once_by_kind(tmp_path: Path) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        ids = [
            store.append("event_batch", batch(3)),
            store.append("command_result", b"{}"),
            store.append("agent_status", b"{}"),
        ]
        kept = store.append("heartbeat", b"{}")

        assert store.discard(ids, reason="content_rejected_frames") == 3
        # A stale repeat finds nothing left to count.
        assert store.discard(ids, reason="content_rejected_frames") == 0

        loss = store.loss_snapshot()
        assert loss["content_rejected_frames"] == 3
        assert loss["event_records"] == 3
        assert loss["command_results"] == 1
        assert loss["other_frames"] == 1
        assert loss["capacity_evicted_frames"] == 0
        assert [entry.id for entry in store.drain(10)] == [kept]
        assert store.size() == 1
    finally:
        store.close()


def test_discard_never_removes_a_causal_projection(tmp_path: Path) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        projection_id, _sequence, _adapter_id = store.append_external_schedule_projection(
            owner="apscheduler",
            source_scope="default",
            stream_id="11111111-1111-4111-8111-111111111111",
            epoch_uuid="22222222-2222-4222-8222-222222222222",
            epoch_number=7,
            adapter_instance_id="brain-issued-adapter-1",
            build_payload=_projection_payload,
        )

        assert store.discard([projection_id], reason="content_rejected_frames") == 0
        assert [entry.id for entry in store.drain(10)] == [projection_id]
        assert store.loss_snapshot()["content_rejected_frames"] == 0
        # A category is not a reason; misattributed frames would corrupt it.
        with pytest.raises(ValueError, match="loss reason"):
            store.discard([projection_id], reason="event_records")
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "category", "error"),
    [
        ("command_result", "command_results", UploadContentRejectedError("rejected")),
        ("agent_status", "other_frames", PayloadTooLargeError("too large")),
    ],
)
async def test_isolated_control_frame_rejection_is_reported_loss(
    tmp_path: Path,
    kind: str,
    category: str,
    error: Exception,
) -> None:
    """Long-poll: an isolated control frame the brain rejects is still dropped
    on the first rejection, and that drop now reaches the loss reports."""
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        store.append(kind, b"{}")
        (entry,) = store.drain(1)
        runtime = _runtime(tmp_path, store)
        transport = _RejectingLongPoll(error)
        runtime._transport = transport  # type: ignore[assignment]

        await _drive_send_loop(runtime, lambda: store.size() == 0)

        assert transport.calls == 1
        assert store.size() == 0
        # A stale second rejection of the same entry cannot count it again.
        runtime._handle_content_reject(store, [entry], error)
        loss = store.loss_snapshot()
        assert loss["content_rejected_frames"] == 1
        assert loss[category] == 1
        assert loss["event_records"] == 0

        heartbeat, status = await _reports(store)
        for frame in (heartbeat, status):
            assert frame.payload.telemetry_loss.content_rejected_frames == 1
            assert getattr(frame.payload.telemetry_loss, category) == 1
        # No event records were lost, so the legacy counter stays zero.
        assert heartbeat.payload.dropped_events == 0
    finally:
        store.close()


@pytest.mark.asyncio
async def test_websocket_local_drops_are_reported_loss(tmp_path: Path) -> None:
    """WebSocket: oversize and unparseable frames are purged unsent, and each
    is counted by kind; the frame that did ship is a delivery, not loss."""
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        store.append("event_batch", oversize_event_batch())
        store.append("command_result", UNREADABLE)
        store.append("heartbeat", heartbeat_frame())
        runtime = _runtime(tmp_path, store)
        transport, ws = _websocket(max_frame_bytes=2048)
        runtime._transport = transport

        await _drive_send_loop(runtime, lambda: store.size() == 0)

        # Only the heartbeat reached the wire, and nothing awaits an ack.
        assert len(ws.sent) == 1
        assert runtime._pending_acks == {}
        assert store.size() == 0
        loss = store.loss_snapshot()
        assert loss["content_rejected_frames"] == 2
        assert loss["event_records"] == 1
        assert loss["command_results"] == 1
        assert loss["other_frames"] == 0

        heartbeat, status = await _reports(store)
        assert heartbeat.payload.telemetry_loss == status.payload.telemetry_loss
        assert heartbeat.payload.telemetry_loss.content_rejected_frames == 2
        assert heartbeat.payload.telemetry_loss.event_records == 1
        assert heartbeat.payload.telemetry_loss.command_results == 1
        assert heartbeat.payload.dropped_events == 1
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("confirm_on_send", [False, True], ids=["websocket", "longpoll"])
async def test_repeated_undeliverable_report_counts_once(
    tmp_path: Path,
    confirm_on_send: bool,
) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        store.append("event_batch", batch(2))
        store.append("heartbeat", heartbeat_frame())
        entries = store.drain(10)
        runtime = _runtime(tmp_path, store)
        runtime._transport = SimpleNamespace(confirm_on_send=confirm_on_send)  # type: ignore[assignment]
        report = UndeliverableFrameError("refused", accepted=[1], drop_indices=[0])

        assert runtime._handle_undeliverable_drop(store, entries, report) is False
        # The same stale report again must not count the frame twice.
        assert runtime._handle_undeliverable_drop(store, entries, report) is False

        assert store.size() == 0
        loss = store.loss_snapshot()
        assert loss["content_rejected_frames"] == 1
        assert loss["event_records"] == 2
        # The accepted heartbeat is a delivery, not loss.
        assert loss["other_frames"] == 0
    finally:
        store.close()
