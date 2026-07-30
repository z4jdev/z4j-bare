"""External round-8 H5 + round-9 H2: the agent DROPS a frame it locally
determines is undeliverable -- oversize (signs larger than the brain-advertised
ceiling), unparseable (buffer corruption / schema drift), or unsigned.

Such a frame is deterministically undeliverable; on reconnect the agent would
re-send it, wedging the buffer behind a frame that can never leave.

send_frames signals these via UndeliverableFrameError (NOT a ConnectionError --
the socket is healthy). The runtime FORCE-PURGES them from the buffer.
Critically, it must NOT route them through the normal
"accepted -> _confirm_or_register" path: on the WS defer-acks transport an
event_batch payload can still peek to a real frame_id (even schema-invalid JSON
that fails full parse), so registering it would defer an event_batch_ack that
never arrives and pin the buffer head forever (the exact loop this drop exists
to eliminate).
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import SecretStr
from z4j_bare.buffer import BufferStore
from z4j_bare.runtime import AgentRuntime
from z4j_bare.transport.websocket import (
    UndeliverableFrameError,
    WebSocketTransport,
)
from z4j_core.errors import ProtocolError
from z4j_core.models import Config
from z4j_core.transport.frames import (
    ErrorFrame,
    ErrorPayload,
    EventBatchFrame,
    EventBatchPayload,
    HeartbeatFrame,
    HeartbeatPayload,
    serialize_frame,
)
from z4j_core.transport.framing import FrameSigner, FrameVerifier

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.asyncio


class _RecordingWs:
    """Minimal stand-in for the websockets ClientConnection: records every
    frame handed to ``send`` so the test can assert an oversize frame was
    never put on the wire."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    async def send(self, data: bytes) -> None:
        self.sent.append(data)


class _InboundWs:
    def __init__(self, frames: list[bytes]) -> None:
        self._frames = iter(frames)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._frames)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self) -> None:
        self.closed = True


def _make_transport() -> WebSocketTransport:
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
    return transport


def _event_batch(data_len: int, frame_id: str = "f" * 32) -> bytes:
    """Serialise an unsigned event_batch whose single event carries a
    ``blob`` of ``data_len`` bytes, so the caller can dial the signed size
    above or below the ceiling."""
    frame = EventBatchFrame(
        id=frame_id,
        ts=datetime.now(UTC),
        payload=EventBatchPayload(
            events=[
                {
                    "id": "evt_oversize",
                    "kind": "received",
                    "engine": "celery",
                    "task_id": "task-1",
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "data": {"blob": "x" * data_len},
                },
            ],
        ),
    )
    return serialize_frame(frame)


# ---------------------------------------------------------------------------
# Transport-level: send_frames raises UndeliverableFrameError, never sends the
# undeliverable frame
# ---------------------------------------------------------------------------


async def test_oversize_frame_raises_and_is_not_sent() -> None:
    transport = _make_transport()
    ws = _RecordingWs()
    transport._ws = ws  # type: ignore[assignment]
    # Tiny ceiling so any real frame overshoots it.
    transport._outbound_max_frame_bytes = 256

    raw = _event_batch(4096)
    with pytest.raises(UndeliverableFrameError) as exc_info:
        await transport.send_frames([raw])

    # The oversize frame is reported for force-purge, NOT put in ``accepted``
    # (which on WS would defer an ack that never comes) ...
    assert exc_info.value.drop_indices == [0]
    assert exc_info.value.accepted == []
    # ... and it was never actually put on the wire.
    assert ws.sent == []


async def test_fatal_upgrade_error_closes_websocket_and_raises_protocol_error() -> None:
    secret = secrets.token_bytes(32)
    agent_id = "22222222-2222-2222-2222-222222222222"
    project_id = "11111111-1111-1111-1111-111111111111"
    session_id = "33333333-3333-3333-3333-333333333333"
    signer = FrameSigner(
        secret=secret,
        agent_id=agent_id,
        project_id=project_id,
        session_id=session_id,
    )
    error = signer.sign_and_serialize(
        ErrorFrame(
            id="err_upgrade",
            payload=ErrorPayload(
                code="scheduler_upgrade_required",
                message="upgrade the scheduler adapter",
                fatal=True,
            ),
        ),
    )
    ws = _InboundWs([error])
    transport = WebSocketTransport(
        brain_url="http://brain.local",
        token="tok",
        project_id=project_id,
        framework_name="celery",
        engines=["celery"],
        schedulers=[],
        capabilities={},
        hmac_secret=secret,
    )
    transport._ws = ws  # type: ignore[assignment]
    transport._verifier = FrameVerifier(
        secret=secret,
        agent_id=agent_id,
        project_id=project_id,
        session_id=session_id,
        direction="brain->agent",
    )

    async def _unused(_frame) -> None:
        raise AssertionError("fatal errors must not reach the dispatcher")

    with pytest.raises(ProtocolError, match="scheduler_upgrade_required"):
        await transport.receive_frames(_unused)
    assert ws.closed is True


async def test_unparseable_frame_raises_for_purge_not_accepted() -> None:
    # round-9 H2: a buffered frame that FAILS full parse but is still valid
    # JSON with an extractable ``id`` (schema-drift backlog) must be reported
    # for force-purge, NOT ``accepted`` -- otherwise the runtime peeks its id
    # and registers it for an ack that never comes.
    transport = _make_transport()
    ws = _RecordingWs()
    transport._ws = ws  # type: ignore[assignment]
    transport._outbound_max_frame_bytes = 1_048_576

    # Valid JSON, has an ``id``, but not a valid frame (unknown type / no
    # payload) -> parse_frame raises, _peek_frame_id would still return the id.
    bad = b'{"id": "ev_schema_drift", "type": "from_the_future", "payload": {}}'
    with pytest.raises(UndeliverableFrameError) as exc_info:
        await transport.send_frames([bad])

    assert exc_info.value.drop_indices == [0]
    assert exc_info.value.accepted == []
    assert ws.sent == []


async def test_deliverable_frame_still_ships() -> None:
    transport = _make_transport()
    ws = _RecordingWs()
    transport._ws = ws  # type: ignore[assignment]
    # Generous ceiling so a normal frame passes.
    transport._outbound_max_frame_bytes = 1_048_576

    raw = _event_batch(8)
    accepted = await transport.send_frames([raw])

    assert accepted == [0]
    assert len(ws.sent) == 1


async def test_oversize_is_purged_but_rest_of_batch_ships() -> None:
    # The oversize frame in the middle of a batch is reported for purge; the
    # small frames on either side still make it onto the wire (the drop does
    # not abort the batch).
    transport = _make_transport()
    ws = _RecordingWs()
    transport._ws = ws  # type: ignore[assignment]
    transport._outbound_max_frame_bytes = 2048

    small_a = _event_batch(8, frame_id="a" * 32)
    big = _event_batch(8192, frame_id="b" * 32)
    small_b = _event_batch(8, frame_id="c" * 32)
    with pytest.raises(UndeliverableFrameError) as exc_info:
        await transport.send_frames([small_a, big, small_b])

    # The two small frames shipped (accepted); the big one is flagged purge.
    assert exc_info.value.accepted == [0, 2]
    assert exc_info.value.drop_indices == [1]
    assert len(ws.sent) == 2


# ---------------------------------------------------------------------------
# Runtime-level (the re-review regression): an oversize event_batch on the WS
# defer-acks transport is PURGED from the buffer, NOT registered awaiting an
# ack that never arrives.
# ---------------------------------------------------------------------------


class _FakeFramework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover
        pass


class _UndeliverableTransport:
    """A WS-style (defer-acks) transport whose send_frames always reports the
    whole batch as undeliverable -- models an event whose data blob pushes its
    one-event frame over the brain ceiling (or a schema-drift backlog frame)."""

    confirm_on_send = False  # WebSocket semantics: acks are deferred
    heartbeat_interval = 10

    def __init__(self) -> None:
        self.calls = 0

    async def connect(self) -> None:  # pragma: no cover - not driven here
        return None

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        self.calls += 1
        raise UndeliverableFrameError(
            "undeliverable",
            accepted=[],
            drop_indices=list(range(len(frames))),
        )

    async def receive_frames(self, on_frame) -> None:  # pragma: no cover
        # Never delivers an inbound frame; blocks until cancelled.
        await asyncio.Event().wait()


async def test_runtime_purges_undeliverable_event_batch_without_registering(
    tmp_path: Path,
) -> None:
    """The re-review finding + round-9 H2: on the WS defer-acks path, an
    undeliverable event_batch reported via UndeliverableFrameError must be
    buffer.confirm'd (purged) and NEVER registered in _pending_acks (which
    would await an ack that can never arrive -> infinite ~90s resend loop +
    buffer-head pin)."""
    buffer = BufferStore(tmp_path / "oversize-buf.sqlite")
    entry_id = buffer.append("event_batch", _event_batch(8, frame_id="e" * 32))

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
    runtime._buffer = buffer
    runtime._transport = _UndeliverableTransport()  # type: ignore[assignment]
    runtime._heartbeat = None

    task = asyncio.create_task(runtime._run_send_loop())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    try:
        # Drive the loop until the oversize entry is purged (buffer empties).
        while loop.time() < deadline:
            if not buffer.drain(10):
                break
            await asyncio.sleep(0.02)
    finally:
        runtime._stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)

    # The oversize entry was PURGED from the buffer ...
    remaining = {e.id for e in buffer.drain(10)}
    assert entry_id not in remaining
    assert remaining == set()
    # ... and was NEVER registered awaiting an ack (the buffer-head-pin bug).
    assert runtime._pending_acks == {}
    buffer.close()


class _Entry:
    """Minimal buffer-entry stand-in for _confirm_or_register."""

    def __init__(self, entry_id: str, kind: str, payload: bytes) -> None:
        self.id = entry_id
        self.kind = kind
        self.payload = payload


def _make_runtime(tmp_path: Path) -> AgentRuntime:
    config = Config(
        brain_url="https://brain.example.com",
        token=SecretStr("test-token-12345678901234567890"),
        project_id="test",
        buffer_path=tmp_path / "unused.sqlite",
        dev_mode=True,
        autostart=False,
        hmac_secret=SecretStr(secrets.token_hex(32)),
    )
    return AgentRuntime(
        config=config,
        framework=_FakeFramework(),  # type: ignore[arg-type]
        engines=[],
    )


async def test_confirm_or_register_trusts_wire_type_not_buffer_kind(
    tmp_path: Path,
) -> None:
    """Round-10 + round-11 external: the ack-deferral keys off the WIRE type,
    not the buffer entry's ``kind`` metadata, in BOTH mislabel directions.

    round-10: a heartbeat payload mislabelled kind="event_batch" must be
    confirmed NOW (never registered for an ack the brain never emits).
    round-11: a REAL event_batch mislabelled kind="heartbeat" must still be
    DEFERRED (else it is confirmed on socket-write and silently lost if the
    brain restarts / rejects / drops the ack)."""
    runtime = _make_runtime(tmp_path)
    now = datetime.now(UTC)

    # round-10 direction: heartbeat payload (wire "heartbeat") under kind=
    # "event_batch" -> confirm now, not deferred.
    hb = HeartbeatFrame(id="hb_mislabelled", payload=HeartbeatPayload())
    hb_as_eb = _Entry("buf-hb", "event_batch", serialize_frame(hb))
    confirm_now = runtime._confirm_or_register([hb_as_eb], [0], defer_acks=True, now=now)
    assert confirm_now == ["buf-hb"]
    assert runtime._pending_acks == {}

    # round-11 RECIPROCAL direction: a REAL event_batch (wire "event_batch")
    # under kind="heartbeat" -> must be DEFERRED (registered), NOT confirmed.
    eb = EventBatchFrame(id="ev_hidden", payload=EventBatchPayload(events=[]))
    eb_as_hb = _Entry("buf-eb-hidden", "heartbeat", serialize_frame(eb))
    confirm_now2 = runtime._confirm_or_register([eb_as_hb], [0], defer_acks=True, now=now)
    assert confirm_now2 == []  # deferred, NOT confirmed on send
    assert "ev_hidden" in runtime._pending_acks

    # Sanity: a correctly-labelled REAL event_batch IS deferred.
    eb2 = EventBatchFrame(id="ev_real", payload=EventBatchPayload(events=[]))
    real = _Entry("buf-eb", "event_batch", serialize_frame(eb2))
    confirm_now3 = runtime._confirm_or_register([real], [0], defer_acks=True, now=now)
    assert confirm_now3 == []  # deferred
    assert "ev_real" in runtime._pending_acks

    # And a control frame (command_result) confirms on send even on WS.
    from z4j_core.transport.frames import CommandResultFrame, CommandResultPayload

    cr = CommandResultFrame(id="cmd_1", payload=CommandResultPayload(status="success", result={}))
    ctrl = _Entry("buf-cmd", "command_result", serialize_frame(cr))
    confirm_now4 = runtime._confirm_or_register([ctrl], [0], defer_acks=True, now=now)
    assert confirm_now4 == ["buf-cmd"]  # confirmed on send


async def test_backpressure_cap_is_wire_aware_not_kind_aware() -> None:
    """Round-12 external LOW: the in-flight cap must count a REAL event_batch
    mislabelled kind="heartbeat" (corruption / a bad migration), else a
    mislabelled batch bypasses _MAX_IN_FLIGHT_BATCHES -- kept + registered for an
    ack yet never counted against the cap. Both the cap selection and the
    ack-deferral must agree via the WIRE type."""
    from z4j_bare.runtime import _entry_is_event_batch

    def _eb(fid: str) -> bytes:
        return serialize_frame(EventBatchFrame(id=fid, payload=EventBatchPayload(events=[])))

    # A real event_batch stored under the wrong kind still reads as event_batch.
    mislabelled = _Entry("m1", "heartbeat", _eb("ev1"))
    assert _entry_is_event_batch(mislabelled) is True
    # A heartbeat under the wrong kind reads as NOT an event_batch.
    hb = HeartbeatFrame(id="hb1", payload=HeartbeatPayload())
    assert _entry_is_event_batch(_Entry("m2", "event_batch", serialize_frame(hb))) is False

    # _cap_event_batch_entries with max=1 must keep only ONE event_batch even
    # when all three are mislabelled kind="heartbeat" (the reviewer's probe).
    entries = [_Entry(f"m{i}", "heartbeat", _eb(f"ev{i}")) for i in range(3)]
    kept = AgentRuntime._cap_event_batch_entries(entries, 1)
    assert len(kept) == 1  # capped despite the wrong kind (was 3 pre-fix)

    # With max=0 (at cap), ALL mislabelled event_batches are excluded.
    kept0 = AgentRuntime._cap_event_batch_entries(entries, 0)
    assert kept0 == []
