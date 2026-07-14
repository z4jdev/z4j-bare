"""Deterministic repro: silent event_batch loss across a WS reconnect.

Safety property under test (the delivery contract from runtime.py's
own comments): an ``event_batch`` buffer entry may only be deleted
after ITS frame id was acked by the brain in the CURRENT session,
or after the entry was re-sent and acked later. "Handed to the
socket" is never sufficient.

Live incident this pins (1.7 matrix, django scenario): a celery
worker agent sent batch 1 (task received) which landed and was
acked; the brain then closed the socket with 4403 (frame ts skew)
while batch 2 (started/succeeded) was in flight; after the agent
auto-reconnected, those events were never re-sent, the local buffer
sqlite was empty (entries=0), and the brain task row is permanently
stuck in state=received.

Mechanism reproduced here, step by step:

1. Session 1: batches 1 and 2 are drained and sent in one call.
   The brain acks batch 1 only; the ack for batch 2 is lost with
   the connection.
2. The connection dies; the supervisor reconnects. The reconnect
   reset clears ``_pending_acks`` (batch 2 is no longer tracked as
   in-flight), but batch 2's buffer entry is untouched -- it was
   never confirmed, because on WebSocket a frame is confirmed ONLY
   by its ack, never by a socket write.
3. Session 2: batch 2 re-drains and is re-sent (accepted by the
   socket layer) but the brain never acks it this session. Real
   producers of that no-ack outcome: wire-layer rejection (the
   observed 4403 ts-skew close), ingest commit failure (the brain
   withholds the ack on a transient DB skip by design), the brain's
   fire-and-forget ack task failing, or a half-open socket that
   buffers writes locally without delivering them.
4. The send loop then stalls in ``send_frames`` (here: a scripted
   block; live: TCP backpressure on the half-open socket). While an
   entry awaits its ack the in-flight drain filter keeps it out of
   subsequent drains, and its ``_pending_acks`` sent_at is recorded
   exactly once (a re-send can never slide the deadline), so the
   watchdog deadline is reached even under a resend storm.
5. The ack watchdog finds the stale pending entry and RE-QUEUES it:
   it removes the entry from ``_pending_acks`` so the send loop
   re-drains and re-sends it, and it NEVER confirms (deletes) it and
   NEVER drops it on a counter. The only DELETE of an event_batch is
   ``buffer.confirm`` on a real ack, or a bounded per-frame content
   quarantine. So an unacked batch 2 can never be silently lost --
   it is retried until the brain acks it or the buffer's size/byte
   overflow (oldest-first, logged) evicts it as the sole backstop.

The historical bug this pins: the watchdog used to infer that a
brain "does not speak acks" from ack-absence and confirm the stale
entry via ``buffer.confirm``, deleting it with zero delivery
guarantee. The redesign removed that inference entirely. The test
FAILS if any code path deletes batch 2 without its frame id having
been acked, and passes when the safety property holds.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import SecretStr
from z4j_bare import runtime as runtime_mod
from z4j_bare.buffer import BufferStore
from z4j_bare.runtime import AgentRuntime
from z4j_core.models import Config
from z4j_core.transport.frames import (
    EventBatchAckFrame,
    EventBatchAckPayload,
    EventBatchFrame,
    EventBatchPayload,
    serialize_frame,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    import pytest
    from z4j_core.transport.frames import Frame

BATCH1_FRAME_ID = "ev_batch1_received"
BATCH2_FRAME_ID = "ev_batch2_started_succeeded"


class _FakeFramework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover
        pass


def _make_ack(frame_id: str) -> EventBatchAckFrame:
    return EventBatchAckFrame(
        id=f"eba_{frame_id}"[:64],
        ts=datetime.now(UTC),
        payload=EventBatchAckPayload(
            acked_id=frame_id,
            received=1,
            accepted=1,
            rejected=0,
        ),
    )


def _event_batch_payload(frame_id: str, kind: str, task_id: str) -> bytes:
    frame = EventBatchFrame(
        id=frame_id,
        ts=datetime.now(UTC),
        payload=EventBatchPayload(
            events=[
                {
                    "id": f"evt_{frame_id}",
                    "kind": kind,
                    "engine": "celery",
                    "task_id": task_id,
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "data": {},
                },
            ],
        ),
    )
    return serialize_frame(frame)


class ScriptedBrainTransport:
    """Fake transport scripting the observed live-matrix sequence.

    Session 1: accept both batches on the first send, deliver an ack
    for batch 1 only (the brain acks only the first-seen frame id;
    batch 2's ack is lost with the connection), then raise
    ``ConnectionError`` once the agent has processed that ack.

    Session 2: accept the re-sent duplicate of batch 2 once, never
    ack it, then block every subsequent send forever (half-open
    socket). Blocking also stops the runtime's per-send refresh of
    the pending-ack timestamp, letting the watchdog deadline expire
    the way a stalled connection does in production.
    """

    heartbeat_interval = 10

    def __init__(self) -> None:
        self.session = 0
        self._calls_this_session = 0
        self._inbound: asyncio.Queue[Frame] = asyncio.Queue()
        self.seen_frame_ids: set[str] = set()
        #: Frame ids whose ack was fully delivered to (and processed
        #: by) the agent, per the scripted brain. The safety property
        #: is checked against this set.
        self.delivered_ack_ids: set[str] = set()
        self.session1_ack_processed = asyncio.Event()
        self.session2_first_send = asyncio.Event()
        self._stall_forever = asyncio.Event()

    async def connect(self) -> None:
        self.session += 1
        self._calls_this_session = 0
        self._inbound = asyncio.Queue()

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        self._calls_this_session += 1
        frame_ids = [json.loads(raw)["id"] for raw in frames]

        if self.session == 1:
            if self._calls_this_session == 1:
                self.seen_frame_ids.update(frame_ids)
                if BATCH1_FRAME_ID in frame_ids:
                    await self._inbound.put(_make_ack(BATCH1_FRAME_ID))
                return list(range(len(frames)))
            # Deterministic ordering: kill the connection only after
            # the agent has processed batch 1's ack, so session 1 ends
            # with batch 1 confirmed and batch 2 pending-unacked (its
            # _pending_acks entry about to be cleared by the reconnect).
            await self.session1_ack_processed.wait()
            raise ConnectionError("brain closed the socket: 4403 frame ts skew")

        # Session 2+ (after supervisor reconnect).
        if self._calls_this_session == 1:
            self.seen_frame_ids.update(frame_ids)
            self.session2_first_send.set()
            return list(range(len(frames)))
        # Half-open socket: subsequent writes never complete.
        await self._stall_forever.wait()
        raise AssertionError("unreachable")  # pragma: no cover

    async def receive_frames(
        self,
        on_frame: Callable[[Frame], Awaitable[None]],
    ) -> None:
        while True:
            frame = await self._inbound.get()
            await on_frame(frame)
            acked_id = frame.payload.acked_id  # type: ignore[union-attr]
            self.delivered_ack_ids.add(acked_id)
            if acked_id == BATCH1_FRAME_ID:
                self.session1_ack_processed.set()


async def test_unacked_event_batch_survives_reconnect_and_watchdog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Shrink the watchdog timings so the 90s production deadline
    # becomes fractions of a second, and the reconnect backoff floor
    # so the supervisor reconnects immediately.
    monkeypatch.setattr(runtime_mod, "_ACK_DEADLINE_SECONDS", 0.2)
    monkeypatch.setattr(runtime_mod, "_ACK_WATCHDOG_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(runtime_mod, "_RECONNECT_INITIAL", 0.01)

    buffer = BufferStore(tmp_path / "repro-buf.sqlite")
    entry1_id = buffer.append(
        "event_batch",
        _event_batch_payload(BATCH1_FRAME_ID, "task.received", "t-1"),
    )
    entry2_id = buffer.append(
        "event_batch",
        _event_batch_payload(BATCH2_FRAME_ID, "task.succeeded", "t-1"),
    )

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
    transport = ScriptedBrainTransport()

    # Wire the runtime's privates directly and drive the supervisor
    # coroutine on this loop; no background thread, no real socket.
    runtime._stop_event = asyncio.Event()
    runtime._reconnect_now = asyncio.Event()
    runtime._buffer = buffer
    runtime._transport = transport  # type: ignore[assignment]
    runtime._dispatcher = object()  # type: ignore[assignment]  # no CommandFrames in this script
    runtime._heartbeat = None

    supervisor = asyncio.create_task(runtime._supervise())
    violated = False
    try:
        await asyncio.wait_for(transport.session2_first_send.wait(), timeout=5.0)

        # Harness sanity checks (not the property under test):
        # batch 1 was acked in session 1 and legitimately confirmed;
        # batch 2 is still buffered at the moment session 2 re-sent it.
        remaining = {e.id for e in buffer.drain(10)}
        assert entry1_id not in remaining, (
            "harness: batch 1 should have been confirmed via its session-1 ack"
        )
        assert entry2_id in remaining, (
            "harness: batch 2 must still be buffered right after its re-send"
        )

        # SAFETY PROPERTY: at all times, the batch 2 buffer entry may
        # only disappear after BATCH2_FRAME_ID was acked. The scripted
        # brain never acks it, so the entry must survive the watchdog
        # deadline (0.2s here; 90s in production).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 1.5
        while loop.time() < deadline:
            still_buffered = any(e.id == entry2_id for e in buffer.drain(10))
            acked = BATCH2_FRAME_ID in transport.delivered_ack_ids
            if not still_buffered and not acked:
                violated = True
                break
            await asyncio.sleep(0.02)
    finally:
        runtime._stop_event.set()
        try:
            await asyncio.wait_for(supervisor, timeout=5.0)
        finally:
            buffer.close()

    assert not violated, (
        "SILENT EVENT LOSS: the buffer entry for batch 2 (frame id "
        f"{BATCH2_FRAME_ID!r}) was deleted although its frame id was never "
        "acked in the current session. On WebSocket an event_batch is "
        "confirmed ONLY by its ack; a stale un-acked entry must be "
        "re-queued by the watchdog (removed from _pending_acks so it "
        "re-drains and re-sends), never confirmed and never dropped on a "
        "counter. If it vanished, some path deleted an unacked batch and "
        "the brain never received the started/succeeded events, so the "
        "task row stays stuck in state=received."
    )


class _CountingNoAckTransport:
    """Accepts one send, never acks, counts every send_frames call.

    Models the R5-M2 storm scenario: a single unacked event_batch. With
    the in-flight drain filter the send loop must send it ONCE and then
    stop re-draining it (the entry is in _pending_acks), so send_frames
    is called exactly once, not hundreds of times per second.
    """

    heartbeat_interval = 10

    def __init__(self) -> None:
        self.send_calls = 0
        self.frames_sent: list[str] = []
        self._inbound: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> None:
        return None

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        self.send_calls += 1
        for raw in frames:
            self.frames_sent.append(json.loads(raw)["id"])
        return list(range(len(frames)))

    async def receive_frames(self, on_frame) -> None:
        while True:
            await on_frame(await self._inbound.get())


async def test_unacked_batch_is_sent_once_not_stormed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R5-M2: an in-flight (sent, unacked) entry is not re-drained.

    Pre-fix the non-destructive drain + no in-flight filter re-sent the
    same entry every loop iteration (367 sends in 80ms observed live).
    """
    monkeypatch.setattr(runtime_mod, "_ACK_DEADLINE_SECONDS", 60.0)
    monkeypatch.setattr(runtime_mod, "_ACK_WATCHDOG_INTERVAL_SECONDS", 0.05)

    buffer = BufferStore(tmp_path / "m2-buf.sqlite")
    buffer.append("event_batch", _event_batch_payload(BATCH1_FRAME_ID, "task.received", "t-1"))

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
    transport = _CountingNoAckTransport()
    runtime._stop_event = asyncio.Event()
    runtime._reconnect_now = asyncio.Event()
    runtime._buffer = buffer
    runtime._transport = transport  # type: ignore[assignment]
    runtime._dispatcher = object()  # type: ignore[assignment]
    runtime._heartbeat = None

    supervisor = asyncio.create_task(runtime._supervise())
    try:
        # Give the send loop many iterations. Pre-fix this window would
        # have produced hundreds of sends of the same frame.
        await asyncio.sleep(0.6)
    finally:
        runtime._stop_event.set()
        try:
            await asyncio.wait_for(supervisor, timeout=5.0)
        finally:
            buffer.close()

    assert transport.send_calls == 1, (
        f"expected the unacked batch to be sent exactly once, got "
        f"{transport.send_calls} sends of {transport.frames_sent}"
    )
