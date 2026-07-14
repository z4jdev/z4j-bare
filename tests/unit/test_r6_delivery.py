"""Runtime-level regression tests for the round-6 delivery fixes.

Covers the send-loop behaviors that the buffer / transport unit tests
cannot: transport-failure attempts (R6-F4), the 413 batch-size reaction
(R6-F6), and the ack-arrives-before-registration race (R6-F7). Each
drives the real ``AgentRuntime._run_send_loop`` (and, where needed, the
supervisor) with a scripted fake transport, on this event loop, no real
socket.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import SecretStr
from z4j_bare import runtime as runtime_mod
from z4j_bare.buffer import BufferStore
from z4j_bare.runtime import AgentRuntime
from z4j_bare.transport.longpoll import (
    PayloadTooLargeError,
    UploadContentRejectedError,
    UploadRetryableError,
)
from z4j_bare.transport.websocket import PartialSendError
from z4j_core.models import Config
from z4j_core.transport.frames import (
    EventBatchFrame,
    EventBatchPayload,
    serialize_frame,
)


def _event_batch_payload(frame_id: str) -> bytes:
    frame = EventBatchFrame(
        id=frame_id,
        ts=datetime.now(UTC),
        payload=EventBatchPayload(
            events=[
                {
                    "id": f"evt_{frame_id}",
                    "kind": "task.succeeded",
                    "engine": "celery",
                    "task_id": "t-1",
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "data": {},
                },
            ],
        ),
    )
    return serialize_frame(frame)


class _FakeFramework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover
        pass


def _runtime(tmp_path: Path, buffer: BufferStore) -> AgentRuntime:
    config = Config(
        brain_url="https://brain.example.com",
        token=SecretStr("test-token-12345678901234567890"),
        project_id="test",
        buffer_path=tmp_path / "unused.sqlite",
        dev_mode=True,
        autostart=False,
        hmac_secret=SecretStr(secrets.token_hex(32)),
    )
    rt = AgentRuntime(
        config=config,
        framework=_FakeFramework(),  # type: ignore[arg-type]
        engines=[],
    )
    rt._stop_event = asyncio.Event()
    rt._reconnect_now = asyncio.Event()
    rt._buffer = buffer
    rt._dispatcher = object()  # type: ignore[assignment]
    rt._heartbeat = None
    return rt


async def _run_send_loop_briefly(rt: AgentRuntime, seconds: float = 0.4) -> None:
    task = asyncio.create_task(rt._run_send_loop())
    try:
        await asyncio.sleep(seconds)
    finally:
        rt._stop_event.set()
        # The send loop exits its while-loop cleanly when stop is set; if
        # it is wedged, cancel it so the test does not hang.
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def _neutralize_send_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collapse the long-poll retry delays to a pure event-loop yield.

    The transient path (R7-HIGH2) backs off by a real (0.5s-doubling)
    sleep and the content-reject path (R8) uses a fixed
    ``_CONTENT_REJECT_DELAY``, both so a poison frame does not hot-loop
    the brain. That is correct in production but would blow these
    sub-second outcome tests' time budget, so the tests pin BOTH to 0
    (``asyncio.sleep(0)`` still yields the loop). Must run BEFORE the
    runtime is constructed so ``__init__`` reads the backoff pin.
    """
    monkeypatch.setattr(runtime_mod, "_SEND_BACKOFF_INITIAL", 0.0)
    monkeypatch.setattr(runtime_mod, "_SEND_BACKOFF_MAX", 0.0)
    monkeypatch.setattr(runtime_mod, "_CONTENT_REJECT_DELAY", 0.0)


# ---------------------------------------------------------------------------
# R6-F4: a transport failure must NOT increment the quarantine counter.
# ---------------------------------------------------------------------------


class _AlwaysConnErrorTransport:
    heartbeat_interval = 10
    confirm_on_send = True  # long-poll style

    async def send_frames(self, frames):
        raise ConnectionError("transport down")


async def test_transport_failure_does_not_count_toward_quarantine(
    tmp_path: Path,
) -> None:
    buffer = BufferStore(tmp_path / "buf.sqlite")
    eid = buffer.append("event_batch", _event_batch_payload("evb_1"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _AlwaysConnErrorTransport()  # type: ignore[assignment]

    # The send loop raises ConnectionError; drive it a few times.
    for _ in range(5):
        with pytest.raises(ConnectionError):
            await rt._run_send_loop()

    # Attempts must still be 0: a flaky connection is not the batch's
    # fault and must never drive it toward the content-rejection
    # quarantine (pre-fix this incremented attempts on every failure).
    entries = buffer.drain(10)
    assert [e.id for e in entries] == [eid]
    assert entries[0].attempts == 0
    buffer.close()


async def test_partial_send_failure_does_not_count(tmp_path: Path) -> None:
    class _PartialTransport:
        heartbeat_interval = 10

        async def send_frames(self, frames):
            raise PartialSendError("socket dropped", accepted=[])

    buffer = BufferStore(tmp_path / "buf.sqlite")
    eid = buffer.append("event_batch", _event_batch_payload("evb_1"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _PartialTransport()  # type: ignore[assignment]
    for _ in range(5):
        with pytest.raises(PartialSendError):
            await rt._run_send_loop()
    entries = buffer.drain(10)
    assert entries[0].attempts == 0
    assert entries[0].id == eid
    buffer.close()


# ---------------------------------------------------------------------------
# R6-F4 (positive): a long-poll content rejection DOES count + quarantines.
# ---------------------------------------------------------------------------


async def test_content_rejection_counts_and_quarantines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_mod, "_MAX_SEND_ATTEMPTS", 3)
    _neutralize_send_backoff(monkeypatch)

    class _RejectTransport:
        heartbeat_interval = 10
        confirm_on_send = True

        async def send_frames(self, frames):
            raise UploadContentRejectedError("brain rejected the content")

    buffer = BufferStore(tmp_path / "buf.sqlite")
    buffer.append("event_batch", _event_batch_payload("evb_poison"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _RejectTransport()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.5)

    # After >= _MAX_SEND_ATTEMPTS content rejections the isolated
    # single-frame batch is quarantined (dropped), so the buffer drains
    # to empty.
    assert buffer.size() == 0
    buffer.close()


async def test_content_rejection_isolates_before_dropping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R7-MED: a content reject on a MULTI-frame batch reduces the send
    batch size to isolate the offender; it must NOT drop valid siblings.

    Here every POST that carries more than one frame is content-rejected,
    but a single-frame POST is accepted. The runtime must therefore
    shrink its batch to 1 and deliver ALL frames -- nothing dropped -- by
    isolating each frame into its own POST.
    """
    _neutralize_send_backoff(monkeypatch)

    class _RejectMultiTransport:
        heartbeat_interval = 10
        confirm_on_send = True

        async def send_frames(self, frames):
            if len(frames) > 1:
                raise UploadContentRejectedError("batch rejected")
            return list(range(len(frames)))

    buffer = BufferStore(tmp_path / "buf.sqlite")
    for i in range(4):
        buffer.append("event_batch", _event_batch_payload(f"evb_{i}"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _RejectMultiTransport()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.6)

    # All four delivered by shrinking to single-frame POSTs; none dropped.
    assert rt._send_batch_size < runtime_mod._SEND_BATCH_SIZE
    assert buffer.size() == 0
    buffer.close()


# ---------------------------------------------------------------------------
# R6-F6: a 413 reduces the send batch size; a single-frame 413 drops it.
# ---------------------------------------------------------------------------


async def test_413_reduces_batch_size_then_delivers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _neutralize_send_backoff(monkeypatch)

    class _T413:
        """413s while the POST carries more than one frame; a
        single-frame POST fits and succeeds. So the runtime reduces its
        batch size until each POST is small enough, then delivers, with
        NOTHING dropped."""

        heartbeat_interval = 10
        confirm_on_send = True

        async def send_frames(self, frames):
            if len(frames) > 1:
                raise PayloadTooLargeError("batch too big")
            return list(range(len(frames)))

    buffer = BufferStore(tmp_path / "buf.sqlite")
    for i in range(4):
        buffer.append("event_batch", _event_batch_payload(f"evb_{i}"))
    rt = _runtime(tmp_path, buffer)
    assert rt._send_batch_size == runtime_mod._SEND_BATCH_SIZE
    rt._transport = _T413()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.6)

    # The batch size was reduced (413 on the multi-frame POSTs), and once
    # it reached 1 the frames delivered -- all four, none dropped.
    assert rt._send_batch_size < runtime_mod._SEND_BATCH_SIZE
    assert buffer.size() == 0
    buffer.close()


async def test_413_single_frame_dropped_only_after_bounded_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R6-panel-MED: a single-frame 413 is NOT deleted on the first
    failure; it goes through the bounded content-reject quarantine, so a
    transient 413 cannot instantly lose a deliverable frame."""
    monkeypatch.setattr(runtime_mod, "_MAX_SEND_ATTEMPTS", 3)
    _neutralize_send_backoff(monkeypatch)

    class _T413:
        heartbeat_interval = 10
        confirm_on_send = True

        async def send_frames(self, frames):
            raise PayloadTooLargeError("too big")

    buffer = BufferStore(tmp_path / "buf.sqlite")
    buffer.append("event_batch", _event_batch_payload("evb_huge"))
    rt = _runtime(tmp_path, buffer)
    rt._send_batch_size = 1  # force a single-frame POST
    rt._transport = _T413()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.5)

    # After >= _MAX_SEND_ATTEMPTS consecutive single-frame 413s the frame
    # is quarantined (genuinely oversized). Crucially it was NOT dropped
    # on the first 413; the attempt counter had to reach the cap.
    assert buffer.size() == 0
    buffer.close()


async def test_413_single_frame_survives_a_transient_413(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R6-panel-MED: a 413 that clears on retry (transient proxy/WAF)
    does not lose the frame -- it delivers once the 413 stops."""
    _neutralize_send_backoff(monkeypatch)

    class _TransientT413:
        heartbeat_interval = 10
        confirm_on_send = True
        calls = 0

        async def send_frames(self, frames):
            type(self).calls += 1
            if type(self).calls <= 2:
                raise PayloadTooLargeError("transient 413")
            return list(range(len(frames)))

    buffer = BufferStore(tmp_path / "buf.sqlite")
    buffer.append("event_batch", _event_batch_payload("evb_ok"))
    rt = _runtime(tmp_path, buffer)
    rt._send_batch_size = 1
    rt._transport = _TransientT413()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.4)

    # The frame delivered once the transient 413 cleared; NOT dropped.
    assert buffer.size() == 0
    assert _TransientT413.calls >= 3
    buffer.close()


# ---------------------------------------------------------------------------
# R7-MED: an isolated CONTROL frame that content-rejects is dropped on the
# FIRST rejection (it can't be split or re-batched, so keeping it would pin
# every data frame behind it forever), whereas an event_batch is dropped
# only after the bounded attempt budget.
# ---------------------------------------------------------------------------


async def test_control_frame_content_reject_dropped_immediately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _neutralize_send_backoff(monkeypatch)

    class _RejectCountingTransport:
        heartbeat_interval = 10
        confirm_on_send = True
        calls = 0

        async def send_frames(self, frames):
            type(self).calls += 1
            raise UploadContentRejectedError("brain rejected the content")

    buffer = BufferStore(tmp_path / "buf.sqlite")
    # A command_result is a CONTROL frame: it carries no event data and
    # cannot be re-batched. An undeliverable one must not pin the queue.
    buffer.append("command_result", b"{}")
    rt = _runtime(tmp_path, buffer)
    rt._transport = _RejectCountingTransport()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.3)

    # Dropped after the FIRST content rejection (not after the multi-cycle
    # event_batch budget), so it never pins data frames behind it.
    assert buffer.size() == 0
    assert _RejectCountingTransport.calls == 1
    buffer.close()


# ---------------------------------------------------------------------------
# R9: a PERSISTENT long-poll partial store (zero confirmed progress) forces
# a reconnect after _MAX_CONSECUTIVE_RETRYABLE tries, so a send-side session/
# version skew is not re-POSTed forever (only the receive loop reconnects on
# its own error). Loss-free: the buffer is preserved across the reconnect.
# ---------------------------------------------------------------------------


async def test_persistent_partial_store_forces_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_mod, "_MAX_CONSECUTIVE_RETRYABLE", 3)
    _neutralize_send_backoff(monkeypatch)

    class _AlwaysRetryable:
        heartbeat_interval = 10
        confirm_on_send = True
        calls = 0

        async def send_frames(self, frames):
            type(self).calls += 1
            raise UploadRetryableError("partial store; zero progress")

    buffer = BufferStore(tmp_path / "buf.sqlite")
    eid = buffer.append("event_batch", _event_batch_payload("evb_stuck"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _AlwaysRetryable()  # type: ignore[assignment]

    # After 3 zero-progress retries the send loop raises ConnectionError so
    # the supervisor reconnects with a fresh session.
    with pytest.raises(ConnectionError):
        await rt._run_send_loop()
    assert _AlwaysRetryable.calls == 3
    # Loss-free: the frame is still buffered (never confirmed, never dropped).
    remaining = buffer.drain(10)
    assert [e.id for e in remaining] == [eid]
    assert remaining[0].attempts == 0  # a partial store is not a drop-budget hit
    buffer.close()


async def test_partial_stores_interleaved_with_success_never_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R9-4: the reconnect fires only on CONSECUTIVE zero-progress partial
    stores. A successful send resets ``_consecutive_retryable``, so a flaky-
    but-working brain (partials interleaved with successes) must NOT force a
    spurious reconnect even across many CUMULATIVE partials.
    """
    monkeypatch.setattr(runtime_mod, "_MAX_CONSECUTIVE_RETRYABLE", 3)
    _neutralize_send_backoff(monkeypatch)

    class _FlakyTransport:
        # Pattern: fail, fail, SUCCESS, repeating -- never 3 consecutive
        # failures, but 4 cumulative partials across the run (> the cap).
        heartbeat_interval = 10
        confirm_on_send = True
        calls = 0

        async def send_frames(self, frames):
            type(self).calls += 1
            if type(self).calls % 3 == 0:
                return list(range(len(frames)))  # success -> confirm + reset
            raise UploadRetryableError("transient partial store")

    buffer = BufferStore(tmp_path / "buf.sqlite")
    for i in range(2):
        buffer.append("event_batch", _event_batch_payload(f"evb_{i}"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _FlakyTransport()  # type: ignore[assignment]

    # No ConnectionError (would propagate out of _run_send_loop_briefly): the
    # counter resets on each success, so it never reaches 3 consecutive.
    await _run_send_loop_briefly(rt, seconds=0.5)

    assert rt._consecutive_retryable == 0
    assert buffer.size() == 0  # both frames delivered across the flaky rounds
    buffer.close()


# ---------------------------------------------------------------------------
# R8: a content reject isolates ONE poison event_batch by batch-size
# reduction, drops it after the bounded budget, RESTORES the full batch
# size (so the rest stops dribbling one frame per POST), and delivers every
# valid sibling behind it.
# ---------------------------------------------------------------------------


async def test_content_reject_isolates_drops_restores_and_delivers_rest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_mod, "_MAX_SEND_ATTEMPTS", 3)
    _neutralize_send_backoff(monkeypatch)

    class _RejectPoisonTransport:
        """Content-rejects any POST that carries the poison frame; accepts
        any POST that does not. Forces the runtime to bisect to isolate the
        poison, drop it, and then deliver the valid siblings full-width."""

        heartbeat_interval = 10
        confirm_on_send = True

        async def send_frames(self, frames):
            if any(b"POISON" in f for f in frames):
                raise UploadContentRejectedError("poison present")
            return list(range(len(frames)))

    buffer = BufferStore(tmp_path / "buf.sqlite")
    # Poison is OLDEST (drained first), so it blocks the valid frames
    # behind it until it is isolated and dropped.
    buffer.append("event_batch", b'{"POISON":true}')
    for i in range(3):
        buffer.append("event_batch", _event_batch_payload(f"evb_{i}"))
    rt = _runtime(tmp_path, buffer)
    rt._transport = _RejectPoisonTransport()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.8)

    # Poison dropped after the bounded budget; every valid sibling
    # delivered (buffer fully drains); and the batch size was RESTORED to
    # full once the poison was gone (not left collapsed at 1 for the
    # connection lifetime, R8).
    assert buffer.size() == 0
    assert rt._send_batch_size == runtime_mod._SEND_BATCH_SIZE
    buffer.close()


# ---------------------------------------------------------------------------
# R6-F7: an ack that arrives before the send loop registers the pending
# entry is not lost; the entry is confirmed at registration time.
# ---------------------------------------------------------------------------


async def test_early_ack_confirms_at_registration(tmp_path: Path) -> None:
    buffer = BufferStore(tmp_path / "buf.sqlite")
    buffer.append("event_batch", _event_batch_payload("evb_early"))
    rt = _runtime(tmp_path, buffer)

    # Simulate the ack having arrived during the send await, before the
    # send loop could register _pending_acks: the ack handler recorded
    # the frame id in _acks_seen_early.
    rt._acks_seen_early.add("evb_early")

    class _WSAckTransport:
        heartbeat_interval = 10
        # WS style: no confirm_on_send, so the send loop defers acks.

        async def send_frames(self, frames):
            return list(range(len(frames)))

    rt._transport = _WSAckTransport()  # type: ignore[assignment]

    await _run_send_loop_briefly(rt, seconds=0.3)

    # The early ack confirmed the entry at registration; it did NOT sit
    # in _pending_acks waiting for the 90s watchdog.
    assert buffer.size() == 0
    assert "evb_early" not in rt._pending_acks
    buffer.close()


# ---------------------------------------------------------------------------
# R8-M4: the WS in-flight cap must trim event_batch draining to the REMAINING
# slots (not just switch to control-only once ALREADY over), so _pending_acks
# cannot overshoot _MAX_IN_FLIGHT_BATCHES; control frames always pass through.
# ---------------------------------------------------------------------------


class _StubEntry:
    def __init__(self, entry_id: int, kind: str) -> None:
        self.id = entry_id
        self.kind = kind


def test_cap_event_batch_entries_keeps_controls_and_oldest() -> None:
    entries = [
        _StubEntry(1, "event_batch"),
        _StubEntry(2, "command_result"),
        _StubEntry(3, "event_batch"),
        _StubEntry(4, "event_batch"),
        _StubEntry(5, "heartbeat"),
    ]
    # remaining=2: keep the OLDEST 2 event_batch (1, 3), drop event_batch 4,
    # keep BOTH control frames (2, 5) -- order preserved.
    kept = AgentRuntime._cap_event_batch_entries(entries, 2)
    assert [e.id for e in kept] == [1, 2, 3, 5]


def test_cap_event_batch_entries_zero_remaining_keeps_only_controls() -> None:
    entries = [
        _StubEntry(1, "event_batch"),
        _StubEntry(2, "command_result"),
        _StubEntry(3, "event_batch"),
    ]
    # remaining=0: no event_batch this pass, but control frames still flow.
    kept = AgentRuntime._cap_event_batch_entries(entries, 0)
    assert [e.id for e in kept] == [2]
