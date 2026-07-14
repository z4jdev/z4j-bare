"""The :class:`AgentRuntime` - the heart of z4j-bare.

The runtime orchestrates every subsystem:

- Reads the resolved :class:`Config` from the framework adapter
- Opens the local SQLite buffer
- Starts a background thread that runs an asyncio event loop
- Inside that loop, runs three cooperating tasks:
    1. Connect/reconnect transport loop (WebSocket primary, long-poll fallback)
    2. Send loop - drains buffer batches to the transport
    3. Heartbeat loop - periodically appends heartbeat frames
    4. Receive loop - handles inbound command frames via the dispatcher
- Exposes sync ``start()`` / ``stop()`` / ``record_event()`` methods so
  it can be driven from any host context (Django, Flask, FastAPI, an
  engine worker process - Celery, RQ, Dramatiq - or a bare Python
  script).

The runtime is deliberately single-instance - one AgentRuntime per
host process. Callers almost never construct it directly; instead,
they call :func:`z4j_bare.install.install_agent`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import sys
import threading
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from z4j_core.errors import (
    AuthenticationError,
    ProtocolError,
    Z4JError,
)
from z4j_core.models import Event
from z4j_core.protocols import FrameworkAdapter, QueueEngineAdapter, SchedulerAdapter
from z4j_core.transport.frames import (
    CommandFrame,
    EventBatchAckFrame,
    EventBatchFrame,
    EventBatchPayload,
    Frame,
    serialize_frame,
)

from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_bare.heartbeat import Heartbeat
from z4j_bare.safety import safe_call
from z4j_bare.transport.longpoll import (
    LongPollTransport,
    PayloadTooLargeError,
    UploadContentRejectedError,
    UploadRetryableError,
)
from z4j_bare.transport.websocket import (
    PartialSendError,
    UndeliverableFrameError,
    WebSocketTransport,
)

if TYPE_CHECKING:
    from z4j_core.models import Config

logger = logging.getLogger("z4j.runtime.supervisor")


def _drain_default_executor(loop: asyncio.AbstractEventLoop, *, deadline_s: float) -> None:
    """Drain the loop's default ThreadPoolExecutor before ``loop.close()``.

    The "Fix B" half of the heartbeat shutdown-race fix. A final
    heartbeat tick may have dispatched a sync provider via
    ``asyncio.to_thread`` -> ``loop.run_in_executor`` -> the default
    executor. Closing the loop with that work in flight races the
    executor teardown and surfaces
    ``RuntimeError('cannot schedule new futures after shutdown')``.
    Draining here closes the window deterministically.

    Version-aware to stay safe on Python 3.11:

    - **3.12+**: ``loop.shutdown_default_executor(timeout=...)`` exists;
      use the native bounded drain.
    - **3.11**: no ``timeout`` parameter. An unbounded
      ``shutdown_default_executor()`` would block teardown forever if
      a provider thread is genuinely wedged (the ``inspector.stats()``
      BRPOP case the heartbeat is built to defend against). Mirror
      CPython's own ``_do_shutdown`` helper-thread pattern: run the
      blocking ``executor.shutdown(wait=True)`` on a side thread and
      bound the wait. If the deadline passes, abandon with
      ``shutdown(wait=False)`` and let the daemon agent thread be the
      ultimate net.

    Best-effort: any error is swallowed by the caller. Never blocks
    longer than ``deadline_s``.
    """
    if loop.is_closed():
        return

    if sys.version_info >= (3, 12):
        import warnings

        try:
            # On a wedged provider thread the timeout fires and the
            # stdlib emits a RuntimeWarning ("executor did not finish
            # joining its threads") - that is the exact case we are
            # deliberately bounding, and the warning is itself
            # teardown noise (it would otherwise reach operator logs).
            # Suppress it here; the daemon agent thread is the net.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                loop.run_until_complete(
                    loop.shutdown_default_executor(timeout=deadline_s),
                )
        except (TimeoutError, RuntimeError):
            # Timeout: a wedged provider thread. RuntimeError: loop
            # state edge during teardown. Either way the daemon thread
            # is the net; don't escalate.
            pass
        return

    # Python 3.11 fallback: own the join with a hard bound.
    executor = getattr(loop, "_default_executor", None)
    if executor is None:
        return
    done = threading.Event()

    def _join() -> None:
        try:
            executor.shutdown(wait=True)
        finally:
            done.set()

    threading.Thread(
        target=_join,
        name="z4j-exec-drain",
        daemon=True,
    ).start()
    if not done.wait(deadline_s):
        # Wedged provider thread; abandon the wait. The pool threads
        # are daemon and will be reaped at interpreter exit.
        with contextlib.suppress(Exception):
            executor.shutdown(wait=False)


def _heartbeat_enabled() -> bool:
    """Whether to run the liveness heartbeat loop. On by default.

    Disable with ``Z4J_HEARTBEAT=0`` (also accepts ``false`` / ``no``
    / ``off``). The heartbeat is a 10s-interval liveness + adapter-
    health loop that a long-lived worker/process benefits from but a
    short-lived one-shot process (a boot-then-exit Django ``manage.py
    <cmd>`` that auto-starts the agent) does not: the command finishes
    its real work in seconds, the heartbeat never ships anything
    actionable, and the final in-flight tick is the thing that races
    the executor teardown. Turning it off for those processes removes
    the race class entirely AND cuts a small amount of startup cost.

    Business events (``record_event`` -> buffer) are unaffected; they
    flow on a separate synchronous path and ship on the next connect
    regardless of whether the heartbeat runs.
    """
    val = os.environ.get("Z4J_HEARTBEAT")
    if val is None:
        return True
    return val.strip().lower() not in ("0", "false", "no", "off", "")


def _peek_frame_meta(payload: bytes) -> tuple[str | None, str | None]:
    """Cheap extraction of the ``id`` and ``type`` fields from a serialized
    frame, without a full Pydantic parse.

    The send loop hands the buffer the pre-serialized frame bytes; to correlate
    the brain's ``event_batch_ack`` to the buffer entry that produced the batch
    we need the frame.id at send time. We ALSO read the wire ``type`` so the
    ack-deferral decision keys off what the frame ACTUALLY is, not the buffer
    entry's ``kind`` metadata -- a mislabelled entry (kind="event_batch" over a
    heartbeat payload, from buffer corruption / a bad migration) would otherwise
    be registered for an ``event_batch_ack`` the brain never emits (round-10
    external LOW). JSON parsing of a few hundred bytes is single-digit
    microseconds and runs only at send time (not for every event).

    Returns ``(None, None)`` if parsing fails. The caller falls back to the
    "confirm immediately" path for that entry, so a pathologically corrupt
    payload doesn't pin the buffer.
    """
    import json as _json

    try:
        decoded = _json.loads(payload)
    except Exception:
        return None, None
    if isinstance(decoded, dict):
        fid = decoded.get("id")
        ftype = decoded.get("type")
        return (
            fid if isinstance(fid, str) else None,
            ftype if isinstance(ftype, str) else None,
        )
    return None, None


def _entry_is_event_batch(entry) -> bool:
    """True if a buffer entry's PARSED WIRE type is ``event_batch`` (falling back
    to the stored ``kind`` metadata only when the payload cannot be peeked).

    The ack-deferral (``_confirm_or_register``) and the backpressure cap
    (``_cap_event_batch_entries`` + the at-cap drain filter) must agree on what
    counts as an event_batch. Keying only on the buffer ``kind`` metadata let a
    mislabelled entry (a real event_batch stored under kind="heartbeat" via
    corruption / a bad migration) slip past the in-flight cap -- registered for
    an ack yet never counted against ``_MAX_IN_FLIGHT_BATCHES`` (round-12
    external LOW). Deciding by the wire type here keeps the two consistent.
    """
    _fid, ftype = _peek_frame_meta(getattr(entry, "payload", None))
    if ftype is not None:
        return ftype == "event_batch"
    return getattr(entry, "kind", None) == "event_batch"


class RuntimeState(StrEnum):
    """Runtime lifecycle states."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"


# Send-loop tuning. These are intentional constants, not config -
# changing them requires thinking about the buffer contract and the
# brain's ingest rate.
_SEND_BATCH_SIZE = 500
#: Floor for the adaptive send-batch size: on a long-poll 413 (body too
#: large) the batch is halved down to this so an oversized frame is
#: isolated to a batch of one, where it can be dropped precisely
#: (R7-MED) rather than pinning valid siblings behind it.
_MIN_SEND_BATCH = 1
_SEND_IDLE_SLEEP = 0.05
_RECONNECT_INITIAL = 1.0
_RECONNECT_MAX = 30.0
_RECONNECT_JITTER = 0.3

#: How long an event_batch frame can sit in ``_pending_acks`` before
#: the watchdog RE-SENDS it (it is never confirmed or dropped there,
#: only re-queued for another send). 90s is generous to allow
#: first-batch drain under heavy fanout (brain ingest is bounded around
#: 80 events/s/connection); going lower re-sends healthy-but-slow
#: batches needlessly (the brain dedups, so it is only wasteful).
_ACK_DEADLINE_SECONDS = 90.0
#: Watchdog poll cadence. Sleeps this long between sweeps. Aim for
#: the fastest cadence that doesn't burn CPU while keeping eviction
#: precise to within a few seconds.
_ACK_WATCHDOG_INTERVAL_SECONDS = 2.0
#: Bounded-retry cap: after this many CONTENT-rejection cycles (the
#: brain is reachable and speaks acks but keeps rejecting this specific
#: batch, or a long-poll upload keeps returning a content-error status)
#: the entry is quarantined (dropped, logged). Only content rejections
#: count -- a flaky connection or a transient 5xx never increments this,
#: so a deliverable batch is never dropped (R6-F4). At the 90s WS ack
#: deadline this is ~15 minutes of active rejection before a genuinely
#: undeliverable event_batch is dropped.
_MAX_SEND_ATTEMPTS = 10
#: Backpressure cap on concurrent un-acked event_batch frames (WS
#: deferred-ack mode). Bounds ``_pending_acks`` and therefore the
#: ``exclude_ids`` set passed to ``buffer.drain`` well under SQLite's
#: 32766-bound-parameter ceiling (R6-F5), and stops the agent piling
#: unbounded un-acked batches on a slow/stalled brain.
_MAX_IN_FLIGHT_BATCHES = 256
#: Long-poll TRANSIENT-retry backoff (seconds): a transient partial
#: store sleeps this long (doubling per consecutive failure, capped)
#: before re-sending the whole batch, so a struggling brain is not
#: hammered (R7-HIGH2). Capped LOW (5s, not 30s): the brain now drops-
#: and-acks every deterministic failure at source, so a partial store is
#: always a genuine transient that self-heals within a few rounds; a 30s
#: cap only slowed recovery and lengthened the window a real outage held
#: the single-threaded send loop (R8).
_SEND_BACKOFF_INITIAL = 0.5
_SEND_BACKOFF_MAX = 5.0
#: Fixed inter-attempt delay (seconds) on the long-poll CONTENT-reject
#: path (413 / 415 / 422; a bare 400 is request-level and retried, R8-M2).
#: Deliberately SMALL and NON-growing so
#: bisection + the bounded per-frame drop clear a poison frame within a
#: couple of seconds instead of the ~150s a growing backoff took, which
#: starved every control frame queued behind the poison (R8: the R7 code
#: shared the growing transient backoff here and blocked the head of the
#: oldest-first queue for minutes).
_CONTENT_REJECT_DELAY = 0.1
#: After this many CONSECUTIVE long-poll partial-store retries that made
#: ZERO confirmed progress, force a reconnect (fresh session_nonce) instead
#: of re-POSTing the identical bytes forever. A normal transient (deadlock /
#: pool blip) self-heals in 1-2 rounds and never approaches this. A
#: PERSISTENT partial-200 that the retry-backoff alone cannot resolve --
#: a send-side session/identity SignatureError (only a reconnect rebuilds
#: the session binding), or a protocol-version skew during a rolling upgrade
#: -- otherwise wedges the send loop forever because only the RECEIVE loop
#: reconnects on its own error. ~20 x 5s cap = ~100s before re-establishing;
#: no frame is dropped (reconnect preserves the buffer), R9.
_MAX_CONSECUTIVE_RETRYABLE = 20

# Auth-error backoff schedule. AuthenticationError indicates the
# brain rejected the agent's bearer token (mismatched HMAC, revoked
# agent, rotated secret, etc.). We retry forever with a 10-minute
# cap so an operator-initiated token rotation eventually succeeds
# without flooding the brain's auth endpoint, and a transiently
# misconfigured agent doesn't permanently park itself offline.
_AUTH_RECONNECT_INITIAL = 10.0
_AUTH_RECONNECT_MAX = 600.0
_AUTH_RECONNECT_JITTER = 0.3

# ProtocolError handling. These come from the wire layer: bad
# handshake, version skew during a brain restart, malformed frames
# from a partial deploy. Almost always transient - same backoff as
# ConnectionError, retried forever.
_PROTOCOL_RECONNECT_INITIAL = 1.0
_PROTOCOL_RECONNECT_MAX = 60.0


def _decode_hmac_secret(value: str) -> bytes:
    """Decode the configured ``hmac_secret`` string into raw bytes.

    Thin wrapper over :func:`z4j_core.transport.hmac.decode_agent_hmac_secret`
    -- the single source of truth for this decode, shared with the
    ``purge_queue`` confirm-token path so frame signing and the purge
    token always key on the identical bytes.
    """
    from z4j_core.transport.hmac import (
        decode_agent_hmac_secret,
    )

    return decode_agent_hmac_secret(value)


class AgentRuntime:
    """The running z4j agent inside the host process.

    Construction does not start anything. Call :meth:`start` to spawn
    the background thread and open the transport. Call :meth:`stop`
    to flush and tear down. The sync methods are safe to call from
    any thread - they drive the inner asyncio loop from the outside.

    Args:
        config: The resolved agent configuration.
        framework: The host framework adapter.
        engines: The queue engine adapters the runtime will manage.
        schedulers: The scheduler adapters (optional).
    """

    def __init__(
        self,
        *,
        config: Config,
        framework: FrameworkAdapter,
        engines: list[QueueEngineAdapter],
        schedulers: list[SchedulerAdapter] | None = None,
    ) -> None:
        self.config = config
        self.framework = framework
        self.engines: dict[str, QueueEngineAdapter] = {e.name: e for e in engines}
        self.schedulers: dict[str, SchedulerAdapter] = {s.name: s for s in (schedulers or [])}

        self._state = RuntimeState.STOPPED
        self._state_lock = threading.Lock()

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_ready = threading.Event()
        self._stop_event: asyncio.Event | None = None

        self._buffer: BufferStore | None = None
        self._transport: WebSocketTransport | None = None
        self._dispatcher: CommandDispatcher | None = None
        self._heartbeat: Heartbeat | None = None

        # Per-class consecutive-failure counters. Reset by
        # ``_connect_and_run`` on successful handshake (which is the
        # only place that observably happens, since the supervisor's
        # task-group exit is always via _StopRequested). Surfaced to
        # the doctor CLI and heartbeat frames so operators can
        # distinguish "agent is in a flap loop" from "agent is fine,
        # just disconnected once."
        self._auth_error_count = 0
        self._protocol_error_count = 0
        self._connection_error_count = 0

        # Per-class connect-retry backoff. Held as instance state
        # rather than stack-locals inside _supervise so that
        # _connect_and_run can reset them on successful handshake.
        # Without this, the delay schedule advanced monotonically
        # for the lifetime of the runtime, leaving a long-stable
        # connection pinned at the cap (30s/60s/600s) after eventual
        # disconnect instead of returning to the floor.
        self._delay_conn = _RECONNECT_INITIAL
        self._delay_proto = _PROTOCOL_RECONNECT_INITIAL
        self._delay_auth = _AUTH_RECONNECT_INITIAL

        # Phase H: timestamp of the most recent successful handshake.
        # None until the first connect, then monotonically updated.
        # Used by the agent_status_provider to compute session age.
        self._last_successful_connect_at: datetime | None = None

        # Pending event_batch ack tracking.
        # Maps ``frame.id -> (buffer_entry_id, sent_at)`` for
        # event_batch frames we shipped but haven't yet seen the
        # brain ack. The send loop adds entries; the receive loop
        # pops on ``EventBatchAckFrame``; the watchdog sweeps
        # entries older than ``_ACK_DEADLINE_SECONDS`` (which on the
        # FIRST stale batch flips the brain to legacy mode for the
        # rest of this session). Bounded indirectly by the buffer
        # size: a stuck-pending entry holds the buffer entry too,
        # so backpressure is via the buffer cap, not unbounded
        # growth here.
        self._pending_acks: dict[str, tuple[int, datetime]] = {}
        # Frame ids the brain acked BEFORE the send loop registered them
        # in ``_pending_acks``. The receive loop can process an
        # ``event_batch_ack`` while the send loop is still awaiting the
        # send that produced it; without this, that ack would pop nothing
        # and the entry would sit un-confirmed until the 90s watchdog
        # (R6-F7). The send loop consults this set at registration time
        # and confirms immediately if the ack already arrived. Bounded;
        # cleared on reconnect.
        self._acks_seen_early: set[str] = set()
        # Current long-poll retry backoff (seconds), grown on consecutive
        # retryable outcomes (transient partial store, content reject,
        # 413) and reset on a successful send. Gives a genuinely-poison
        # frame a real backoff instead of a 50ms hot-loop (R7-HIGH2).
        self._send_backoff: float = _SEND_BACKOFF_INITIAL
        # Consecutive long-poll partial-store retries with zero confirmed
        # progress. Reset on any successful send and on reconnect; when it
        # crosses ``_MAX_CONSECUTIVE_RETRYABLE`` the send loop forces a
        # reconnect so a persistent session/version skew is not re-POSTed
        # forever (R9).
        self._consecutive_retryable: int = 0
        # Current per-send batch size. Starts at ``_SEND_BATCH_SIZE`` and
        # is halved on an HTTP 413 (long-poll body too large) down to a
        # floor of 1, so an agent behind a small server body cap adapts
        # instead of looping on an oversized POST (R6-F6).
        self._send_batch_size: int = _SEND_BATCH_SIZE

        # Reconnect-now event. set() by the SIGHUP handler from
        # z4j_bare.control.install_sighup_handler. The supervisor
        # checks this between cycles and skips its backoff timer
        # when set, going straight to the next connect attempt.
        # Shared across all framework adapters because they all
        # use this same runtime.
        self._reconnect_now: asyncio.Event | None = None

        # 1.3.3: how often the periodic schedule resync timer fires.
        # 0 disables the timer (boot snapshot still fires; on-demand
        # ``schedule.resync`` command still works). Sourced from
        # ``Config.schedule_resync_interval_seconds`` if present,
        # else 900 (15 minutes) as the documented default.
        self._schedule_resync_interval: float = float(
            getattr(config, "schedule_resync_interval_seconds", 900),
        )
        # Live reference to the currently connected scheduler adapter
        # list. Populated on every successful connect, cleared on
        # disconnect. Read by the dispatcher's ``schedule.resync``
        # command handler so it can drive a snapshot on demand.
        self._connected_schedulers_ref: list[SchedulerAdapter] = []

    # ------------------------------------------------------------------
    # Sync API (safe from any thread/context)
    # ------------------------------------------------------------------

    def request_reconnect(self) -> None:
        """Request the supervisor skip its current backoff and reconnect now.

        Thread-safe. Called by the SIGHUP signal handler installed
        by ``z4j_bare.control.install_sighup_handler``. The
        supervisor's ``asyncio.wait_for`` on the stop_event will
        early-return when this event fires; the next iteration
        attempts a fresh connection without waiting for the rest of
        the backoff timer.

        Returns silently if the runtime hasn't started yet.
        """
        if self._loop is not None and self._reconnect_now is not None:
            self._loop.call_soon_threadsafe(self._reconnect_now.set)

    def supervisor_state(self) -> dict[str, object]:
        """Return current supervisor health for the doctor CLI.

        Snapshot includes the runtime state, per-error-class
        consecutive-failure counters, and (if connected) the current
        session id. Doctor formats this for human consumption; status
        prints a one-line summary.
        """
        return {
            "state": self.state.value,
            "auth_error_count": self._auth_error_count,
            "protocol_error_count": self._protocol_error_count,
            "connection_error_count": self._connection_error_count,
            "session_id": (
                getattr(self._transport, "session_id", None)
                if self._transport is not None
                else None
            ),
        }

    @property
    def state(self) -> RuntimeState:
        with self._state_lock:
            return self._state

    def start(self) -> None:
        """Start the runtime. Non-blocking after background thread is live.

        Idempotent: calling ``start`` on an already-running runtime
        is a no-op. Safe to call from Django's ``AppConfig.ready()``.

        Refuses to start without a configured ``hmac_secret`` - the
        protocol v2 wire requires every frame to carry an envelope
        HMAC, so there is no longer a meaningful "unsigned" mode.
        ``dev_mode`` only relaxes the plain-``ws://`` guard; it does
        not let you skip signing.
        """
        if self.config.hmac_secret is None:
            raise RuntimeError(
                "z4j agent refusing to start: hmac_secret is required. "
                "Set Z4J_HMAC_SECRET (or settings.Z4J['hmac_secret']) "
                "to the per-project secret printed by the brain when "
                "the project was created.",
            )

        with self._state_lock:
            if self._state != RuntimeState.STOPPED:
                return
            self._state = RuntimeState.STARTING

        # Open the buffer on the caller's thread (fast, synchronous).
        self._buffer = BufferStore(
            path=self.config.buffer_path,
            max_entries=self.config.buffer_max_events,
            max_bytes=self.config.buffer_max_bytes,
        )

        # Spawn the background thread that owns the event loop.
        # ``_loop_ready`` MUST be cleared before the thread starts so
        # this caller's wait() cannot return spuriously on a leftover
        # signal from a previous start/stop cycle.
        self._loop_ready.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="z4j-agent",
            daemon=True,
        )
        self._thread.start()

        if not self._loop_ready.wait(timeout=5.0):
            # The background thread never signalled ready - either it
            # failed to construct the loop or the JIT thread start
            # was delayed past the timeout. Tear down so the runtime
            # is left in a clean STOPPED state.
            logger.error(
                "z4j agent runtime: background loop did not become ready within 5s; tearing down",
            )
            self._abort_start()
            raise RuntimeError(
                "z4j agent runtime failed to start: background loop did not become ready",
            )

        with self._state_lock:
            self._state = RuntimeState.RUNNING

        # Pidfile + SIGHUP wiring (1.1.2+). Best-effort: failure to
        # write the pidfile or install the handler is logged but
        # does NOT prevent the runtime from running. The agent stays
        # functional; the only feature operators lose is the
        # ``z4j-<adapter> restart`` shortcut (they can still
        # restart their host process via their supervisor).
        try:
            from z4j_bare.control import (
                install_sighup_handler,
                write_pidfile,
            )

            adapter_id = self.framework.name if self.framework else "bare"
            try:
                pf = write_pidfile(adapter_id)
                logger.debug("z4j agent: pidfile written at %s", pf)
            except OSError as exc:
                logger.warning(
                    "z4j agent: pidfile write failed (%s); "
                    "`z4j-%s restart` will not work until the "
                    "next clean restart",
                    exc,
                    adapter_id,
                )
            install_sighup_handler(self)
        except Exception:
            logger.exception(
                "z4j agent: failed to set up control surface "
                "(pidfile + SIGHUP). Runtime is unaffected.",
            )

        logger.info(
            "z4j agent runtime started (project=%s, engines=%s)",
            self.config.project_id,
            list(self.engines),
        )

    def _abort_start(self) -> None:
        """Best-effort cleanup when start() fails to come up cleanly."""
        if self._buffer is not None:
            try:
                self._buffer.close()
            except Exception:
                logger.exception("error closing buffer during start abort")
            self._buffer = None
        # Wait briefly for the thread to exit on its own; daemon=True
        # means it will not block process shutdown if it never does.
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        self._thread = None
        with self._state_lock:
            self._state = RuntimeState.STOPPED

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the runtime and flush the buffer.

        Blocks up to ``timeout`` seconds while the background loop
        shuts down cleanly. Safe to call from any thread. Idempotent.
        """
        with self._state_lock:
            if self._state in (RuntimeState.STOPPED, RuntimeState.STOPPING):
                return
            self._state = RuntimeState.STOPPING

        if self._loop is not None and self._stop_event is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)

        if self._thread is not None:
            self._thread.join(timeout=timeout)

        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None

        # Best-effort pidfile cleanup so a stale entry doesn't
        # confuse the next ``z4j-<adapter> restart``.
        try:
            from z4j_bare.control import remove_pidfile

            remove_pidfile(self.framework.name if self.framework else "bare")
        except Exception:  # noqa: S110  best-effort pidfile cleanup on stop
            pass

        with self._state_lock:
            self._state = RuntimeState.STOPPED

        logger.info("z4j agent runtime stopped")

    def record_event(self, event: Event) -> None:
        """Append an event to the outbound buffer.

        Intended to be called from an engine's hot-path callback
        (Celery signal handler, RQ Job callback, Dramatiq middleware
        method) or any other host-app hot path. **Non-blocking**.
        Wraps the buffer write in :func:`safe_call` so a buffer error
        never propagates into the host code.

        Thread-safety: ``_buffer`` is read into a local variable
        ONCE, then used. Even if ``stop()`` races to set
        ``self._buffer = None`` on another thread, the local
        reference keeps the BufferStore alive for the duration of
        this call. The BufferStore's own internal lock serializes
        concurrent appends.
        """
        buffer = self._buffer
        if buffer is None or buffer.closed:
            return
        safe_call(self._enqueue_single_event, buffer, event)

    @staticmethod
    def _enqueue_single_event(buffer: BufferStore, event: Event) -> None:
        """Internal helper serializing one event as an event_batch frame.

        v1 sends one event per frame - batching happens naturally in
        the send loop because the transport drains up to
        ``_SEND_BATCH_SIZE`` buffer entries at a time. A future
        optimization is to batch multiple events into a single
        frame, but the current shape keeps each buffer entry
        self-contained and easy to confirm individually.
        """
        import secrets as _secrets

        frame = EventBatchFrame(
            # 128-bit id: event_batch ids key _pending_acks, so a 48-bit
            # collision could let a real ack for one entry delete a different
            # unstored entry (R8-M5). 35 chars, within the 64-char id cap.
            id=f"ev_{_secrets.token_hex(16)}",
            ts=datetime.now(UTC),
            payload=EventBatchPayload(
                events=[
                    {
                        # Stable per-event id minted on the agent at
                        # capture time. Survives buffer round-trips
                        # so the brain can dedupe replays from a
                        # re-connecting agent - without this the
                        # brain mints its own UUID per ingest and
                        # the (occurred_at, id) conflict key never
                        # fires.
                        "id": str(event.id),
                        "kind": event.kind.value,
                        "engine": event.engine,
                        "task_id": event.task_id,
                        "occurred_at": event.occurred_at.isoformat(),
                        "data": event.data,
                    }
                ],
            ),
        )
        buffer.append("event_batch", serialize_frame(frame))

    def buffer_size(self) -> int:
        """Number of entries currently in the outbound buffer.

        Reflected in the next heartbeat. Useful for health checks
        and metrics.
        """
        return self._buffer.size() if self._buffer is not None else 0

    # ------------------------------------------------------------------
    # Background thread + asyncio loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Entry point for the background thread.

        Order matters: we MUST construct the event loop and the
        cooperative ``_stop_event`` BEFORE signalling ``_loop_ready``,
        otherwise ``start()`` could return and ``stop()`` could try
        to schedule on a half-built loop.
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._stop_event = asyncio.Event()
            self._reconnect_now = asyncio.Event()
        except Exception:
            logger.exception("z4j agent failed to create asyncio loop")
            self._loop_ready.set()  # unblock the caller's wait()
            return

        # Loop and stop_event are both live now; safe to publish.
        self._loop_ready.set()

        try:
            loop.run_until_complete(self._main())
        except Exception:
            logger.exception("z4j agent runtime loop crashed")
        finally:
            # Deterministic executor drain BEFORE close (the "Fix B"
            # half of the heartbeat shutdown-race fix). Any final
            # heartbeat tick dispatched a sync provider through the
            # loop's default ThreadPoolExecutor via ``to_thread``; if
            # we close the loop with a tick still in flight, the
            # executor teardown races the submit. Draining the
            # executor here (while the loop is still usable) closes
            # that window. Bounded so a genuinely wedged provider
            # thread (the ``inspector.stats()`` BRPOP case the
            # heartbeat defends against) can never hang teardown -
            # the daemon thread is the ultimate net.
            # Drain is best-effort hardening; never let it block
            # or crash the close path below.
            with contextlib.suppress(Exception):
                _drain_default_executor(loop, deadline_s=2.0)
            with contextlib.suppress(Exception):
                loop.close()
            self._loop = None

    async def _main(self) -> None:  # noqa: PLR0912, PLR0915  asyncio main orchestration
        """The actual asyncio main - runs transport, send loop, heartbeat."""
        assert self._buffer is not None
        assert self._stop_event is not None

        # Protocol v2 moves envelope-HMAC verification into the
        # transport layer (:class:`FrameVerifier`), so the command
        # dispatcher no longer owns a verifier of its own. Pre-flight
        # the secret length here so the whole runtime refuses to
        # come up with a malformed secret rather than discovering it
        # only on first connect.
        assert self.config.hmac_secret is not None, (
            "start() should have rejected a missing hmac_secret"
        )
        secret_bytes = _decode_hmac_secret(
            self.config.hmac_secret.get_secret_value(),
        )
        if len(secret_bytes) < 32:
            logger.error(
                "z4j agent: configured hmac_secret is too short "
                "(must decode to at least 32 bytes); refusing to start",
            )
            return

        self._transport = self._build_transport(secret_bytes)
        self._dispatcher = CommandDispatcher(
            engines=self.engines,
            schedulers=self.schedulers,
            buffer=self._buffer,
            resync_schedules=self.resync_schedules_now,
        )

        def _collect_engine_health() -> dict[str, str]:
            """Aggregate health from all engine adapters for the heartbeat."""
            import json as _json

            result: dict[str, str] = {}
            for name, engine in self.engines.items():
                get_health = getattr(engine, "get_health", None)
                if callable(get_health):
                    try:
                        health = get_health()
                        for k, v in health.items():
                            # Serialize complex values as JSON so the
                            # brain can parse them back. Simple scalars
                            # stay as strings.
                            if isinstance(v, (dict, list)):
                                result[f"{name}.{k}"] = _json.dumps(
                                    v,
                                    default=str,
                                )
                            else:
                                result[f"{name}.{k}"] = str(v)
                    except Exception:
                        result[f"{name}.error"] = "health check failed"
            return result

        # Phase H: status_provider drains supervisor counters + buffer
        # state into the agent_status frame the heartbeat loop emits
        # alongside the heartbeat. Closure over self so the live
        # values are read at emission time, not snapshot at boot.
        def _collect_agent_status() -> dict[str, Any]:
            from z4j_core.transport.versioning import (
                CURRENT_PROTOCOL as _CUR_PROTO,
            )
            from z4j_core.version import __version__ as _agent_ver

            buffer_depth = self._buffer.size() if self._buffer is not None else 0
            session_age: float | None = None
            last_connect: datetime | None = self._last_successful_connect_at
            if last_connect is not None:
                session_age = max(
                    (datetime.now(UTC) - last_connect).total_seconds(),
                    0.0,
                )
            return {
                "auth_failure_streak": self._auth_error_count,
                "protocol_failure_streak": self._protocol_error_count,
                "connection_failure_streak": self._connection_error_count,
                "last_successful_connect_at": last_connect,
                "current_session_age_seconds": session_age,
                "buffer_depth": buffer_depth,
                "agent_version": _agent_ver,
                "protocol_version": str(_CUR_PROTO),
                "engines": list(self.engines),
                "schedulers": list(self.schedulers),
            }

        if _heartbeat_enabled():
            self._heartbeat = Heartbeat(
                buffer=self._buffer,
                stop_event=self._stop_event,
                interval=10.0,
                health_provider=_collect_engine_health,
                status_provider=_collect_agent_status,
            )
        else:
            # One-shot / short-lived mode: no liveness heartbeat. The
            # send/receive/ack loops still run so business events flush;
            # we just don't spin the 10s health tick that would race
            # the executor teardown on a boot-then-exit process.
            self._heartbeat = None
            logger.debug(
                "z4j agent heartbeat disabled (Z4J_HEARTBEAT=0); running heartbeat-less",
            )

        # CRIT #1: wire engine + scheduler signal handlers. Without this
        # the agent connects but never observes anything in the host
        # process. The runtime owns lifetime - we always disconnect
        # in the finally block, even if connect_signals on a later
        # adapter raised half-way through.
        loop = asyncio.get_running_loop()
        connected_engines: list[QueueEngineAdapter] = []
        connected_schedulers: list[SchedulerAdapter] = []
        engine_consumer_tasks: list[asyncio.Task[None]] = []
        try:
            for engine in self.engines.values():
                connect = getattr(engine, "connect_signals", None)
                if callable(connect):
                    connect(loop=loop)
                connected_engines.append(engine)

            for scheduler in self.schedulers.values():
                connect = getattr(scheduler, "connect_signals", None)
                if callable(connect):
                    # Bind the scheduler's own ``name`` into the sink
                    # closure so Event.engine carries the correct
                    # adapter identifier (e.g. "celery-beat",
                    # "apscheduler", "rq-scheduler"). The sink itself
                    # is engine-agnostic - the per-scheduler closure
                    # is what fixes the previous "everything reports
                    # as celery-beat" bug.
                    scheduler_name = scheduler.name

                    def _sink(action: str, schedule: object, _name: str = scheduler_name) -> None:
                        self._scheduler_sink(_name, action, schedule)

                    connect(sink=_sink)
                connected_schedulers.append(scheduler)

            # CRIT #2: drain engine event queues → outbound buffer.
            # Each engine's ``connect_signals`` makes the engine's
            # native hooks (Celery signals, RQ Job hooks, Dramatiq
            # middleware) enqueue Events into the engine's internal
            # asyncio.Queue. ``subscribe_events()`` yields from that
            # queue. We consume each engine in its own task so
            # multiple engines don't block each other.
            for engine in connected_engines:
                subscribe = getattr(engine, "subscribe_events", None)
                if callable(subscribe):
                    task = asyncio.create_task(
                        self._consume_engine_events(engine),
                        name=f"z4j-engine-events-{getattr(engine, 'name', 'unknown')}",
                    )
                    engine_consumer_tasks.append(task)

            # 1.3.3 - Phase A: initial schedule inventory at boot.
            # Every scheduler adapter (z4j-celerybeat, z4j-apscheduler,
            # z4j-rqscheduler, z4j-arqcron, z4j-hueyperiodic,
            # z4j-taskiqscheduler) implements
            # ``async list_schedules() -> list[Schedule]``. Pre-1.3.3
            # the runtime only listened to ``connect_signals`` reactive
            # hooks (Django post_save etc.), which meant schedules that
            # existed BEFORE the agent was installed were invisible to
            # the dashboard until each was edited+saved. We now drain
            # ``list_schedules()`` once on boot and emit a single
            # ``schedule.snapshot`` event per scheduler so the brain
            # can reconcile the full inventory in one transaction. Plus
            # the periodic timer (Phase B) catches drift over time, and
            # the brain's ``schedule.resync`` command (Phase C) lets
            # the dashboard "Sync now" button force one on demand.
            # Fired as a fire-and-forget task per scheduler so a slow
            # adapter (e.g. APScheduler with a remote SQL jobstore)
            # doesn't block the rest of the agent's startup.
            schedule_inventory_tasks: list[asyncio.Task[None]] = []
            for scheduler in connected_schedulers:
                schedule_inventory_tasks.append(
                    asyncio.create_task(
                        self._emit_schedule_snapshot(scheduler, reason="boot"),
                        name=f"z4j-schedule-inventory-{getattr(scheduler, 'name', 'unknown')}",
                    )
                )

            # 1.3.3 - Phase B: periodic schedule resync.
            # A long-lived task that drains ``list_schedules()`` on
            # every registered scheduler every
            # ``schedule_resync_interval_seconds`` (default 900s = 15
            # min). Catches drift between the brain and the agent's
            # source (e.g. someone added a PeriodicTask via SQL
            # while the agent was offline, or an agent reconnected
            # after missing a ``schedule.deleted`` event). Cancelled
            # in the same finally block as the engine consumers.
            periodic_task: asyncio.Task[None] | None = None
            if connected_schedulers and self._schedule_resync_interval > 0:
                periodic_task = asyncio.create_task(
                    self._periodic_schedule_resync(connected_schedulers),
                    name="z4j-schedule-resync-timer",
                )
                engine_consumer_tasks.append(periodic_task)

            # Stash schedulers + buffer reference so the dispatcher's
            # ``schedule.resync`` command handler can drive a snapshot
            # on demand (Phase C). The dispatcher gets these via the
            # runtime accessor ``_schedule_snapshot_handler``.
            self._connected_schedulers_ref = connected_schedulers

            await self._supervise()
        finally:
            # Cancel engine event consumers first - they reference
            # adapters that are about to have their signals torn down.
            for task in engine_consumer_tasks:
                task.cancel()
            for task in engine_consumer_tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

            # Disconnect in reverse order, swallowing per-adapter errors
            # so one failing adapter cannot strand another's signals.
            for scheduler in reversed(connected_schedulers):
                disconnect = getattr(scheduler, "disconnect_signals", None)
                if callable(disconnect):
                    try:
                        disconnect()
                    except Exception:
                        logger.exception(
                            "z4j agent: scheduler %s disconnect_signals raised",
                            getattr(scheduler, "name", scheduler),
                        )

            for engine in reversed(connected_engines):
                disconnect = getattr(engine, "disconnect_signals", None)
                if callable(disconnect):
                    try:
                        disconnect()
                    except Exception:
                        logger.exception(
                            "z4j agent: engine %s disconnect_signals raised",
                            getattr(engine, "name", engine),
                        )

            if self._transport is not None:
                await safe_close(self._transport)

    async def _consume_engine_events(self, engine: QueueEngineAdapter) -> None:
        """Drain an engine's ``subscribe_events()`` into ``record_event``.

        Runs as a long-lived task started in ``_main``. When the
        engine yields an event, we immediately push it into the
        outbound buffer via :meth:`record_event`. The task is
        cancelled during shutdown.
        """
        subscribe = getattr(engine, "subscribe_events", None)
        if not callable(subscribe):
            return
        try:
            async for event in subscribe():
                self.record_event(event)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception(
                "z4j agent: engine %s event consumer crashed",
                getattr(engine, "name", engine),
            )

    async def _emit_schedule_snapshot(
        self,
        scheduler: SchedulerAdapter,
        *,
        reason: str,
    ) -> None:
        """Drain a scheduler adapter's ``list_schedules`` and emit ONE
        ``schedule.snapshot`` event carrying the full inventory.

        The brain's event ingestor 3-way diffs against the DB scoped
        to ``(project, scheduler)``, inserts new rows, updates
        existing rows, deletes rows missing from the snapshot. The
        whole thing is one transaction on the brain side.

        Called from three places:

        1. Boot (Phase A) once per scheduler, ``reason="boot"``.
        2. Periodic timer (Phase B), ``reason="periodic"``.
        3. ``schedule.resync`` command receiver (Phase C),
           ``reason="command"``.

        Wrapped in defensive try/except: a misbehaving adapter that
        raises during ``list_schedules`` MUST NOT take down the
        whole runtime, especially on the periodic path where it
        would loop on the next tick anyway.
        """
        from uuid import uuid4

        from z4j_core.models import Event, EventKind  # local import to avoid cycles

        scheduler_name = getattr(scheduler, "name", "unknown")
        try:
            schedules = await scheduler.list_schedules()
        except Exception:
            logger.exception(
                "z4j agent: scheduler %s list_schedules failed (reason=%s)",
                scheduler_name,
                reason,
            )
            return

        # Serialize each schedule. ``model_dump(mode="json")`` produces
        # JSON-safe primitives so the brain's frame validator does not
        # reject e.g. datetime / UUID objects.
        schedules_payload: list[dict[str, object]] = []
        for schedule in schedules:
            dump = getattr(schedule, "model_dump", None)
            if not callable(dump):
                continue
            try:
                schedules_payload.append(dump(mode="json"))
            except Exception:
                logger.exception(
                    "z4j agent: scheduler %s yielded a Schedule that failed model_dump",
                    scheduler_name,
                )
                continue

        placeholder = uuid4()
        event = Event(
            id=uuid4(),
            project_id=placeholder,
            agent_id=placeholder,
            engine=scheduler_name,
            task_id="",
            kind=EventKind.SCHEDULE_SNAPSHOT,
            occurred_at=datetime.now(UTC),
            data={
                "scheduler": scheduler_name,
                "schedules": schedules_payload,
                "reason": reason,
            },
        )
        self.record_event(event)
        logger.info(
            "z4j agent: scheduler %s snapshot emitted (count=%d, reason=%s)",
            scheduler_name,
            len(schedules_payload),
            reason,
        )

    async def _periodic_schedule_resync(
        self,
        schedulers: list[SchedulerAdapter],
    ) -> None:
        """Long-lived task: re-emit a snapshot for every scheduler on
        a fixed cadence so brain ↔ agent state can never drift
        further than ``schedule_resync_interval_seconds`` (default
        15 min).

        Sleeps via ``asyncio.wait_for(stop_event.wait(), timeout=...)``
        so a graceful shutdown wakes the task immediately rather than
        waiting up to a full interval.
        """
        interval = self._schedule_resync_interval
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval,
                )
                # Stop signalled - exit cleanly.
                return
            except TimeoutError:
                # Normal tick - drain every scheduler.
                pass
            for scheduler in schedulers:
                if self._stop_event.is_set():
                    return
                await self._emit_schedule_snapshot(
                    scheduler,
                    reason="periodic",
                )

    async def resync_schedules_now(self, reason: str = "command") -> int:
        """Drain every connected scheduler adapter and emit one
        ``schedule.snapshot`` per adapter.

        Intended caller: the dispatcher's ``schedule.resync`` command
        handler (Phase C). Returns the number of schedulers drained.
        Safe to call any time post-connect; before connect_signals
        has run the connected list is empty and the call is a no-op.
        """
        schedulers = list(getattr(self, "_connected_schedulers_ref", ()) or ())
        for scheduler in schedulers:
            await self._emit_schedule_snapshot(scheduler, reason=reason)
        return len(schedulers)

    def _scheduler_sink(
        self,
        scheduler_name: str,
        action: str,
        schedule: object,
    ) -> None:
        """Sink passed to scheduler adapters' ``connect_signals``.

        Schedulers call this (via a per-scheduler closure that binds
        ``scheduler_name``) from inside their native lifecycle hooks
        - Django signals for celery-beat, APScheduler listeners for
        apscheduler, etc. We translate the ``(action, schedule)`` pair
        into a generic ``Event`` shape stamped with the *actual*
        scheduler's name and put it on the outbound buffer via
        :meth:`record_event`. Wrapped in :func:`safe_call` so a
        malformed schedule cannot crash the host process's signal
        handler.

        ``scheduler_name`` is required (no default): the previous
        Phase-1 implementation hardcoded ``"celery-beat"`` here, which
        would have mislabelled every APScheduler/rq-scheduler event
        once those adapters land. See docs/BARE_AUDIT_2026Q2.md F2.
        """
        from uuid import uuid4

        from z4j_core.models import Event, EventKind  # local import to avoid cycles

        def _build_and_record() -> None:
            kind_map = {
                "created": EventKind.SCHEDULE_CREATED,
                "updated": EventKind.SCHEDULE_UPDATED,
                "deleted": EventKind.SCHEDULE_DELETED,
            }
            kind = kind_map.get(action)
            if kind is None:
                return
            schedule_dict: dict[str, object] = {}
            dump = getattr(schedule, "model_dump", None)
            if callable(dump):
                try:
                    schedule_dict = dump(mode="json")
                except Exception:
                    schedule_dict = {}
            placeholder = uuid4()
            event = Event(
                id=uuid4(),
                project_id=placeholder,
                agent_id=placeholder,
                engine=scheduler_name,
                task_id="",
                kind=kind,
                occurred_at=datetime.now(UTC),
                data={"schedule": schedule_dict},
            )
            self.record_event(event)

        safe_call(_build_and_record)

    async def _supervise(self) -> None:  # noqa: PLR0912, PLR0915  supervisor reconnect loop
        """Supervisor: connect → run tasks → on disconnect, reconnect.

        Forever-retry contract (1.1.2+): every error class except
        ``_StopRequested`` schedules a reconnect. The reconnect loop
        is structurally infinite; only an explicit operator stop
        terminates it. This is the difference between an agent that
        self-heals and an agent that goes offline forever after one
        bad handshake.

        Per-error-class backoff schedules:

        - ``ConnectionError`` (network, TCP, TLS, WS-level): 1s -> 30s
        - ``ProtocolError`` (handshake, version skew, malformed frame): 1s -> 60s
        - ``AuthenticationError`` (rejected bearer, HMAC mismatch,
          revoked agent): 10s -> 600s. Longer cap because if it's
          a real config issue, hammering the brain at 30s intervals
          is rude; if it's a transient secret-rotation window, 10
          minutes is short enough that operators don't notice.
        - ``Exception`` (anything else, including bugs in our own
          code): treated as ConnectionError-equivalent. Logged with
          full traceback at every cycle.

        Logging policy (matches Sentry / OpenTelemetry / New Relic
        SDK convention so the agent does not spam dev consoles when
        the brain is unreachable for an extended period):

        - First disconnect of a streak: WARNING with full context
          and a hint to raise the ``z4j.agent`` logger to DEBUG for
          per-attempt detail.
        - Subsequent disconnects of the same class while the streak
          continues: DEBUG.
        - Every 10th attempt in a long streak: INFO summary so an
          operator tailing the log sees a heartbeat-rate signal that
          the agent is still alive and trying.
        - Successful reconnect after a streak: INFO log emitted by
          ``_connect_and_run`` with the failure count.

        ``_connect_and_run`` runs an ``asyncio.TaskGroup`` which raises
        ``ExceptionGroup`` (PEP 654) when any child task fails. We use
        ``except*`` to peel out the failure classes so the longer
        AuthenticationError schedule is applied independently of the
        others.
        """
        assert self._stop_event is not None
        # Counters and per-class delays are instance state initialised
        # in __init__ and reset on successful handshake by
        # _connect_and_run. We re-initialise them here as well in case
        # _supervise is invoked more than once over the runtime's life
        # (it currently is not, but the contract should not depend on
        # call-count).
        self._auth_error_count = 0
        self._protocol_error_count = 0
        self._connection_error_count = 0
        self._delay_conn = _RECONNECT_INITIAL
        self._delay_proto = _PROTOCOL_RECONNECT_INITIAL
        self._delay_auth = _AUTH_RECONNECT_INITIAL
        stop_loop = False
        while not stop_loop and not self._stop_event.is_set():
            error_class: str | None = None
            err: BaseException | None = None
            try:
                await self._connect_and_run()
            except* _StopRequested:
                # Watchdog cancelled the group on stop_event - clean exit.
                # PEP 654 forbids ``return`` inside an ``except*`` block,
                # so we set a flag and break out at the next loop guard.
                stop_loop = True
            except* AuthenticationError as eg:
                error_class = "auth"
                err = _first(eg)
                self._auth_error_count += 1
            except* ProtocolError as eg:
                error_class = "protocol"
                err = _first(eg)
                self._protocol_error_count += 1
            except* ConnectionError as eg:
                error_class = "connection"
                err = _first(eg)
                self._connection_error_count += 1
            except* Exception as eg:
                # Unknown failure class. Treat as connection-class
                # for backoff purposes; log full traceback so future
                # categorisation is possible. Never fatal.
                logger.exception(
                    "z4j agent unexpected supervisor error",
                    exc_info=eg,
                )
                error_class = "connection"
                err = _first(eg)
                self._connection_error_count += 1

            if stop_loop:
                break

            if err is not None:
                count = (
                    self._auth_error_count
                    if error_class == "auth"
                    else self._protocol_error_count
                    if error_class == "protocol"
                    else self._connection_error_count
                )
                _log_disconnect(error_class, err, count)

            if self._stop_event.is_set():
                return

            # Pick the schedule for the most recent error class.
            if error_class == "auth":
                base = self._delay_auth
                jitter = _AUTH_RECONNECT_JITTER
                cap = _AUTH_RECONNECT_MAX
            elif error_class == "protocol":
                base = self._delay_proto
                jitter = _RECONNECT_JITTER
                cap = _PROTOCOL_RECONNECT_MAX
            else:
                base = self._delay_conn
                jitter = _RECONNECT_JITTER
                cap = _RECONNECT_MAX

            sleep_for = base + random.uniform(0, base * jitter)  # noqa: S311  non-security reconnect jitter
            # Wake either on stop (clean exit) or reconnect_now
            # (SIGHUP from z4j-<adapter> restart). On reconnect_now
            # we clear the event and skip straight to the next
            # connect attempt; the per-class backoff timer is NOT
            # reset (so an operator can't accidentally hammer the
            # brain by spamming SIGHUP).
            assert self._reconnect_now is not None
            stop_task = asyncio.create_task(self._stop_event.wait())
            reconnect_task = asyncio.create_task(self._reconnect_now.wait())
            try:
                done, pending = await asyncio.wait(
                    {stop_task, reconnect_task},
                    timeout=sleep_for,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                if stop_task in done:
                    return  # stop requested
                if reconnect_task in done:
                    self._reconnect_now.clear()
                    logger.info(
                        "z4j agent reconnect requested via SIGHUP; skipping remaining backoff",
                    )
            finally:
                # Defensively cancel any task that survived the
                # done/pending split (shouldn't happen, but
                # cancellation is cheap).
                for t in (stop_task, reconnect_task):
                    if not t.done():
                        t.cancel()

            # Advance only the schedule that fired. Other classes
            # keep their state so a flap pattern in one class doesn't
            # zero out an unrelated class's progress.
            if error_class == "auth":
                self._delay_auth = min(self._delay_auth * 2.0, cap)
            elif error_class == "protocol":
                self._delay_proto = min(self._delay_proto * 2.0, cap)
            else:
                self._delay_conn = min(self._delay_conn * 2.0, cap)

    async def _connect_and_run(self) -> None:
        """One supervisor cycle: connect, run tasks, until disconnect."""
        assert self._transport is not None
        assert self._stop_event is not None
        assert self._dispatcher is not None
        # NB: self._heartbeat may be None in heartbeat-less mode
        # (Z4J_HEARTBEAT=0); guarded at each use below.

        await self._transport.connect()

        # Connection established. Reset failure counters AND the
        # per-class backoff delays so the next disconnect starts at
        # the floor again. This is the only reachable reset point in
        # the supervisor lifecycle: the supervise() task-group always
        # exits via _StopRequested, never by clean return, so a reset
        # placed there would never fire. Without this reset a runtime
        # that flapped once on startup and then stabilised for hours
        # would still be pinned at the 30s/60s/600s cap on its next
        # disconnect, which is worse than starting at 1s/1s/10s.
        prior_failures = (
            self._connection_error_count + self._protocol_error_count + self._auth_error_count
        )
        if prior_failures > 0:
            logger.info(
                "z4j agent recovered after %d failed connect attempt(s)",
                prior_failures,
            )
        self._connection_error_count = 0
        self._protocol_error_count = 0
        self._auth_error_count = 0
        self._delay_conn = _RECONNECT_INITIAL
        self._delay_proto = _PROTOCOL_RECONNECT_INITIAL
        self._delay_auth = _AUTH_RECONNECT_INITIAL
        # Phase H: timestamp the successful connect so the agent_status
        # frame can report session age. Updated on every connect, not
        # just the first.
        self._last_successful_connect_at = datetime.now(UTC)

        if self._heartbeat is not None:
            self._heartbeat.set_interval(
                float(self._transport.heartbeat_interval),
            )

        # Reset per-connection ack tracking before the send loop starts
        # on this connection. Un-acked entries from the previous
        # connection re-drain and re-send (the brain dedups any that
        # landed); nothing is confirmed or dropped on reconnect.
        self._pending_acks.clear()
        self._acks_seen_early.clear()
        # Restore the full send batch size on a fresh connection: a 413
        # that shrank it may have been a per-connection proxy limit that
        # no longer applies (R6-F6). If the new brain still 413s, it
        # shrinks again.
        self._send_batch_size = _SEND_BATCH_SIZE
        # Reset the retryable-outcome backoff too, so a backoff grown on a
        # dying session does not throttle the first sends of a fresh one.
        self._send_backoff = _SEND_BACKOFF_INITIAL
        # Fresh session -> the persistent-retryable reconnect counter starts
        # over (R9).
        self._consecutive_retryable = 0

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._run_send_loop(), name="z4j-send")
            if self._heartbeat is not None:
                tg.create_task(self._heartbeat.run(), name="z4j-heartbeat")
            tg.create_task(self._run_receive_loop(), name="z4j-receive")
            tg.create_task(self._ack_watchdog_loop(), name="z4j-ack-watchdog")
            # One "watchdog" task exits when stop_event fires, cancelling the group.
            tg.create_task(self._wait_for_stop(), name="z4j-watchdog")

    async def _wait_for_stop(self) -> None:
        assert self._stop_event is not None
        await self._stop_event.wait()
        raise _StopRequested()

    @staticmethod
    def _cap_event_batch_entries(entries: list, max_event_batch: int) -> list:
        """Keep every non-event_batch entry plus the OLDEST ``max_event_batch``
        event_batch entries (order preserved).

        The excess event_batch entries stay in the buffer (drain is a
        non-destructive SELECT), so they re-drain once acks free in-flight
        slots -- bounding the WS ``_pending_acks`` window at the cap without
        starving control frames or losing any event (R8-M4).
        """
        if max_event_batch <= 0:
            # Not expected in the below-cap branch (remaining >= 1 there), but
            # be safe: send only control frames this pass. Decide by WIRE type,
            # not the stored ``kind`` (round-12 external LOW).
            return [e for e in entries if not _entry_is_event_batch(e)]
        kept: list = []
        eb_count = 0
        for e in entries:
            if _entry_is_event_batch(e):
                if eb_count >= max_event_batch:
                    continue
                eb_count += 1
            kept.append(e)
        return kept

    async def _run_send_loop(self) -> None:  # noqa: PLR0912  central send-outcome dispatch (one branch per transport outcome)
        """Drain the buffer to the transport in batches."""
        assert self._transport is not None
        assert self._stop_event is not None
        # Capture the buffer ONCE into a live local (see
        # ``_ack_watchdog_loop`` for the full rationale): ``stop()`` can
        # set ``self._buffer = None`` on another thread, and a supervisor
        # reconnect can start this task afterwards. Hold the reference;
        # every BufferStore method re-checks ``_closed`` under its own
        # lock, so calls remain safe no-ops once teardown closes it.
        buffer = self._buffer
        if buffer is None:
            return

        while not self._stop_event.is_set():
            # Exclude entries already sent-and-awaiting-ack. Without this
            # the same un-acked event_batch entries re-drain every
            # iteration and re-send at line rate (they are only removed
            # on ack), flooding the brain and starving fresh entries
            # behind a full in-flight window (R5-M2). The excluded set is
            # the buffer-entry ids currently in ``_pending_acks``.
            in_flight = {entry_id for entry_id, _sent_at in self._pending_acks.values()}
            # Backpressure: STRICTLY cap concurrent un-acked event_batch
            # frames. When at the cap, keep draining CONTROL frames
            # (command acks/results confirm on send and never become
            # pending, so they must not be starved) but exclude
            # event_batch so ``_pending_acks`` never grows past the cap
            # (R7-MED). Below the cap, drain everything.
            if len(self._pending_acks) >= _MAX_IN_FLIGHT_BATCHES:
                entries = buffer.drain(
                    self._send_batch_size,
                    exclude_ids=in_flight or None,
                    exclude_kinds={"event_batch"},
                )
                # The SQL exclude is on the stored ``kind``; also drop any entry
                # whose PARSED WIRE type is event_batch (a mislabelled one that
                # the kind-based exclude missed) so a corrupt kind cannot slip an
                # event_batch past the in-flight cap and register for an ack
                # (round-12 external LOW). The drained set here is only control
                # frames + any mislabelled ones, so the extra peek is cheap.
                entries = [e for e in entries if not _entry_is_event_batch(e)]
            else:
                entries = buffer.drain(self._send_batch_size, exclude_ids=in_flight or None)
                # Below the cap, still trim event_batch entries to the
                # REMAINING in-flight slots on a defer-acks (WS) transport. The
                # cap check above only fires once ALREADY at/over the cap, so a
                # single drain of up to _SEND_BATCH_SIZE starting from e.g. 255
                # pending could register 500 more and overshoot to 755 (R8-M4).
                # Control frames are never trimmed (they confirm on send and
                # never become pending). Not applied to long-poll
                # (confirm_on_send: _pending_acks stays 0, so an unconditional
                # cap would throttle its 500-frame drain to the cap).
                if not getattr(self._transport, "confirm_on_send", False):
                    remaining = _MAX_IN_FLIGHT_BATCHES - len(self._pending_acks)
                    entries = self._cap_event_batch_entries(entries, remaining)
            if not entries:
                await asyncio.sleep(_SEND_IDLE_SLEEP)
                continue

            frames = [e.payload for e in entries]
            try:
                accepted = await self._transport.send_frames(frames)
            except UndeliverableFrameError as exc:
                # One or more buffered frames are locally undeliverable
                # (unparseable / unsigned / oversize): DETERMINISTIC. The
                # socket is healthy, so stay connected -- confirm/register the
                # frames that DID ship and force-purge the undeliverable ones.
                self._handle_undeliverable_drop(buffer, entries, exc)
                continue
            except (PartialSendError, ConnectionError):
                # TRANSPORT failure (socket dropped mid-batch, connection
                # error, retryable HTTP status like 3xx/5xx/429). The
                # batch was not delivered. We do NOT confirm anything, and
                # we do NOT increment any drop counter: a flaky connection
                # is not the batch's fault (R6-F4/R7). Re-raise so the
                # supervisor reconnects; the batch re-drains next session
                # (the brain dedups any that did land).
                raise
            except UploadRetryableError:
                # Long-poll TRANSIENT partial store (a 200 that stored
                # fewer than sent: a brain-side DB deadlock / pool timeout
                # / transient skip). The frames ARE deliverable; the brain
                # just could not store them this instant, and it now drops-
                # and-acks every DETERMINISTIC failure at source, so a
                # partial store is ALWAYS a genuine transient that recovers
                # within a few rounds. Re-send the whole batch after a
                # MODEST capped backoff (never counts toward any drop
                # budget, R7-HIGH2). Blocking here is harmless: during a
                # real transient outage nothing can be stored anyway, so no
                # deliverable frame is being starved. The brain dedups
                # already-stored frames on replay.
                #
                # But a FEW partial-store causes are not resolved by the
                # backoff alone -- a send-side session/identity signature
                # failure (only a fresh session_nonce rebuilds the binding)
                # or a protocol-version skew during a rolling upgrade. Only
                # the RECEIVE loop reconnects on its own error, so after
                # ``_MAX_CONSECUTIVE_RETRYABLE`` zero-progress retries force a
                # reconnect here too (loss-free: the buffer is preserved).
                self._consecutive_retryable += 1
                if self._consecutive_retryable >= _MAX_CONSECUTIVE_RETRYABLE:
                    raise ConnectionError(
                        "long-poll made no confirmed progress across "
                        f"{self._consecutive_retryable} partial-store retries; "
                        "reconnecting for a fresh session",
                    ) from None
                await self._backoff_retry()
                continue
            except (PayloadTooLargeError, UploadContentRejectedError) as exc:
                # Long-poll CONTENT problem: 413 (too large) or 400/415/422
                # (malformed). Reduce/split a multi-frame batch so valid
                # siblings still deliver; drop only a SINGLE frame that
                # persistently fails, after a bounded budget (R7-MED). Use a
                # SMALL FIXED delay (not the growing transient backoff) so an
                # isolated poison frame at the buffer HEAD is isolated and
                # dropped within seconds and does not starve control frames
                # queued behind it for minutes (R8: head-of-line fix).
                self._handle_content_reject(buffer, entries, exc)
                await asyncio.sleep(_CONTENT_REJECT_DELAY)
                continue

            # A successful send resets the transient retry backoff. It does
            # NOT restore ``_send_batch_size`` -- a 413 shrink is undone only
            # when the oversized frame is dropped (``_handle_content_reject``)
            # or on reconnect, so a still-oversized batch is not immediately
            # re-sent full and re-413'd.
            self._reset_send_backoff()

            # Defer ``buffer.confirm`` for event_batch frames until
            # the matching ``event_batch_ack`` arrives from the
            # brain. Without the application-level ack, confirming
            # on ws-layer ``send()`` success would silently lose
            # events whenever the brain dropped the batch (deadlock,
            # restart mid-batch, ingest queue full, transient skip).
            # A WS event_batch is confirmed ONLY by a real ack; if the
            # ack never comes, the watchdog re-sends (never confirms,
            # never drops on a counter).
            #
            # Heartbeat / agent_status / command_ack / command_result
            # frames don't need acks (they're observability + control,
            # not data); confirm those immediately regardless of mode.
            now = datetime.now(UTC)
            # A transport that confirms on send (long-poll: the HTTP 200
            # IS the ack, there is no ack frame coming) confirms
            # event_batch entries immediately; every other transport
            # (WebSocket) DEFERS confirmation until the brain's
            # ``event_batch_ack`` arrives. There is no "legacy pre-1.5
            # brain" fallback that confirms on socket-write: that path
            # deleted un-acked events whenever a modern brain merely
            # withheld an ack for a transient DB skip (R7-HIGH1). On WS,
            # an event_batch is confirmed ONLY by a real ack.
            defer_acks = not getattr(self._transport, "confirm_on_send", False)
            confirm_now = self._confirm_or_register(entries, accepted, defer_acks, now)
            if confirm_now:
                buffer.confirm(confirm_now)
            if self._heartbeat is not None and accepted:
                self._heartbeat.record_flush(now)

    def _confirm_or_register(
        self,
        entries: list,
        accepted: list[int],
        defer_acks: bool,
        now: datetime,
    ) -> list[int]:
        """Decide which just-sent entries to confirm now vs. defer to ack.

        For a ``defer_acks`` transport (WebSocket), a frame whose parsed WIRE
        type is ``event_batch`` goes into ``_pending_acks`` to await its
        ``event_batch_ack`` -- unless the ack ALREADY arrived during the send
        await (recorded in ``_acks_seen_early``, R6-F7), in which case confirm
        it now. Everything else (control frames, and every frame on a
        confirm-on-send transport) confirms immediately. The decision keys off
        the parsed wire type, NOT the buffer entry's ``kind`` metadata, so
        neither mislabel direction can drop an ack or await one that never
        comes (round-10 + round-11 external).
        """
        confirm_now: list[int] = []
        for i in accepted:
            entry = entries[i]
            # On a confirm-on-send transport (long-poll) the HTTP 200 IS the
            # ack, so every frame confirms now. Only a defer-acks transport (WS)
            # awaits a per-frame ack.
            if not defer_acks:
                confirm_now.append(entry.id)
                continue
            # Trust the parsed WIRE type over the buffer entry's ``kind``
            # metadata, and inspect EVERY accepted entry (do NOT pre-gate on
            # ``kind``). Both mislabel directions must be handled (round-10 +
            # round-11 external): a real event_batch mislabelled kind="heartbeat"
            # must still be DEFERRED (else it is confirmed on socket-write and
            # SILENTLY LOST if the brain restarts / rejects / drops the ack),
            # and a heartbeat mislabelled kind="event_batch" must be confirmed
            # now (it will never receive an event_batch_ack). Defer solely when
            # the wire type is event_batch with a usable id.
            frame_id, frame_type = _peek_frame_meta(entry.payload)
            if frame_id is None or frame_type != "event_batch":
                confirm_now.append(entry.id)
            elif frame_id in self._acks_seen_early:
                # The ack for this frame already arrived while we were
                # awaiting the send, before we could register it. Confirm
                # now instead of registering it for a deadline that has
                # already passed (R6-F7).
                self._acks_seen_early.discard(frame_id)
                confirm_now.append(entry.id)
            elif frame_id not in self._pending_acks:
                # First send of this frame. Record sent_at ONCE. The
                # in-flight drain filter means we should not re-send a
                # pending frame, but guard the overwrite anyway so a
                # re-send can never slide the watchdog deadline (that was
                # the mechanism by which the deadline was never reached,
                # R5-M2).
                self._pending_acks[frame_id] = (entry.id, now)
        return confirm_now

    def _handle_undeliverable_drop(
        self,
        buffer: BufferStore,
        entries: list,
        exc: UndeliverableFrameError,
    ) -> None:
        """Purge frames the transport reported as locally undeliverable.

        The transport shipped ``exc.accepted`` and rejected ``exc.drop_indices``
        (unparseable / unsigned / oversize). Confirm / register the sent frames
        exactly as a normal send would, then FORCE-PURGE the undeliverable ones
        directly. The purge must bypass :meth:`_confirm_or_register`: an
        undeliverable ``event_batch`` can still peek to a real frame_id, so
        registering it would defer an ``event_batch_ack`` that never arrives and
        pin the buffer head forever -- the exact loop this drop exists to
        eliminate.
        """
        now = datetime.now(UTC)
        # A partial ship is forward progress; reset the transient backoff.
        self._reset_send_backoff()
        defer_acks = not getattr(self._transport, "confirm_on_send", False)
        confirm_now = self._confirm_or_register(entries, exc.accepted, defer_acks, now)
        purge = confirm_now + [entries[i].id for i in exc.drop_indices]
        if purge:
            buffer.confirm(purge)
        if self._heartbeat is not None and exc.accepted:
            self._heartbeat.record_flush(now)

    async def _backoff_retry(self) -> None:
        """Sleep the current TRANSIENT-retry backoff, then grow it.

        Called ONLY on a long-poll transient partial store
        (``UploadRetryableError``). The delay doubles per consecutive
        failure up to ``_SEND_BACKOFF_MAX`` (5s) so a struggling brain is
        not hammered while it recovers (R7-HIGH2); a successful send
        resets it via :meth:`_reset_send_backoff`. The CONTENT-reject path
        does NOT use this -- it uses a small fixed ``_CONTENT_REJECT_DELAY``
        so an isolated poison frame is dropped fast instead of starving
        frames behind it (R8).
        """
        await asyncio.sleep(self._send_backoff)
        self._send_backoff = min(self._send_backoff * 2, _SEND_BACKOFF_MAX)

    def _reset_send_backoff(self) -> None:
        """Reset the retryable-outcome backoff + progress counter after a
        successful send.

        The backoff and the ``_consecutive_retryable`` reconnect counter are
        reset (a successful send is confirmed forward progress, R9). The
        adaptive ``_send_batch_size`` is deliberately left where a 413 shrank
        it (it is restored to the full ``_SEND_BATCH_SIZE`` on reconnect):
        growing it back mid connection would oscillate straight into the same
        413 on the next oversized frame.
        """
        self._send_backoff = _SEND_BACKOFF_INITIAL
        self._consecutive_retryable = 0

    def _handle_content_reject(
        self,
        buffer: BufferStore,
        entries: list,
        exc: Exception,
    ) -> None:
        """React to a long-poll CONTENT rejection (413 / 415 / 422).

        The batch was reachable and the brain answered -- it just will
        not store THIS content. Two cases:

        * **Multi-frame batch** -- one frame is (probably) the culprit
          but we don't know which. Halve ``_send_batch_size`` (floor
          ``_MIN_SEND_BATCH``) so the next drain pulls a smaller batch;
          repeated rejections bisect down to the single offending frame,
          while valid siblings keep delivering (R7-MED). Nothing is
          dropped here.
        * **Single-frame batch** -- the culprit is isolated. An
          ``event_batch`` frame gets its attempt counter bumped and is
          dropped ONLY once it crosses ``_MAX_SEND_ATTEMPTS`` (bounded,
          logged). A control frame (command_result / command_ack /
          agent_status / heartbeat) cannot be re-batched or bisected and
          would pin the queue forever, so it is dropped immediately with
          a warning -- losing it merely times the command out server
          side, which is recoverable, whereas pinning the queue loses
          everything behind it (R7-MED).

        Once the isolated offender is actually DROPPED, ``_send_batch_size``
        is restored to the full ``_SEND_BATCH_SIZE`` (R8): the shrink only
        existed to isolate that frame, so the remaining (valid) buffer must
        ship at full width again rather than dribble one frame per POST for
        the rest of the connection.
        """
        if len(entries) > 1:
            new_size = max(self._send_batch_size // 2, _MIN_SEND_BATCH)
            if new_size != self._send_batch_size:
                self._send_batch_size = new_size
            logger.warning(
                "content-reject on %d-frame batch (%s); shrinking "
                "send batch size to %d to isolate the offending frame",
                len(entries),
                type(exc).__name__,
                self._send_batch_size,
            )
            return

        # Single frame: the culprit is isolated. Bump the DEDICATED
        # content-reject budget (not the shared ``attempts`` metric, R8-H1)
        # and drop only once THAT budget is exhausted.
        entry = entries[0]
        if entry.kind == "event_batch":
            buffer.increment_content_rejects([entry.id])
            dropped = buffer.evict_if_exhausted([entry.id], _MAX_SEND_ATTEMPTS)
            if dropped:
                logger.error(
                    "dropping event_batch entry %d after %d content "
                    "rejections (%s); frame is undeliverable",
                    entry.id,
                    _MAX_SEND_ATTEMPTS,
                    type(exc).__name__,
                )
                # Offender gone -- restore full width so the rest of the
                # buffer stops dribbling one frame per POST (R8).
                self._send_batch_size = _SEND_BATCH_SIZE
            return

        # An isolated CONTROL frame cannot be split or re-batched and
        # would pin the send queue indefinitely. Drop it now (confirm =
        # delete) so data frames behind it keep flowing.
        logger.error(
            "dropping undeliverable %s control frame (entry %d): %s",
            entry.kind,
            entry.id,
            type(exc).__name__,
        )
        buffer.confirm([entry.id])
        # Offender gone -- restore full width (R8).
        self._send_batch_size = _SEND_BATCH_SIZE

    async def _run_receive_loop(self) -> None:
        """Read inbound frames and dispatch commands.

        Frames have already been parsed + HMAC-verified by the
        transport's :class:`FrameVerifier` before they get here.
        """
        assert self._transport is not None

        async def on_frame(frame: Frame) -> None:
            await self._handle_inbound(frame)

        await self._transport.receive_frames(on_frame)

    async def _ack_watchdog_loop(self) -> None:
        """Re-send WS event_batch entries whose ack has not arrived.

        Any ``_pending_acks`` entry older than ``_ACK_DEADLINE_SECONDS``
        is removed from the pending map so the send loop re-drains and
        re-sends it. It is NEVER confirmed (deleted) and NEVER dropped
        here: an event_batch is confirmed only by a real ack (positive
        proof of storage) or by the buffer's size overflow eviction. The
        brain dedups a re-sent-but-already-stored batch by its
        content-derived event_id, so replay collapses to one row.

        Exits cleanly when ``stop_event`` is set so the supervisor
        can tear the connection down without a stale task.
        """
        assert self._stop_event is not None
        # Capture the buffer ONCE into a local. ``stop()`` may set
        # ``self._buffer = None`` on another thread during teardown; if a
        # supervisor reconnect races that (the send loop dropped, the
        # supervisor spun up a fresh connection task-group) this watchdog
        # task can start AFTER the buffer was Noned. A bare
        # ``assert self._buffer is not None`` would then surface an
        # AssertionError traceback as teardown noise. Hold a live
        # reference instead - the BufferStore's own in-lock ``_closed``
        # check makes every operation a safe no-op once it is closed.
        buffer = self._buffer
        if buffer is None:
            return
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_ACK_WATCHDOG_INTERVAL_SECONDS,
                )
                return
            except TimeoutError:
                pass
            now = datetime.now(UTC)
            stale_ids: list[int] = []
            stale_keys: list[str] = []
            for frame_id, (entry_id, sent_at) in self._pending_acks.items():
                if (now - sent_at).total_seconds() < _ACK_DEADLINE_SECONDS:
                    continue
                stale_keys.append(frame_id)
                stale_ids.append(entry_id)
            if not stale_ids:
                continue
            # An event_batch not acked within the deadline is NEITHER
            # confirmed NOR dropped: we only remove its pending-ack
            # tracking so it re-drains and re-sends on the next send-loop
            # pass. The brain dedups the re-send by content-derived
            # event_id, so a batch that WAS stored but whose ack was lost
            # collapses to one row. An event_batch is deleted ONLY by a
            # real ack (positive proof of storage) or by the buffer's
            # size/byte overflow eviction (oldest-first, logged) -- never
            # by a retry counter. This removes the two 1.7.0 loss paths:
            # the "assume pre-1.5 brain -> confirm on socket write"
            # legacy flip (which deleted un-acked events when a modern
            # brain merely withheld an ack for a transient DB skip) and
            # the attempt-count quarantine (which dropped a deliverable
            # batch after ~15 min of a socket/DB outage or a persistent
            # transient rejection). Retry is unbounded in count and
            # bounded only by the buffer size (R7-HIGH1).
            for k in stale_keys:
                self._pending_acks.pop(k, None)
            # increment_attempts is kept for the operator-visible
            # stuck-entry metric only; nothing evicts on it for WS.
            buffer.increment_attempts(stale_ids)
            logger.warning(
                "z4j agent: brain did not ack %d event_batch frame(s) "
                "within %.0fs; re-sending (the brain dedups any that "
                "landed). Entries are retried until acked; only the "
                "buffer size cap bounds this.",
                len(stale_ids),
                _ACK_DEADLINE_SECONDS,
            )

    async def _handle_inbound(self, frame: Frame) -> None:
        """Route one verified inbound frame."""
        assert self._dispatcher is not None
        if isinstance(frame, CommandFrame):
            await self._dispatcher.handle(frame)
            return
        if isinstance(frame, EventBatchAckFrame):
            self._handle_event_batch_ack(frame)
            return
        logger.debug("z4j agent received %s frame", frame.type)

    def _handle_event_batch_ack(self, frame: EventBatchAckFrame) -> None:
        """Confirm-and-evict the buffer entry matching the ack.

        Brain emits one ``event_batch_ack`` per committed
        ``event_batch`` carrying ``payload.acked_id = original
        event_batch.id``. We look up the entry id we deferred when
        sending and tell the buffer to drop it. An ack is the ONLY
        thing that confirms (deletes) an event_batch entry on the WS
        path -- positive proof the brain durably stored it.
        """
        assert self._buffer is not None
        acked_id = frame.payload.acked_id
        if not acked_id:
            # Brain sent ack without correlation id (some pre-release
            # or 1.5.x spec drift). Nothing safe to confirm against;
            # the entry stays pending and the watchdog re-sends it.
            return
        pending = self._pending_acks.pop(acked_id, None)
        if pending is None:
            # Either a duplicate/aged-out ack (safe to ignore), OR an ack
            # that arrived while the send loop was still awaiting the send
            # that produced this frame and had not yet registered it in
            # ``_pending_acks``. Record it so the send loop confirms it at
            # registration time instead of leaving it to the 90s watchdog
            # (R6-F7). Bounded: in normal operation this set is ~empty.
            if len(self._acks_seen_early) < _MAX_IN_FLIGHT_BATCHES * 4:
                self._acks_seen_early.add(acked_id)
            return
        entry_id, _sent_at = pending
        self._buffer.confirm([entry_id])

    # ------------------------------------------------------------------
    # Transport selection
    # ------------------------------------------------------------------

    def _build_transport(
        self,
        hmac_secret: bytes,
    ) -> WebSocketTransport | LongPollTransport:
        """Construct the transport based on the configured mode.

        ``config.transport`` selects between:

        - ``"ws"`` (or ``"auto"``, the default): WebSocket transport.
          Lowest latency. The right choice for almost everyone.
        - ``"longpoll"``: HTTPS long-poll fallback. Use when a
          corporate proxy strips ``Upgrade`` headers, or in any
          deployment where the WebSocket round-trip is unreliable.
          Slightly higher latency, byte-identical envelope HMAC and
          frame routing on the brain side.

        ``"auto"`` is currently a synonym for ``"ws"``. A future
        version may add WebSocket-then-fallback negotiation; that
        would land here.
        """
        capabilities: dict[str, list[str]] = {}
        for name, engine in self.engines.items():
            capabilities[name] = sorted(engine.capabilities())
        for name, scheduler in self.schedulers.items():
            capabilities[name] = sorted(scheduler.capabilities())

        if self.config.transport == "longpoll":
            # Long-poll has no handshake frame, so the agent has to
            # advertise its own ``agent_id`` up-front. Operators
            # discover the agent_id when they mint the token from
            # the brain's /agents page; it is also stamped on every
            # event the agent emits via the env var
            # ``Z4J_AGENT_ID`` (read by ``Config``).
            return LongPollTransport(
                brain_url=str(self.config.brain_url),
                token=self.config.token.get_secret_value(),
                project_id=self.config.project_id,
                agent_id=str(getattr(self.config, "agent_id", "") or ""),
                framework_name=self.framework.name,
                engines=list(self.engines),
                schedulers=list(self.schedulers),
                capabilities=capabilities,
                hmac_secret=hmac_secret,
                dev_mode=self.config.dev_mode,
            )

        # Worker-first protocol (1.2.0+): generate a stable worker_id
        # for this process. ``<framework>-<pid>-<unix_ms>`` is unique
        # across gunicorn workers (different pids), Celery workers
        # (different pids), and process restarts (different start
        # times) under the same agent_token. The brain registers
        # each worker as a discrete connection slot keyed by this
        # id, so multiple workers under the same agent_id no longer
        # fight (the 1.1.x flap pattern).
        import os as _os
        import time as _time
        from datetime import UTC, datetime

        worker_started_at = datetime.now(UTC)
        worker_id = f"{self.framework.name}-{_os.getpid()}-{int(_time.time() * 1000)}"

        # worker_role: explicit operator config takes precedence;
        # adapter default if it declared one (1.2.0+ adapter API);
        # else None (legacy / untyped).
        worker_role = (
            getattr(self.config, "worker_role", None)
            or getattr(self.framework, "default_worker_role", None)
            or None
        )
        if callable(worker_role):
            try:
                worker_role = worker_role()
            except Exception:
                worker_role = None

        return WebSocketTransport(
            brain_url=str(self.config.brain_url),
            token=self.config.token.get_secret_value(),
            project_id=self.config.project_id,
            framework_name=self.framework.name,
            engines=list(self.engines),
            schedulers=list(self.schedulers),
            capabilities=capabilities,
            hmac_secret=hmac_secret,
            agent_name=self.config.agent_name,
            dev_mode=self.config.dev_mode,
            worker_id=worker_id,
            worker_role=worker_role,
            worker_pid=_os.getpid(),
            worker_started_at=worker_started_at,
        )


class _StopRequested(Z4JError):  # noqa: N818  internal control-flow sentinel, not a user-facing error
    """Internal sentinel used to cancel the asyncio TaskGroup on stop."""

    code = "stop_requested"

    def __init__(self) -> None:
        super().__init__("agent stop requested")


async def safe_close(transport: WebSocketTransport) -> None:
    """Close the transport, swallowing any error."""
    try:
        await transport.close()
    except Exception:
        logger.exception("error while closing transport")


def _first(eg: BaseExceptionGroup[BaseException]) -> BaseException:
    """Return the first leaf exception inside an exception group.

    The supervisor uses this to surface a representative cause when
    logging - the full group is still attached via ``__cause__``.
    """
    for exc in eg.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            return _first(exc)
        return exc
    return eg


# Every Nth consecutive failure of the same class re-emits an INFO
# summary so an operator tailing the log sees a heartbeat-rate
# signal that the agent is still alive and trying. 10 is chosen so
# that with the connection-error backoff schedule (1s, 2s, 4s, 8s,
# 16s, 30s, 30s, ...) the first INFO summary fires roughly two
# minutes into a streak - quick enough that someone investigating
# does not assume the agent has died, slow enough that the log
# does not look like spam.
_LOG_SUMMARY_EVERY = 10


def _log_disconnect(
    error_class: str | None,
    err: BaseException,
    count: int,
) -> None:
    """Tiered logging for supervisor disconnects.

    First failure in a streak emits a WARNING with full context. Every
    subsequent failure of the same class drops to DEBUG. Every Nth
    consecutive failure escalates back to INFO as a "still trying"
    summary. This mirrors the SDK convention used by Sentry, OpenTelemetry,
    New Relic, and Datadog: an unreachable backend should never cause
    the host application's stderr to flood.

    Args:
        error_class: One of ``"auth"`` / ``"protocol"`` / ``"connection"``
            (or ``None`` if the supervisor saw no error - the caller
            never invokes this helper in that case).
        err: The leaf exception extracted from the supervisor's
            ExceptionGroup.
        count: Position of this failure within the current streak.
            ``1`` means it is the first failure since the last
            successful handshake (or since the runtime started).
    """
    if count == 1:
        # First failure of a streak: full WARNING with the class and
        # an explicit pointer to the per-attempt DEBUG channel for
        # operators who want the firehose.
        if error_class == "auth":
            logger.warning(
                "z4j agent auth rejected: %s. Will retry with backoff "
                "(10min cap). Subsequent identical failures suppressed; "
                "set the z4j.agent logger to DEBUG to see every attempt.",
                err,
            )
        elif error_class == "protocol":
            logger.warning(
                "z4j agent protocol error: %s. Will retry with backoff. "
                "Subsequent identical failures suppressed; set the "
                "z4j.agent logger to DEBUG to see every attempt.",
                err,
            )
        else:
            logger.warning(
                "z4j agent disconnected: %s. Will retry with backoff. "
                "Subsequent identical failures suppressed; set the "
                "z4j.agent logger to DEBUG to see every attempt.",
                err,
            )
    elif count % _LOG_SUMMARY_EVERY == 0:
        # Periodic INFO so a tailing operator can see the agent is
        # still alive. Class is part of the message so the message
        # is also useful when piped through a log aggregator.
        logger.info(
            "z4j agent: still disconnected (#%d %s)",
            count,
            error_class,
        )
    else:
        # Suppressed mid-streak failure. DEBUG keeps the per-attempt
        # detail available to anyone investigating without ever
        # surfacing in the default-level host log.
        logger.debug(
            "z4j agent disconnect retry #%d (%s): %s",
            count,
            error_class,
            err,
        )


__all__ = ["AgentRuntime", "RuntimeState"]
