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
import base64
import logging
import random
import threading
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

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
from z4j_bare.transport.longpoll import LongPollTransport
from z4j_bare.transport.websocket import PartialSendError, WebSocketTransport

if TYPE_CHECKING:
    from z4j_core.models import Config

logger = logging.getLogger("z4j.runtime.supervisor")


def _peek_frame_id(payload: bytes) -> str | None:
    """Cheap extraction of the ``id`` field from a serialized frame.

    The send loop hands the buffer the pre-serialized frame bytes;
    to correlate the brain's ``event_batch_ack`` to the buffer entry
    that produced the batch, we need the frame.id at send time
    without paying for a full Pydantic parse on every entry. JSON
    parsing of a few hundred bytes is single-digit microseconds and
    runs only at send time (not for every event).

    Returns ``None`` if parsing fails. Caller falls back to the
    legacy "confirm immediately" path for that entry, so a
    pathologically corrupt payload doesn't pin the buffer.
    """
    import json as _json

    try:
        decoded = _json.loads(payload)
    except Exception:  # noqa: BLE001
        return None
    if isinstance(decoded, dict):
        fid = decoded.get("id")
        if isinstance(fid, str):
            return fid
    return None


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
_SEND_IDLE_SLEEP = 0.05
_RECONNECT_INITIAL = 1.0
_RECONNECT_MAX = 30.0
_RECONNECT_JITTER = 0.3

#: How long an event_batch frame can sit in
#: ``_pending_acks`` before the watchdog evicts it. First eviction
#: also flips ``_brain_supports_acks`` to False (legacy-mode
#: fallback). 90s is generous to allow first-batch drain under
#: heavy fanout (brain ingest is bounded around 80
#: events/s/connection); going lower thrashes legacy-mode under
#: load and silently undermines the per-batch ack delivery
#: guarantee.
_ACK_DEADLINE_SECONDS = 90.0
#: Watchdog poll cadence. Sleeps this long between sweeps. Aim for
#: the fastest cadence that doesn't burn CPU while keeping eviction
#: precise to within a few seconds.
_ACK_WATCHDOG_INTERVAL_SECONDS = 2.0

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

    The brain returns the per-project secret as
    ``base64.urlsafe_b64encode(raw_32_bytes)`` on agent mint
    (see ``z4j_brain.api.agents.CreateAgentResponse.hmac_secret``).
    The operator pastes that string into ``Z4J_HMAC_SECRET`` /
    ``settings.Z4J["hmac_secret"]`` and the agent must decode it
    back to the same 32 bytes the brain uses for HMAC.

    Padding is added if missing (urlsafe-base64 is sometimes
    written without trailing ``=`` for shell ergonomics). On a
    decode failure we raise ``ValueError`` rather than silently
    falling back to UTF-8 encoding - the latter would mismatch
    the brain's signature and produce a confusing
    ``SignatureError`` at first frame instead of a clear config
    error at start-up.
    """
    candidate = value.strip()
    if not candidate:
        raise ValueError("hmac_secret is empty")
    padded = candidate + "=" * (-len(candidate) % 4)
    try:
        return base64.urlsafe_b64decode(padded)
    except (ValueError, base64.binascii.Error) as exc:  # type: ignore[attr-defined]
        raise ValueError(
            "hmac_secret must be urlsafe-base64 (the value the brain "
            "returns from POST /agents); got an undecodable string",
        ) from exc


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
        self.schedulers: dict[str, SchedulerAdapter] = {
            s.name: s for s in (schedulers or [])
        }

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
        # Whether the brain we're talking to speaks the v1.5 ack
        # protocol. True at the start of each session. The watchdog
        # flips it to False ONLY if a batch ages out without ever
        # seeing an ack AND we have not observed any ack from this
        # brain yet (``_brain_acks_observed`` below). Once we see
        # one ack we know the brain is 1.5+ and never fall back -
        # a missing ack thereafter means a poison batch the brain
        # rejected, NOT a legacy brain, and the agent must let the
        # buffer entry stay un-confirmed so the next reconnect
        # re-sends it.
        self._brain_supports_acks: bool = True
        # Sticky "we've seen at least one ack from this brain" flag.
        # Set on first ``EventBatchAckFrame`` arrival; reset only on
        # ``_connect_and_run`` cycle (the brain may have changed
        # versions across reconnect).
        self._brain_acks_observed: bool = False

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
                "z4j agent runtime: background loop did not become ready "
                "within 5s; tearing down",
            )
            self._abort_start()
            raise RuntimeError(
                "z4j agent runtime failed to start: background loop did "
                "not become ready",
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
        except Exception:  # noqa: BLE001
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
            except Exception:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001
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
            id=f"ev_{_secrets.token_hex(6)}",
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
        except Exception:  # noqa: BLE001
            logger.exception("z4j agent failed to create asyncio loop")
            self._loop_ready.set()  # unblock the caller's wait()
            return

        # Loop and stop_event are both live now; safe to publish.
        self._loop_ready.set()

        try:
            loop.run_until_complete(self._main())
        except Exception:  # noqa: BLE001
            logger.exception("z4j agent runtime loop crashed")
        finally:
            try:
                loop.close()
            except Exception:  # noqa: BLE001
                pass
            self._loop = None

    async def _main(self) -> None:
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
                                    v, default=str,
                                )
                            else:
                                result[f"{name}.{k}"] = str(v)
                    except Exception:  # noqa: BLE001
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

            buffer_depth = (
                self._buffer.size() if self._buffer is not None else 0
            )
            session_age: float | None = None
            last_connect: datetime | None = self._last_successful_connect_at
            if last_connect is not None:
                session_age = max(
                    (datetime.now(UTC) - last_connect).total_seconds(), 0.0,
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

        self._heartbeat = Heartbeat(
            buffer=self._buffer,
            stop_event=self._stop_event,
            interval=10.0,
            health_provider=_collect_engine_health,
            status_provider=_collect_agent_status,
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
                    def _sink(action: str, schedule: object,
                              _name: str = scheduler_name) -> None:
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
                schedule_inventory_tasks.append(asyncio.create_task(
                    self._emit_schedule_snapshot(scheduler, reason="boot"),
                    name=f"z4j-schedule-inventory-{getattr(scheduler, 'name', 'unknown')}",
                ))

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
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass

            # Disconnect in reverse order, swallowing per-adapter errors
            # so one failing adapter cannot strand another's signals.
            for scheduler in reversed(connected_schedulers):
                disconnect = getattr(scheduler, "disconnect_signals", None)
                if callable(disconnect):
                    try:
                        disconnect()
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "z4j agent: scheduler %s disconnect_signals raised",
                            getattr(scheduler, "name", scheduler),
                        )

            for engine in reversed(connected_engines):
                disconnect = getattr(engine, "disconnect_signals", None)
                if callable(disconnect):
                    try:
                        disconnect()
                    except Exception:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001
            logger.exception(
                "z4j agent: scheduler %s list_schedules failed (reason=%s)",
                scheduler_name, reason,
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
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j agent: scheduler %s yielded a Schedule that "
                    "failed model_dump",
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
            scheduler_name, len(schedules_payload), reason,
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
                    self._stop_event.wait(), timeout=interval,
                )
                # Stop signalled - exit cleanly.
                return
            except asyncio.TimeoutError:
                # Normal tick - drain every scheduler.
                pass
            for scheduler in schedulers:
                if self._stop_event.is_set():
                    return
                await self._emit_schedule_snapshot(
                    scheduler, reason="periodic",
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
        self, scheduler_name: str, action: str, schedule: object,
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
                except Exception:  # noqa: BLE001
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

    async def _supervise(self) -> None:
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
            except* Exception as eg:  # noqa: BLE001
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
                    self._auth_error_count if error_class == "auth"
                    else self._protocol_error_count if error_class == "protocol"
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

            sleep_for = base + random.uniform(0, base * jitter)
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
                        "z4j agent reconnect requested via SIGHUP; "
                        "skipping remaining backoff",
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
        assert self._heartbeat is not None
        assert self._dispatcher is not None

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
            self._connection_error_count
            + self._protocol_error_count
            + self._auth_error_count
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

        self._heartbeat.set_interval(float(self._transport.heartbeat_interval))

        # Reset per-connection ack tracking
        # before the send loop starts on this connection. The
        # ``_brain_supports_acks`` probe re-runs on every connect so
        # an operator who restarts a 1.5+ brain after a 1.4 brain
        # picks up ack mode again without restarting agents.
        self._pending_acks.clear()
        self._brain_supports_acks = True
        self._brain_acks_observed = False

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._run_send_loop(), name="z4j-send")
            tg.create_task(self._heartbeat.run(), name="z4j-heartbeat")
            tg.create_task(self._run_receive_loop(), name="z4j-receive")
            tg.create_task(self._ack_watchdog_loop(), name="z4j-ack-watchdog")
            # One "watchdog" task exits when stop_event fires, cancelling the group.
            tg.create_task(self._wait_for_stop(), name="z4j-watchdog")

    async def _wait_for_stop(self) -> None:
        assert self._stop_event is not None
        await self._stop_event.wait()
        raise _StopRequested()

    async def _run_send_loop(self) -> None:
        """Drain the buffer to the transport in batches."""
        assert self._buffer is not None
        assert self._transport is not None
        assert self._stop_event is not None

        while not self._stop_event.is_set():
            entries = self._buffer.drain(_SEND_BATCH_SIZE)
            if not entries:
                await asyncio.sleep(_SEND_IDLE_SLEEP)
                continue

            frames = [e.payload for e in entries]
            try:
                accepted = await self._transport.send_frames(frames)
            except PartialSendError:
                # The socket dropped mid-batch. We do NOT confirm any
                # frames because "sent to socket" doesn't guarantee the
                # brain received them (the TCP connection may have died
                # mid-flight). Instead, increment attempts on the entire
                # batch and let the reconnect loop retry everything.
                # The brain's ingestor deduplicates by (occurred_at, id)
                # so replayed events are safe.
                self._buffer.increment_attempts([e.id for e in entries])
                raise
            except ConnectionError:
                # Bubble up to the supervisor so it can reconnect.
                # Mark attempt counts so stuck entries are visible.
                self._buffer.increment_attempts([e.id for e in entries])
                raise

            # Defer ``buffer.confirm`` for event_batch frames until
            # the matching ``event_batch_ack`` arrives from the
            # brain. Without the application-level ack, confirming
            # on ws-layer ``send()`` success would silently lose
            # events whenever the brain dropped the batch (deadlock,
            # restart mid-batch, ingest queue full).
            #
            # Bidi compat: an old brain (pre-1.5) does NOT send acks.
            # The watchdog (_ack_watchdog_loop) detects stale entries
            # and sets ``_brain_supports_acks = False`` after the
            # first ack-window expires unanswered; subsequent batches
            # then confirm immediately (legacy mode). One round-trip
            # of latency per agent on the first batch is the cost of
            # the safe negotiation - acceptable.
            #
            # Heartbeat / agent_status / command_ack / command_result
            # frames don't need acks (they're observability + control,
            # not data); confirm those immediately regardless of mode.
            now = datetime.now(UTC)
            confirm_now: list[int] = []
            for i in accepted:
                entry = entries[i]
                if entry.kind == "event_batch" and self._brain_supports_acks:
                    frame_id = _peek_frame_id(entry.payload)
                    if frame_id is None:
                        confirm_now.append(entry.id)
                    else:
                        self._pending_acks[frame_id] = (entry.id, now)
                else:
                    confirm_now.append(entry.id)

            if confirm_now:
                self._buffer.confirm(confirm_now)
            if self._heartbeat is not None and accepted:
                self._heartbeat.record_flush(now)

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
        """Bug A.2: evict ``_pending_acks`` entries past their deadline.

        Two responsibilities:

        1. Detect a brain that doesn't speak the v1.5 ack protocol.
           If any pending entry ages past ``_ACK_DEADLINE_SECONDS``
           we flip ``_brain_supports_acks`` to False so subsequent
           batches confirm immediately (legacy mode). The first
           batch on a fresh connect pays at most one deadline of
           latency before legacy mode kicks in - acceptable.
        2. Evict the now-stale pending entries by confirming them
           in the buffer (so they don't pin the buffer indefinitely
           after legacy-mode is engaged). Safe: the brain dedupes
           replays via the content-derived event_id (Bug X-B fix),
           so evicted-then-re-shipped events collapse to one row.

        Exits cleanly when ``stop_event`` is set so the supervisor
        can tear the connection down without a stale task.
        """
        assert self._stop_event is not None
        assert self._buffer is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_ACK_WATCHDOG_INTERVAL_SECONDS,
                )
                return
            except (asyncio.TimeoutError, TimeoutError):
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
            for k in stale_keys:
                self._pending_acks.pop(k, None)
            if self._brain_acks_observed:
                # We've seen at least one ack from this brain in this
                # session, so it's a 1.5+ build. A missing ack here
                # means the brain rejected the batch (poison events,
                # transient DB error, etc.). Re-increment the buffer
                # entries' attempt counter and DO NOT confirm - the
                # next reconnect cycle re-sends them, and the brain's
                # content-derived event_id collapses any
                # duplicate-with-prior-success that was committed.
                # Bounded retries via the buffer's attempt counter
                # eventually evict a true poison entry; the operator
                # sees the elevated drop counter in the next
                # heartbeat.
                logger.warning(
                    "z4j agent: brain did not ack event_batch within "
                    "%.0fs; brain previously acked so treating as "
                    "rejected batch, will re-send on reconnect "
                    "(stale_count=%d)",
                    _ACK_DEADLINE_SECONDS, len(stale_ids),
                )
                self._buffer.increment_attempts(stale_ids)
                continue
            if self._brain_supports_acks:
                # First-batch timeout AND we've never seen an ack:
                # the brain is pre-1.5 and never sends acks. Fall
                # back to legacy "confirm immediately" mode for this
                # session.
                self._brain_supports_acks = False
                logger.warning(
                    "z4j agent: brain did not ack event_batch within %.0fs "
                    "and no prior ack observed; assuming pre-1.5 brain "
                    "and switching to legacy mode for this session "
                    "(stale_count=%d)",
                    _ACK_DEADLINE_SECONDS, len(stale_ids),
                )
            self._buffer.confirm(stale_ids)

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

    def _handle_event_batch_ack(self, frame: "EventBatchAckFrame") -> None:
        """Confirm-and-evict the buffer entry matching the ack.

        Brain emits one ``event_batch_ack`` per committed
        ``event_batch`` carrying ``payload.acked_id = original
        event_batch.id``. We look up the entry id we deferred when
        sending and tell the buffer to drop it. The first ack
        received also confirms the brain speaks the v1.5 ack-aware
        protocol.
        """
        assert self._buffer is not None
        # First ack pins the brain as "speaks the v1.5 ack protocol".
        # The watchdog must NOT flip to legacy mode after this point;
        # any subsequent missing ack means the brain rejected the
        # batch (poison events, DB outage), not a legacy brain.
        self._brain_acks_observed = True
        acked_id = frame.payload.acked_id
        if not acked_id:
            # Brain sent ack without correlation id (some pre-release
            # or 1.5.x spec drift). Nothing safe to confirm against;
            # the watchdog handles eviction.
            return
        pending = self._pending_acks.pop(acked_id, None)
        if pending is None:
            # Could be a duplicate ack (network retry) or an ack for a
            # batch that already aged out via the watchdog. Safe to
            # ignore.
            return
        entry_id, _sent_at = pending
        self._buffer.confirm([entry_id])

    # ------------------------------------------------------------------
    # Transport selection
    # ------------------------------------------------------------------

    def _build_transport(
        self, hmac_secret: bytes,
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
        worker_id = (
            f"{self.framework.name}-{_os.getpid()}-"
            f"{int(_time.time() * 1000)}"
        )

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
            except Exception:  # noqa: BLE001
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


class _StopRequested(Z4JError):
    """Internal sentinel used to cancel the asyncio TaskGroup on stop."""

    code = "stop_requested"

    def __init__(self) -> None:
        super().__init__("agent stop requested")


async def safe_close(transport: WebSocketTransport) -> None:
    """Close the transport, swallowing any error."""
    try:
        await transport.close()
    except Exception:  # noqa: BLE001
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
    error_class: str | None, err: BaseException, count: int,
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
            "z4j agent: still disconnected (#%d %s)", count, error_class,
        )
    else:
        # Suppressed mid-streak failure. DEBUG keeps the per-attempt
        # detail available to anyone investigating without ever
        # surfacing in the default-level host log.
        logger.debug(
            "z4j agent disconnect retry #%d (%s): %s",
            count, error_class, err,
        )


__all__ = ["AgentRuntime", "RuntimeState"]
