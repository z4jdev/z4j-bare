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

logger = logging.getLogger("z4j.agent.runtime")


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

# Auth-error backoff schedule. AuthenticationError indicates the
# brain rejected the agent's bearer token (mismatched HMAC, revoked
# agent, rotated secret, etc.). Pre-1.1.2 this was treated as fatal
# and stopped the reconnect loop forever, leaving the agent process
# alive but offline until manually restarted (the failure mode that
# bit tasks.jfk.work on 2026-04-28). 1.1.2 retries forever with a
# longer cap (10 minutes) so an operator-initiated token rotation
# eventually succeeds without flooding the brain's auth endpoint.
_AUTH_RECONNECT_INITIAL = 10.0
_AUTH_RECONNECT_MAX = 600.0
_AUTH_RECONNECT_JITTER = 0.3

# ProtocolError handling. These come from the wire layer: bad
# handshake, version skew during a brain restart, malformed frames
# from a partial deploy. Almost always transient - same backoff as
# ConnectionError. Pre-1.1.2 these were also fatal; same fix.
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

        # Per-class consecutive-failure counters (reset on clean
        # connect cycle). Surfaced to the doctor CLI and heartbeat
        # frames so operators can distinguish "agent is in a flap
        # loop" from "agent is fine, just disconnected once."
        self._auth_error_count = 0
        self._protocol_error_count = 0
        self._connection_error_count = 0

        # Reconnect-now event. set() by the SIGHUP handler from
        # z4j_bare.control.install_sighup_handler. The supervisor
        # checks this between cycles and skips its backoff timer
        # when set, going straight to the next connect attempt.
        # Shared across all framework adapters because they all
        # use this same runtime.
        self._reconnect_now: asyncio.Event | None = None

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

        self._heartbeat = Heartbeat(
            buffer=self._buffer,
            stop_event=self._stop_event,
            interval=10.0,
            health_provider=_collect_engine_health,
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

        ``_connect_and_run`` runs an ``asyncio.TaskGroup`` which raises
        ``ExceptionGroup`` (PEP 654) when any child task fails. We use
        ``except*`` to peel out the failure classes so the longer
        AuthenticationError schedule is applied independently of the
        others.
        """
        assert self._stop_event is not None
        # Per-class delay state - each error class has its own
        # exponential timer so a flap in one doesn't reset the others.
        delay_conn = _RECONNECT_INITIAL
        delay_proto = _PROTOCOL_RECONNECT_INITIAL
        delay_auth = _AUTH_RECONNECT_INITIAL
        # Per-class consecutive-failure counters for diagnostics.
        # Surfaced in heartbeats and the doctor CLI so operators can
        # see "5 consecutive AuthErrors" without grepping logs.
        self._auth_error_count = 0
        self._protocol_error_count = 0
        self._connection_error_count = 0
        stop_loop = False
        while not stop_loop and not self._stop_event.is_set():
            error_class: str | None = None
            err: BaseException | None = None
            try:
                await self._connect_and_run()
                # Clean cycle - reset every per-class delay so the
                # next failure starts at the floor again.
                delay_conn = _RECONNECT_INITIAL
                delay_proto = _PROTOCOL_RECONNECT_INITIAL
                delay_auth = _AUTH_RECONNECT_INITIAL
                self._auth_error_count = 0
                self._protocol_error_count = 0
                self._connection_error_count = 0
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
                if error_class == "auth":
                    logger.warning(
                        "z4j agent auth rejected (#%d consecutive): %s; "
                        "retrying with backoff (10min cap)",
                        self._auth_error_count,
                        err,
                    )
                elif error_class == "protocol":
                    logger.warning(
                        "z4j agent protocol error (#%d consecutive): %s; "
                        "retrying with backoff",
                        self._protocol_error_count,
                        err,
                    )
                else:
                    logger.warning("z4j agent disconnected: %s", err)

            if self._stop_event.is_set():
                return

            # Pick the schedule for the most recent error class.
            if error_class == "auth":
                base = delay_auth
                jitter = _AUTH_RECONNECT_JITTER
                cap = _AUTH_RECONNECT_MAX
            elif error_class == "protocol":
                base = delay_proto
                jitter = _RECONNECT_JITTER
                cap = _PROTOCOL_RECONNECT_MAX
            else:
                base = delay_conn
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
                delay_auth = min(delay_auth * 2.0, cap)
            elif error_class == "protocol":
                delay_proto = min(delay_proto * 2.0, cap)
            else:
                delay_conn = min(delay_conn * 2.0, cap)

    async def _connect_and_run(self) -> None:
        """One supervisor cycle: connect, run tasks, until disconnect."""
        assert self._transport is not None
        assert self._stop_event is not None
        assert self._heartbeat is not None
        assert self._dispatcher is not None

        await self._transport.connect()
        self._heartbeat.set_interval(float(self._transport.heartbeat_interval))

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._run_send_loop(), name="z4j-send")
            tg.create_task(self._heartbeat.run(), name="z4j-heartbeat")
            tg.create_task(self._run_receive_loop(), name="z4j-receive")
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

            accepted_ids = [entries[i].id for i in accepted]
            self._buffer.confirm(accepted_ids)
            if self._heartbeat is not None and accepted_ids:
                self._heartbeat.record_flush(datetime.now(UTC))

    async def _run_receive_loop(self) -> None:
        """Read inbound frames and dispatch commands.

        Frames have already been parsed + HMAC-verified by the
        transport's :class:`FrameVerifier` before they get here.
        """
        assert self._transport is not None

        async def on_frame(frame: Frame) -> None:
            await self._handle_inbound(frame)

        await self._transport.receive_frames(on_frame)

    async def _handle_inbound(self, frame: Frame) -> None:
        """Route one verified inbound frame."""
        assert self._dispatcher is not None
        if isinstance(frame, CommandFrame):
            await self._dispatcher.handle(frame)
            return
        logger.debug("z4j agent received %s frame", frame.type)

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


__all__ = ["AgentRuntime", "RuntimeState"]
