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

    # ------------------------------------------------------------------
    # Sync API (safe from any thread/context)
    # ------------------------------------------------------------------

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

        ``_connect_and_run`` runs an ``asyncio.TaskGroup`` which raises
        ``ExceptionGroup`` (PEP 654) when any child task fails. We use
        ``except*`` to peel out the failure classes we care about
        without losing the others - a bare ``except`` would silently
        swallow ``AuthenticationError`` if it was wrapped in a group.
        """
        assert self._stop_event is not None
        delay = _RECONNECT_INITIAL
        stop_loop = False
        while not stop_loop and not self._stop_event.is_set():
            fatal: BaseException | None = None
            transient: BaseException | None = None
            try:
                await self._connect_and_run()
                delay = _RECONNECT_INITIAL  # reset on clean cycle
            except* _StopRequested:
                # Watchdog cancelled the group on stop_event - clean exit.
                # PEP 654 forbids ``return`` inside an ``except*`` block,
                # so we set a flag and break out at the next loop guard.
                stop_loop = True
            except* AuthenticationError as eg:
                fatal = _first(eg)
            except* ProtocolError as eg:
                fatal = _first(eg)
            except* ConnectionError as eg:
                transient = _first(eg)
            except* Exception as eg:  # noqa: BLE001
                # Anything else is treated as transient - reconnect
                # after the backoff. Log the full group so operators
                # see every sub-exception, not just the first one.
                logger.exception(
                    "z4j agent unexpected supervisor error",
                    exc_info=eg,
                )
                transient = _first(eg)

            if stop_loop:
                break

            if fatal is not None:
                logger.error(
                    "z4j agent fatal: %s; stopping reconnect loop",
                    fatal,
                )
                return
            if transient is not None:
                logger.warning("z4j agent disconnected: %s", transient)

            if self._stop_event.is_set():
                return

            sleep_for = delay + random.uniform(0, delay * _RECONNECT_JITTER)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_for)
                return  # stop requested
            except TimeoutError:
                pass
            delay = min(delay * 2.0, _RECONNECT_MAX)

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
