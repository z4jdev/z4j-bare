"""The :class:`AgentRuntime` - the heart of z4j-bare.

The runtime orchestrates every subsystem:

- Reads the resolved :class:`Config` from the framework adapter
- Opens the local SQLite buffer
- Starts a background thread that runs an asyncio event loop
- Inside that loop, runs four cooperating tasks:
    1. Connect/reconnect loop for the configured transport. ``"auto"`` is
       currently a synonym for ``"ws"``; long-poll must be selected explicitly.
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
import hashlib
import hmac
import logging
import os
import random
import sys
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

from z4j_core.errors import (
    AgentIncompatibleError,
    AuthenticationError,
    ProtocolError,
    Z4JError,
)
from z4j_core.models import Event
from z4j_core.protocols import FrameworkAdapter, QueueEngineAdapter, SchedulerAdapter
from z4j_core.transport.frames import (
    RETRY_BY_REFERENCE_CAPABILITY,
    CommandFrame,
    EventBatchAckFrame,
    EventBatchFrame,
    EventBatchPayload,
    Frame,
    serialize_frame,
)

from z4j_bare.buffer import EXTERNAL_SCHEDULE_ENTRY_KIND, BufferStore
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
    from collections.abc import Callable

    from z4j_core.models import Config

logger = logging.getLogger("z4j.runtime.supervisor")

# Leave ample room for the transport HMAC envelope beneath the default 1 MiB
# Brain ceiling.  Stable snapshots are split at this unsigned-frame bound and
# each resulting frame is verified again after complete serialization.
_EXTERNAL_SNAPSHOT_FRAME_TARGET_BYTES = 768 * 1024
_EXTERNAL_SNAPSHOT_ROW_PAYLOAD_BYTES = _EXTERNAL_SNAPSHOT_FRAME_TARGET_BYTES - 32 * 1024


@dataclass(frozen=True, slots=True)
class _ExternalScheduleAuthority:
    """Brain-issued publication authority for one scheduler source scope."""

    stream_id: str
    epoch_uuid: str
    epoch_number: int
    adapter_instance_id: str
    owner: str
    source_scope: str
    stable_source: bool


def _advertised_capabilities(
    engines: dict[str, QueueEngineAdapter],
    schedulers: dict[str, SchedulerAdapter],
) -> dict[str, list[str]]:
    """Build capabilities from the adapter objects actually loaded.

    Retry authority is adapter-owned. Only an adapter that explicitly attests
    the safe contract receives the versioned marker, so a current runtime paired
    with an old adapter fails closed.
    """
    capabilities: dict[str, list[str]] = {}
    for name, engine in engines.items():
        advertised = set(engine.capabilities())
        if getattr(engine, "safe_retry_by_reference", False) is True:
            advertised.add(RETRY_BY_REFERENCE_CAPABILITY)
        capabilities[name] = sorted(advertised)
    for name, scheduler in schedulers.items():
        capabilities[name] = sorted(scheduler.capabilities())
    return capabilities


async def _await_in_daemon_thread(fn: Callable[[], Any]) -> Any:
    """Run a blocking ``fn`` on a DAEMON thread and await its result.

    M5: ``asyncio.to_thread`` runs on the loop's default ThreadPoolExecutor,
    whose workers are NON-daemon and registered in ``_threads_queues``, so
    CPython's ``concurrent.futures`` atexit hook JOINS them on interpreter exit.
    A genuinely-wedged blocking call (e.g. an orphan scan stuck on a locked
    SQLite DB) would therefore hang process exit indefinitely, past every
    bounded drain. A raw daemon thread is excluded from BOTH that hook and
    threading's own atexit (daemon threads are never joined), so exit stays
    bounded while the result/exception is bridged back to the loop. Cancelling
    the await abandons the thread (it finishes in the background); that is safe
    here because the only such call, the orphan scan, serialises every buffer
    write behind the buffer's lock and fails closed on a closed buffer.
    """
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()

    def _settle(setter: Callable[[Any], None], value: Any) -> None:
        # RL3: the awaiter may have been CANCELLED (shutdown) before the daemon
        # worker finishes; setting a result/exception on an already-cancelled or
        # already-done future raises InvalidStateError. Re-check ``done()`` INSIDE
        # the loop callback (the only place it is safe to read, single-threaded).
        # The loop may also be closing during shutdown; if so nothing awaits fut.
        def _apply() -> None:
            if not fut.done():
                setter(value)

        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(_apply)

    def _runner() -> None:
        try:
            result = fn()
        except BaseException as exc:  # bridge any failure back to the awaiter
            _settle(fut.set_exception, exc)
        else:
            _settle(fut.set_result, result)

    threading.Thread(target=_runner, name="z4j-orphan-scan", daemon=True).start()
    return await fut


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
        # Wedged provider thread; abandon the wait. NOTE: the default executor's
        # workers are NON-daemon and are JOINED (not reaped) by
        # concurrent.futures' atexit hook, so a genuinely-wedged pool worker can
        # still delay interpreter exit; this bound only caps how long WE wait
        # here. Latency-critical blocking calls that could wedge indefinitely
        # (the orphan scan) run on dedicated DAEMON threads instead (see
        # _await_in_daemon_thread, M5) so they are never atexit-joined.
        with contextlib.suppress(Exception):
            executor.shutdown(wait=False)


def _derive_deployment_id(hmac_secret: Any) -> str:
    """H8/RH5: per-deployment buffer fingerprint derived from the agent's
    hmac_secret (truncated SHA-256; reveals nothing about the secret).

    MUST read the UNMASKED secret via ``get_secret_value()`` -- pydantic
    ``str(SecretStr)`` is the literal ``"**********"`` for EVERY secret, which
    made this fingerprint a single constant across all deployments and defeated
    H8 cross-deployment orphan isolation entirely (the RH5 regression, which
    shipped once). Falls back to ``str(...)`` only for a plain-string secret
    (no get_secret_value), which is not a SecretStr and so not masked.

    runtime:152: hash the DECODED key bytes, not the base64 TEXT. The secret is
    urlsafe-base64 and the HMAC identity is ``decode_agent_hmac_secret(raw)``,
    which strips whitespace and pads -- so padded / unpadded / newline-wrapped
    encodings of the SAME key produce the SAME HMAC identity. Hashing the raw
    text instead gave those equivalent forms DIFFERENT deployment ids, so one
    deployment's own workers (secret written slightly differently) refused to
    adopt each other's buffers. Decode first so the fingerprint tracks the key
    identity. Fall back to the raw bytes only if the secret is not decodable
    (which the HMAC path itself would already have rejected at startup).
    """
    getter = getattr(hmac_secret, "get_secret_value", None)
    raw = getter() if callable(getter) else str(hmac_secret)
    try:
        key_bytes = _decode_hmac_secret(raw)
    except Exception:
        key_bytes = raw.encode("utf-8")
    return hashlib.sha256(key_bytes).hexdigest()[:16]


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
#: rather than pinning valid siblings behind it.
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
#: so a deliverable batch is never dropped. At the 90s WS ack
#: deadline this is ~15 minutes of active rejection before a genuinely
#: undeliverable event_batch is dropped.
_MAX_SEND_ATTEMPTS = 10
#: Backpressure cap on concurrent un-acked event_batch frames (WS
#: deferred-ack mode). Bounds ``_pending_acks`` and therefore the
#: ``exclude_ids`` set passed to ``buffer.drain`` well under SQLite's
#: 32766-bound-parameter ceiling, and stops the agent piling
#: unbounded un-acked batches on a slow/stalled brain.
_MAX_IN_FLIGHT_BATCHES = 256
#: Long-poll TRANSIENT-retry backoff (seconds): a transient partial
#: store sleeps this long (doubling per consecutive failure, capped)
#: before re-sending the whole batch, so a struggling brain is not
#: hammered. Capped LOW (5s, not 30s): the brain now drops-
#: and-acks every deterministic failure at source, so a partial store is
#: always a genuine transient that self-heals within a few rounds; a 30s
#: cap only slowed recovery and lengthened the window a real outage held
#: the single-threaded send loop.
_SEND_BACKOFF_INITIAL = 0.5
_SEND_BACKOFF_MAX = 5.0
#: Fixed inter-attempt delay (seconds) on the long-poll CONTENT-reject
#: path (413 / 415 / 422; a bare 400 is request-level and retried).
#: Deliberately SMALL and NON-growing so
#: bisection + the bounded per-frame drop clear a poison frame within a
#: couple of seconds instead of the ~150s a growing backoff took, which
#: starved every control frame queued behind the poison (the code
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
#: no frame is dropped (reconnect preserves the buffer).
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

# Incompatible-agent handling. The brain has closed with a code that says this
# build is unacceptable: an unsupported wire protocol, or a version outside the
# supported range. Nothing about reconnecting changes that answer, so retrying
# on the protocol schedule above is a storm against a brain that already said
# no. What fixes it is a person upgrading something, on human timescales.
#
# Not fatal on purpose. The agent lives inside somebody's application, so
# stopping for good would mean an operator who corrects the deployment gets no
# agent back until they restart their app.
_INCOMPATIBLE_RECONNECT_INITIAL = 120.0
_INCOMPATIBLE_RECONNECT_MAX = 3600.0


@dataclass(frozen=True, slots=True)
class _ReconnectSchedule:
    """Retry timing for one supervisor error class."""

    initial: float
    jitter: float
    maximum: float


#: Every error class the supervisor can classify, with its retry timing.
#:
#: This table is the single source of truth for those class names. The
#: consecutive-failure streak, the sleep before the next attempt and the
#: schedule advance are all looked up by the same key, so a class cannot be
#: known to one of them and unknown to another. When those lived in parallel
#: hand-written branches they drifted: the branch that chose the streak had no
#: arm for ``incompatible`` and fell through to the connection streak, which
#: reads zero on a fresh rejection. A streak of zero is not a first failure, so
#: the one ERROR that tells an operator to upgrade something was never emitted
#: -- the agent logged a mid-streak summary at INFO on every attempt instead.
#: An unknown class now raises a KeyError at the first failure rather than
#: quietly reporting another class's numbers.
_RECONNECT_SCHEDULES: Final[dict[str, _ReconnectSchedule]] = {
    "auth": _ReconnectSchedule(
        initial=_AUTH_RECONNECT_INITIAL,
        jitter=_AUTH_RECONNECT_JITTER,
        maximum=_AUTH_RECONNECT_MAX,
    ),
    "incompatible": _ReconnectSchedule(
        initial=_INCOMPATIBLE_RECONNECT_INITIAL,
        jitter=_RECONNECT_JITTER,
        maximum=_INCOMPATIBLE_RECONNECT_MAX,
    ),
    "protocol": _ReconnectSchedule(
        initial=_PROTOCOL_RECONNECT_INITIAL,
        jitter=_RECONNECT_JITTER,
        maximum=_PROTOCOL_RECONNECT_MAX,
    ),
    "connection": _ReconnectSchedule(
        initial=_RECONNECT_INITIAL,
        jitter=_RECONNECT_JITTER,
        maximum=_RECONNECT_MAX,
    ),
}

# ``except*`` executes every matching arm. Keep the precedence used to merge
# those arms in one production helper so tests exercise the decision that the
# supervisor itself makes rather than a copied table.
_SUPERVISOR_FAILURE_RANK: Final[dict[str, int]] = {
    "connection": 0,
    "protocol": 1,
    "incompatible": 2,
    "auth": 3,
}

#: Error class used when the supervisor completes an iteration without
#: classifying a failure. ``_connect_and_run`` always unwinds through
#: ``_StopRequested`` today, so this is a floor rather than a live path: it
#: keeps a clean return on a retry schedule instead of spinning with no delay.
_DEFAULT_ERROR_CLASS: Final = "connection"


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
        # A monotonically-increasing START EPOCH. Each start() that wins the
        # STOPPED->STARTING transition captures its epoch; the RUNNING publish only
        # fires while this is STILL the current epoch, so a start() that a
        # concurrent stop()+restart superseded while it was blocked in bring-up
        # cannot publish RUNNING over the new owner (a value check on the state
        # enum alone could not tell its own STARTING from the newer one).
        self._start_epoch = 0
        self._loop_ready = threading.Event()
        # Set when the loop thread fails to reach a healthy steady
        # state (loop-creation error, or _main raising/returning during setup),
        # so start() reports the failure instead of RUNNING on a dead loop.
        self._loop_error = threading.Event()
        self._stop_event: asyncio.Event | None = None

        self._buffer: BufferStore | None = None
        self._transport: WebSocketTransport | None = None
        self._dispatcher: CommandDispatcher | None = None
        self._heartbeat: Heartbeat | None = None

        # Per-class consecutive-failure streaks and connect-retry
        # backoff, both keyed by the supervisor's error class. Reset by
        # ``_connect_and_run`` on successful handshake (which is the
        # only place that observably happens, since the supervisor's
        # task-group exit is always via _StopRequested). Streaks are
        # surfaced to the doctor CLI and heartbeat frames so operators
        # can distinguish "agent is in a flap loop" from "agent is
        # fine, just disconnected once." Delays are instance state
        # rather than stack-locals inside _supervise, so a long-stable
        # connection starts its next disconnect at the floor instead of
        # staying pinned at the cap it reached hours earlier.
        self._failure_streaks: dict[str, int] = {}
        self._delays: dict[str, float] = {}
        self._reset_reconnect_state()

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
        # The send loop consults this set at registration time
        # and confirms immediately if the ack already arrived. Bounded;
        # cleared on reconnect.
        self._acks_seen_early: set[str] = set()
        # Current long-poll retry backoff (seconds), grown on consecutive
        # retryable outcomes (transient partial store, content reject,
        # 413) and reset on a successful send. Gives a genuinely-poison
        # frame a real backoff instead of a 50ms hot-loop.
        self._send_backoff: float = _SEND_BACKOFF_INITIAL
        # Consecutive long-poll partial-store retries with zero confirmed
        # progress. Reset on any successful send and on reconnect; when it
        # crosses ``_MAX_CONSECUTIVE_RETRYABLE`` the send loop forces a
        # reconnect so a persistent session/version skew is not re-POSTed
        # forever.
        self._consecutive_retryable: int = 0
        # Current per-send batch size. Starts at ``_SEND_BATCH_SIZE`` and
        # is halved on an HTTP 413 (long-poll body too large) down to a
        # floor of 1, so an agent behind a small server body cap adapts
        # instead of looping on an oversized POST.
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
        # Boundary D: no scheduler observation is projected until the Brain
        # assigns an exact stream epoch.  Locks serialize source observation
        # with durable sequence reservation for each scheduler adapter.
        self._external_schedule_authorities: dict[
            str,
            _ExternalScheduleAuthority,
        ] = {}
        self._external_schedule_locks: dict[str, asyncio.Lock] = {}
        self._snapshot_signal_pending: set[str] = set()
        self._snapshot_signal_dirty: set[str] = set()
        self._schedule_observation_tasks: set[asyncio.Task[None]] = set()

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

    def _reset_reconnect_state(self) -> None:
        """Put every error class back to a zero streak and its floor delay."""
        self._failure_streaks = dict.fromkeys(_RECONNECT_SCHEDULES, 0)
        self._delays = {name: sched.initial for name, sched in _RECONNECT_SCHEDULES.items()}

    @property
    def _auth_error_count(self) -> int:
        return self._failure_streaks["auth"]

    @property
    def _protocol_error_count(self) -> int:
        """Both protocol-class streaks as one number.

        The ``agent_status`` frame carries a single protocol streak field, and
        an incompatible-agent rejection is a protocol-level rejection. Summing
        is exact rather than approximate: a successful handshake clears every
        streak, so at most one class is ever non-zero.
        """
        return self._failure_streaks["protocol"] + self._failure_streaks["incompatible"]

    @property
    def _connection_error_count(self) -> int:
        return self._failure_streaks["connection"]

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

        # H6: VALIDATE the hmac_secret (decode + length) BEFORE deriving the
        # deployment fingerprint and constructing the BufferStore below. The
        # deployment_id derives from the secret, and BufferStore.__init__ then
        # discards/restamps a reused-pid buffer using it -- so an INVALID secret
        # (non-base64, or a key that decodes to < 32 bytes) would PURGE/restamp
        # pending buffered data BEFORE the later _main validation rejects the key
        # and refuses to run. Validate first, mutate second: refuse to start
        # (raise) here so the buffer is never touched for a config we reject.
        try:
            _secret_bytes = _decode_hmac_secret(self.config.hmac_secret.get_secret_value())
        except Exception as exc:
            raise RuntimeError(
                "z4j agent refusing to start: hmac_secret is not valid "
                "urlsafe-base64 (the value the brain returns from POST /agents).",
            ) from exc
        if len(_secret_bytes) < 32:
            raise RuntimeError(
                "z4j agent refusing to start: hmac_secret must decode to at "
                f"least 32 bytes; got {len(_secret_bytes)}.",
            )

        with self._state_lock:
            if self._state != RuntimeState.STOPPED:
                return
            self._state = RuntimeState.STARTING
            # Claim this start's epoch. A concurrent stop()+restart that
            # supersedes us while we are blocked in bring-up will bump it, so the
            # RUNNING publish below can detect it is no longer the current owner.
            self._start_epoch += 1
            my_epoch = self._start_epoch
            # M6: record the pid that owns the live threads. reinit_after_fork
            # compares it to detect a genuine fork child (pid changed, the
            # inherited threads are dead) vs a same-process double-call (pid
            # unchanged, the threads are really alive and must not be orphaned).
            self._started_pid = os.getpid()

        # H8: derive a per-deployment fingerprint from the agent's secret so
        # orphan adoption never crosses two z4j deployments that share a per-uid
        # buffer root (same OS user, different projects/secrets). Stable per
        # deployment and reveals nothing about the secret (truncated SHA-256).
        deployment_id = _derive_deployment_id(self.config.hmac_secret)

        # RM7: buffer construction + bring-up runs under a guard that resets the
        # state to STOPPED on ANY failure. BufferStore.__init__ can raise (an
        # unwritable path, a transient disk-full, an H5 discard error); without
        # this the runtime would be stranded in STARTING with ``_buffer=None``, so
        # a later start() (e.g. after the disk recovers) would no-op at the
        # STOPPED guard above and the agent would stay disabled forever.
        # ``_abort_start`` closes any partial buffer, joins the thread, and resets
        # to STOPPED so the next start() genuinely retries.
        try:
            self._start_bringup(deployment_id)
        except BaseException:
            self._abort_start()
            raise

        # A concurrent stop() (and possibly a restart) may have
        # moved us out of STARTING while _start_bringup was blocked (e.g. in a slow
        # connect_signals). Publish RUNNING ONLY if we are STILL both STARTING and
        # the CURRENT epoch. Two supersede cases:
        #   - a plain stop() (state left STARTING): we own the handles we brought
        #     up, so tear them down (the abort).
        #   - a stop()+restart by a NEWER start() (epoch advanced): the newer start
        #     now owns self._loop/_thread/_buffer, so we must NOT tear those down.
        #     Our own orphaned thread is a bounded daemon that self-terminates and,
        #     via the identity guard in _run_loop's finally, cannot clobber the new
        #     owner's handles. Just return.
        with self._state_lock:
            superseded_by_newer_start = self._start_epoch != my_epoch
            lost_to_stop = self._state != RuntimeState.STARTING
            if not lost_to_stop and not superseded_by_newer_start:
                self._state = RuntimeState.RUNNING
        if superseded_by_newer_start:
            logger.warning(
                "z4j agent runtime: start superseded by a newer start "
                "(epoch %d -> %d); leaving the current owner's runtime intact",
                my_epoch,
                self._start_epoch,
            )
            return
        if lost_to_stop:
            self._abort_start()
            return

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

    def _start_bringup(self, deployment_id: str) -> None:
        """Open the fresh buffer, spawn the loop thread, and wait for readiness.

        Raises on any failure so start()'s RM7 guard resets state to STOPPED.
        """
        # Open the buffer on the caller's thread (fast, synchronous).
        self._buffer = BufferStore(
            path=self.config.buffer_path,
            max_entries=self.config.buffer_max_events,
            max_bytes=self.config.buffer_max_bytes,
            deployment_id=deployment_id,
        )

        # Spawn the background thread that owns the event loop.
        # ``_loop_ready`` MUST be cleared before the thread starts so
        # this caller's wait() cannot return spuriously on a leftover
        # signal from a previous start/stop cycle.
        self._loop_ready.clear()
        self._loop_error.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="z4j-agent",
            daemon=True,
        )
        self._thread.start()

        if not self._loop_ready.wait(timeout=5.0):
            # The background thread never signalled ready - either it failed to
            # construct the loop or the JIT thread start was delayed past the
            # timeout. Raise so start()'s guard resets state to STOPPED cleanly.
            logger.error(
                "z4j agent runtime: background loop did not become ready within 5s",
            )
            raise RuntimeError(
                "z4j agent runtime failed to start: background loop did not become ready",
            )

        # _loop_ready is now the STEADY-STATE signal, but _run_loop also
        # sets it (with _loop_error) when _main dies during setup. So a ready
        # signal alone is not proof of health -- check the error flag + that the
        # thread is still alive, and raise (RM7 guard -> STOPPED, retryable) if
        # the loop died on the way up.
        if self._loop_error.is_set() or not (self._thread is not None and self._thread.is_alive()):
            raise RuntimeError(
                "z4j agent runtime failed to start: the event loop did not reach "
                "a healthy running state",
            )

    def reinit_after_fork(self) -> None:
        """Re-establish this runtime in a freshly ``os.fork()``ed child.

        A fork copies memory but NOT threads, so a child inherits a
        runtime object whose ``_state`` looks RUNNING while its transport
        / heartbeat / dispatcher threads are dead. Events captured in the
        child then pile into a buffer no live thread drains, and because
        the process-singleton is non-None the child's own install path
        short-circuits. This is the gunicorn/uWSGI ``--preload`` failure
        mode.

        This method forces a clean restart: reset to STOPPED, drop the
        parent's dead thread + buffer handle, re-resolve the per-PID
        buffer path for THIS child (so siblings don't share one file),
        and ``start()`` fresh threads + a new brain connection.

        It is NOT auto-wired via ``os.register_at_fork``: a blanket
        at-fork handler would also fire in Celery's prefork pool
        children, where the agent must NOT run. Operators wire it
        explicitly for web servers only, via ``z4j_bare.post_fork()`` in
        a gunicorn ``post_fork`` / uWSGI ``@postfork`` hook.
        """
        # M6: refuse to run in a NON-forked process whose threads are still
        # alive. If the recorded owner pid equals ours we did NOT fork -- this
        # is a stray second post_fork() call, or the hook wired in a
        # non-forking context. Blindly resetting here would overwrite the live
        # _loop / _thread / _buffer handles so stop() could never signal the
        # old loop, leaking its WS session + BufferStore forever and spawning
        # a duplicate stack that drains the same buffer file. Do nothing.
        # M6: STARTING as well as RUNNING -- a same-process reinit during the
        # brief startup window would otherwise slip past and reset a live
        # (starting) stack.
        if getattr(self, "_started_pid", None) == os.getpid() and self._state in (
            RuntimeState.RUNNING,
            RuntimeState.STARTING,
            RuntimeState.STOPPING,  # RM8: a reinit racing shutdown is also not a fork
        ):
            logger.warning(
                "z4j agent: reinit_after_fork() called in the SAME process "
                "(pid=%d) as the running agent -- not a fork. Ignoring to "
                "avoid orphaning the live agent thread. Wire post_fork() only "
                "in a real fork hook (gunicorn post_fork / uWSGI @postfork).",
                os.getpid(),
            )
            return

        with self._state_lock:
            self._state = RuntimeState.STOPPED
            self._thread = None
            old_buffer = self._buffer
            self._buffer = None
        # M6: this child inherited the parent's buffer as a shared open file
        # description. Close ONLY the inherited lock fd (os.close, no flock
        # unlock) so the parent keeps its live ownership lock now and its
        # buffer becomes adoptable once it dies, without leaking the fd for the
        # child's whole life. Do NOT call old_buffer.close() -- its
        # _release_lock would flock(LOCK_UN) the shared OFD and drop the
        # PARENT's lock.
        if old_buffer is not None:
            old_buffer.release_fork_inherited_lock()
        # M5: re-derive the per-PID buffer filename in the SAME directory the
        # config already points at, instead of forcing it back to
        # ~/.z4j via _default_buffer_path(). An operator who set an explicit
        # buffer_path (provisioned/persistent storage, tighter perms) and uses
        # the documented gunicorn --preload + post_fork flow otherwise had
        # every worker silently relocate its buffer to ~/.z4j (or the tmp
        # fallback), violating the durability path they configured. Only the
        # pid-specific filename changes; the directory is preserved.
        with contextlib.suppress(Exception):
            child_buffer_path = self.config.buffer_path.parent / f"buffer-{os.getpid()}.sqlite"
            self.config = self.config.model_copy(
                update={"buffer_path": child_buffer_path},
            )
        self.start()

    def _abort_start(self) -> None:
        """Best-effort cleanup when start() fails to come up cleanly."""
        # SIGNAL the loop to stop BEFORE dropping the thread handle. A
        # readiness timeout can fire while _main is still blocked in a slow
        # connect_signals(); without this the thread handle is dropped but the
        # loop later RESUMES and keeps running (an untracked live loop a retry
        # start() could overlap). Setting the cooperative stop event makes the
        # loop tear itself down the moment the blocking setup call returns, so at
        # most one loop is ever live.
        if self._loop is not None and self._stop_event is not None:
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._buffer is not None:
            try:
                # Bound the abort close with a lock deadline, mirroring
                # stop(). Without it, a signal callback or a wedged orphan-scan
                # holding the buffer lock at startup-timeout would block the abort
                # (and thus start()) indefinitely. The abandon-on-timeout path
                # marks the buffer closed and keeps the ownership flock held to
                # process exit, so a bounded close is safe.
                self._buffer.close(lock_timeout=2.0)
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
        import time as _time

        deadline_started = _time.monotonic()
        with self._state_lock:
            if self._state in (RuntimeState.STOPPED, RuntimeState.STOPPING):
                return
            self._state = RuntimeState.STOPPING

        # If the loop already closed (it raced its own teardown at
        # _run_loop's finally before self._loop was cleared), call_soon_threadsafe
        # raises RuntimeError -- the thread is already exiting so the wake is moot.
        # Suppress it (mirrors _abort_start's pattern) instead of letting it
        # skip the cleanup + STOPPED transition below and strand the runtime in
        # STOPPING (which the guard above would then never let another stop()
        # retry, and the STOPPED start-guard would never let a start() proceed).
        if self._loop is not None and self._stop_event is not None:
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._stop_event.set)

        try:
            if self._thread is not None:
                # The thread join shares the SINGLE stop() budget with the
                # buffer close below (join gets the remaining budget, not the full
                # timeout), so the two BLOCKING waits together cannot exceed the
                # caller-supplied deadline.
                join_budget = max(0.0, timeout - (_time.monotonic() - deadline_started))
                self._thread.join(timeout=join_budget)

            if self._buffer is not None:
                # RM6 +: bound the close's LOCK acquisition by the
                # REMAINING stop() budget (not a fixed 2s). A daemon orphan-scan
                # wedged on SQLite I/O cannot hang stop() past the budget: on
                # timeout close() abandons the handle; when uncontended it acquires
                # immediately. (The final uncontended clean-close ops run to
                # completion, bounded by buffer size / disk.)
                remaining = max(0.0, timeout - (_time.monotonic() - deadline_started))
                self._buffer.close(lock_timeout=remaining)
                self._buffer = None

            # Best-effort pidfile cleanup so a stale entry doesn't
            # confuse the next ``z4j-<adapter> restart``.
            try:
                from z4j_bare.control import remove_pidfile

                remove_pidfile(self.framework.name if self.framework else "bare")
            except Exception:  # noqa: S110  best-effort pidfile cleanup on stop
                pass
        finally:
            # STOPPED must ALWAYS be published, even if a teardown step
            # above raises -- otherwise the runtime is stranded in STOPPING (no
            # later stop() retries, no start() proceeds) permanently.
            with self._state_lock:
                self._state = RuntimeState.STOPPED

        logger.info("z4j agent runtime stopped")

    def record_event(self, event: Event) -> None:
        """Append an event to the outbound buffer.

        Intended to be called from an engine's hot-path callback
        (Celery signal handler, RQ Job callback, Dramatiq middleware
        method) or another host-app hot path. The write is local and bounded,
        but synchronous: it takes the buffer lock and writes SQLite. Wraps the
        write in :func:`safe_call` so a buffer error never propagates into the
        host code.

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
            # unstored entry. 35 chars, within the 64-char id cap.
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
            self._loop_error.set()
            self._loop_ready.set()  # unblock the caller's wait()
            return

        # _loop_ready is now published by _main() only once it reaches
        # STEADY STATE (consumer tasks created, signals wired -- just before
        # _supervise), NOT here. So start() never reports RUNNING on a loop that
        # dies during setup (an engine connect_signals raising, a short-secret
        # early return, etc.). If _main returns/raises before reaching that
        # point, the finally below flags the error and unblocks the waiter.
        try:
            loop.run_until_complete(self._main())
        except Exception:
            logger.exception("z4j agent runtime loop crashed")
            self._loop_error.set()
        finally:
            if not self._loop_ready.is_set():
                # _main never reached steady state; the start() waiter is still
                # blocked. Flag the failure and release it fast.
                self._loop_error.set()
                self._loop_ready.set()
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
            # Clear the shared handle ONLY if it still points at OUR loop.
            # A slow teardown can outlive start()'s join timeout; if a retry
            # start() has since spawned a new thread and published its own loop
            # into self._loop, an unconditional clear here would null the NEW
            # owner's live loop. The identity guard clears only what this thread
            # still owns.
            if self._loop is loop:
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
            activate_schedule_stream=self.activate_external_schedule_stream,
            control_external_schedule=self.control_external_schedule,
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

            # RH8: refresh the buffer's liveness lease unconditionally (even
            # with the heartbeat disabled and no events flowing) so an idle
            # owner is never mistaken for dead by a peer's orphan-adoption. The
            # append path already refreshes the lease when events/heartbeats
            # flow; this covers the fully-idle case.
            if self._buffer is not None:
                engine_consumer_tasks.append(
                    asyncio.create_task(
                        self._periodic_lease_refresh(self._buffer),
                        name="z4j-buffer-lease-refresh",
                    ),
                )
                # C fresh-first + RH8: recover only after the fresh sink and
                # runtime are ready. The task scans immediately and then on a
                # cadence, so old-buffer classification is recovery rather than
                # a startup precondition, while a still-fresh dead-owner lease is
                # reconsidered once it ages out.
                engine_consumer_tasks.append(
                    asyncio.create_task(
                        self._periodic_orphan_adoption(self._buffer),
                        name="z4j-buffer-orphan-rescan",
                    ),
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

            # Publish STEADY-STATE readiness only here -- the loop is
            # built, signals are wired, and every consumer task is created. Only
            # now may start() report RUNNING; a failure anywhere above leaves
            # _loop_ready unset so _run_loop's finally flags _loop_error.
            self._loop_ready.set()

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
    ) -> dict[str, object] | None:
        """Capture and durably sequence one complete external snapshot.

        Unsequenced N-1 schedule events are intentionally not emitted.  The
        Brain first supplies an exact stream epoch via
        ``schedule.external.activate``; every later observation for that
        adapter is serialized by one asyncio lock and committed to the local
        SQLite buffer together with its source sequence.
        """
        scheduler_name = getattr(scheduler, "name", "unknown")
        authority = getattr(self, "_external_schedule_authorities", {}).get(
            scheduler_name,
        )
        if authority is None:
            logger.debug(
                "z4j agent: scheduler %s snapshot deferred until the Brain "
                "issues external stream authority (reason=%s)",
                scheduler_name,
                reason,
            )
            return None
        locks = getattr(self, "_external_schedule_locks", None)
        if locks is None:
            locks = {}
            self._external_schedule_locks = locks
        lock = locks.setdefault(scheduler_name, asyncio.Lock())
        async with lock:
            return await self._emit_schedule_snapshot_locked(
                scheduler,
                authority=authority,
                reason=reason,
            )

    async def _emit_schedule_snapshot_locked(  # noqa: PLR0915
        self,
        scheduler: SchedulerAdapter,
        *,
        authority: _ExternalScheduleAuthority,
        reason: str,
    ) -> dict[str, object] | None:
        """Observe then atomically reserve+append while the adapter lock is held."""
        import secrets as _secrets
        from uuid import uuid4

        from z4j_core.schedule_external import (
            canonical_external_json,
            external_projection_body,
            external_projection_digest,
            external_snapshot_frame_body,
            external_snapshot_frame_digest,
        )

        scheduler_name = getattr(scheduler, "name", "unknown")
        try:
            schedules = await scheduler.list_schedules()
        except Exception:
            logger.exception(
                "z4j agent: scheduler %s list_schedules failed (reason=%s)",
                scheduler_name,
                reason,
            )
            return None

        observed_at = datetime.now(UTC)
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
                # M13: a schedule that fails to serialize must NOT be silently
                # DROPPED from this snapshot. The snapshot is AUTHORITATIVE --
                # the brain reconciler deletes any schedule absent from it -- so
                # a partial inventory that omits one un-dumpable job reads as a
                # deletion of a job that still exists (e.g. args=[b"\xff"] maps
                # cleanly in the adapter but only blows up here at JSON time).
                # Abort the whole snapshot, the same fail-safe as a
                # list_schedules error above: skip this cycle so NOTHING is
                # deleted, and let the next resync retry. Adapters are expected
                # to keep args/kwargs JSON-safe (see the apscheduler adapter's
                # degraded mapping); reaching here means one slipped through.
                logger.exception(
                    "z4j agent: scheduler %s yielded a Schedule that failed "
                    "model_dump; skipping this snapshot to avoid a false deletion",
                    scheduler_name,
                )
                return None

        buffer = self._buffer
        if buffer is None or buffer.closed:
            return None
        snapshot_id = str(uuid4())

        def _build_payloads(
            sequence: int,
            adapter_instance_id: str,
        ) -> list[bytes]:
            projection_body = external_projection_body(
                stream_id=authority.stream_id,
                epoch_uuid=authority.epoch_uuid,
                epoch_number=authority.epoch_number,
                sequence=sequence,
                kind="snapshot",
                owner=authority.owner,
                source_scope=authority.source_scope,
                adapter_instance_id=adapter_instance_id,
                schedules=schedules_payload,
                complete=True,
                stable_source=authority.stable_source,
            )
            snapshot_digest = external_projection_digest(projection_body)
            normalized_rows = projection_body["schedules"]
            chunks: list[list[dict[str, Any]]] = []
            current: list[dict[str, Any]] = []
            for row in normalized_rows:
                candidate = [*current, row]
                candidate_bytes = len(
                    canonical_external_json({"schedules": candidate}),
                )
                if current and candidate_bytes > _EXTERNAL_SNAPSHOT_ROW_PAYLOAD_BYTES:
                    chunks.append(current)
                    current = [row]
                else:
                    current = candidate
                if (
                    len(
                        canonical_external_json({"schedules": current}),
                    )
                    > _EXTERNAL_SNAPSHOT_ROW_PAYLOAD_BYTES
                ):
                    raise ValueError(
                        "one external schedule row exceeds the stable snapshot frame limit",
                    )
            if current:
                chunks.append(current)

            frame_count = len(chunks)
            payloads: list[bytes] = []

            def _serialize_snapshot_frame(
                *,
                frame_kind: str,
                frame_index: int,
                rows: list[dict[str, Any]],
            ) -> bytes:
                frame_body = external_snapshot_frame_body(
                    stream_id=authority.stream_id,
                    epoch_uuid=authority.epoch_uuid,
                    epoch_number=authority.epoch_number,
                    sequence=sequence,
                    owner=authority.owner,
                    source_scope=authority.source_scope,
                    adapter_instance_id=adapter_instance_id,
                    snapshot_id=snapshot_id,
                    frame_kind=frame_kind,
                    frame_index=frame_index,
                    frame_count=frame_count,
                    row_count=len(normalized_rows),
                    snapshot_digest=snapshot_digest,
                    stable_source=authority.stable_source,
                    schedules=rows,
                )
                frame = EventBatchFrame(
                    id=f"ev_{_secrets.token_hex(16)}",
                    ts=observed_at,
                    payload=EventBatchPayload(
                        events=[
                            {
                                "id": str(uuid4()),
                                "kind": "schedule.snapshot",
                                "engine": scheduler_name,
                                "task_id": "",
                                "occurred_at": observed_at.isoformat(),
                                "data": {
                                    "external_snapshot_frame": frame_body,
                                    "frame_digest": (
                                        external_snapshot_frame_digest(
                                            frame_body,
                                        )
                                    ),
                                    "reason": reason,
                                },
                            },
                        ],
                    ),
                )
                serialized = serialize_frame(frame)
                if len(serialized) > _EXTERNAL_SNAPSHOT_FRAME_TARGET_BYTES:
                    raise ValueError(
                        "external stable snapshot framing exceeded its serialized frame limit",
                    )
                return serialized

            for index, chunk in enumerate(chunks):
                payloads.append(
                    _serialize_snapshot_frame(
                        frame_kind="rows",
                        frame_index=index,
                        rows=chunk,
                    ),
                )
            payloads.append(
                _serialize_snapshot_frame(
                    frame_kind="terminal",
                    frame_index=frame_count,
                    rows=[],
                ),
            )
            return payloads

        try:
            entry_ids, sequence, adapter_instance_id = (
                buffer.append_external_schedule_projection_frames(
                    owner=authority.owner,
                    source_scope=authority.source_scope,
                    stream_id=authority.stream_id,
                    epoch_uuid=authority.epoch_uuid,
                    epoch_number=authority.epoch_number,
                    adapter_instance_id=authority.adapter_instance_id,
                    build_payloads=_build_payloads,
                )
            )
        except Exception:
            logger.exception(
                "z4j agent: scheduler %s failed to reserve and buffer its "
                "external projection (reason=%s)",
                scheduler_name,
                reason,
            )
            return None
        logger.info(
            "z4j agent: scheduler %s external snapshot buffered "
            "(count=%d, frames=%d, sequence=%d, reason=%s)",
            scheduler_name,
            len(schedules_payload),
            len(entry_ids),
            sequence,
            reason,
        )
        return {
            "scheduler": scheduler_name,
            "stream_id": authority.stream_id,
            "epoch_uuid": authority.epoch_uuid,
            "epoch_number": authority.epoch_number,
            "sequence": sequence,
            "adapter_instance_id": adapter_instance_id,
            "schedule_count": len(schedules_payload),
            "frame_count": len(entry_ids),
        }

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

    async def _periodic_lease_refresh(self, buffer: BufferStore) -> None:
        """RH8: keep this owner's buffer liveness lease fresh so a peer never
        adopts a LIVE-but-idle buffer. Runs until stop; a lease write must never
        take down the runtime, so failures are swallowed."""
        from z4j_bare.buffer import _LEASE_REFRESH_SECONDS

        while not self._stop_event.is_set():
            with contextlib.suppress(Exception):
                buffer.touch_lease()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_LEASE_REFRESH_SECONDS,
                )
                return  # stop signalled
            except TimeoutError:
                pass  # normal tick

    async def _periodic_orphan_adoption(self, buffer: BufferStore) -> None:
        """Scan for recoverable buffers immediately, then periodically.

        C fresh-first: this task is created before readiness is published but
        cannot run until ``_main`` yields after setting ``_loop_ready``. Old
        buffer classification is therefore never a startup precondition. RH8:
        cadence scans reconsider a dead owner's initially fresh lease after it
        ages out. Best-effort; never raises.
        """
        from z4j_bare.buffer import _ORPHAN_RESCAN_SECONDS, adopt_orphaned_buffers

        while not self._stop_event.is_set():
            try:
                # The scan opens + drains orphan SQLite DBs, which
                # can be slow on a large/locked orphan set. Run it OFF the event
                # loop so it never blocks sends, acks, shutdown, or this process's
                # lease refresh. Writes into `buffer` are serialised by its
                # threading.Lock, so the cross-thread append is safe.
                # M5: a DAEMON thread (not asyncio.to_thread's pooled non-daemon
                # worker), so a genuinely-wedged scan on a locked orphan DB is
                # never atexit-joined and can never hang process exit past the
                # bounded shutdown drain.
                adopted = await _await_in_daemon_thread(
                    lambda: adopt_orphaned_buffers(buffer, home_dir=buffer.path.parent)
                )
                if adopted:
                    logger.info(
                        "z4j agent: recovered %d buffered event(s) from a dead "
                        "peer on a periodic re-scan",
                        adopted,
                    )
            except Exception:
                logger.warning(
                    "z4j agent: periodic orphaned-buffer adoption failed",
                    exc_info=True,
                )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_ORPHAN_RESCAN_SECONDS,
                )
                return  # stop signalled
            except TimeoutError:
                pass  # normal cadence tick -- re-scan above

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

    async def activate_external_schedule_stream(
        self,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> dict[str, object]:
        """Accept one Brain-issued epoch and publish its activation snapshot."""
        scheduler_name = str(
            target.get("scheduler") or parameters.get("scheduler") or parameters.get("owner") or ""
        ).strip()
        scheduler = self.schedulers.get(scheduler_name)
        if scheduler is None:
            raise ValueError(f"no scheduler adapter registered for {scheduler_name or '<missing>'}")
        connected = getattr(self, "_connected_schedulers_ref", ())
        if scheduler not in connected:
            raise RuntimeError(f"scheduler adapter {scheduler_name} is not connected")

        owner = str(parameters.get("owner") or "").strip()
        source_scope = str(parameters.get("source_scope") or "").strip()
        stream_id = str(parameters.get("stream_id") or "").strip()
        epoch_uuid = str(parameters.get("epoch_uuid") or "").strip()
        adapter_instance_id = str(parameters.get("adapter_instance_id") or "").strip()
        try:
            epoch_number = int(parameters.get("epoch_number"))
        except (TypeError, ValueError) as exc:
            raise ValueError("external activation epoch_number is invalid") from exc
        if (
            owner != scheduler_name
            or not source_scope
            or not stream_id
            or not epoch_uuid
            or not adapter_instance_id
            or epoch_number <= 0
        ):
            raise ValueError("external activation does not bind the exact scheduler stream epoch")
        # This flag is a signed Brain command obligation, not agent-supplied
        # evidence.  The Brain may set it only after its activation policy has
        # established a source-native stable read or operator quiescence.
        if parameters.get("stable_source") is not True:
            raise ValueError(
                "external activation requires Brain-authorized stable source observation"
            )

        authority = _ExternalScheduleAuthority(
            stream_id=stream_id,
            epoch_uuid=epoch_uuid,
            epoch_number=epoch_number,
            adapter_instance_id=adapter_instance_id,
            owner=owner,
            source_scope=source_scope,
            stable_source=True,
        )
        authorities = getattr(self, "_external_schedule_authorities", None)
        if authorities is None:
            authorities = {}
            self._external_schedule_authorities = authorities
        locks = getattr(self, "_external_schedule_locks", None)
        if locks is None:
            locks = {}
            self._external_schedule_locks = locks
        lock = locks.setdefault(scheduler_name, asyncio.Lock())
        async with lock:
            existing = authorities.get(scheduler_name)
            if existing is not None and existing != authority:
                raise RuntimeError(
                    "scheduler adapter already holds a different external stream epoch"
                )
            authorities[scheduler_name] = authority
            result = await self._emit_schedule_snapshot_locked(
                scheduler,
                authority=authority,
                reason="activation",
            )
            if result is None:
                if existing is None:
                    authorities.pop(scheduler_name, None)
                raise RuntimeError("external activation snapshot was not durably buffered")
            return result

    async def control_external_schedule(  # noqa: PLR0912, PLR0915
        self,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> dict[str, object]:
        """Execute one exact set-to-state operation under its reserved sequence."""

        import secrets as _secrets
        from uuid import UUID, uuid4

        from z4j_core.schedule_external import (
            canonical_external_json,
            external_control_result_matches_desired,
            external_projection_body,
            external_projection_digest,
            normalize_external_schedule,
        )

        required_fields = {
            "operation_id",
            "scheduler",
            "schedule_id",
            "source_key",
            "z4j_schedule_id",
            "stream_id",
            "epoch_uuid",
            "epoch_number",
            "adapter_instance_id",
            "expected_accepted_sequence",
            "expected_projection_digest",
            "desired_projection",
            "desired_projection_digest",
            "registry_owner_id",
            "session_generation",
        }
        if set(parameters) != required_fields:
            raise ValueError("external control fields are not the closed protocol vocabulary")
        try:
            operation_id = str(UUID(str(parameters["operation_id"])))
            stream_id = str(UUID(str(parameters["stream_id"])))
            epoch_uuid = str(UUID(str(parameters["epoch_uuid"])))
            UUID(str(parameters["z4j_schedule_id"]))
            UUID(str(parameters["registry_owner_id"]))
        except (TypeError, ValueError) as exc:
            raise ValueError("external control UUID is invalid") from exc
        scheduler_name = str(parameters["scheduler"]).strip()
        source_key = str(parameters["source_key"]).strip()
        if not scheduler_name or str(parameters["schedule_id"]) != source_key or not source_key:
            raise ValueError("external control source identity is invalid")
        scheduler = self.schedulers.get(scheduler_name)
        if scheduler is None:
            raise ValueError(f"no scheduler adapter registered for {scheduler_name!r}")
        connected = getattr(self, "_connected_schedulers_ref", ())
        if scheduler not in connected:
            raise RuntimeError(f"scheduler adapter {scheduler_name} is not connected")
        try:
            epoch_number = int(parameters["epoch_number"])
            expected_sequence = int(
                parameters["expected_accepted_sequence"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("external control epoch or sequence is invalid") from exc
        if (
            isinstance(parameters["epoch_number"], bool)
            or isinstance(parameters["expected_accepted_sequence"], bool)
            or epoch_number <= 0
            or expected_sequence < 0
        ):
            raise ValueError("external control epoch or sequence is invalid")
        adapter_instance_id = str(
            parameters["adapter_instance_id"],
        ).strip()
        expected_projection_digest = str(
            parameters["expected_projection_digest"],
        )
        desired_projection_digest = str(
            parameters["desired_projection_digest"],
        )
        desired_raw = parameters["desired_projection"]
        if not isinstance(desired_raw, dict):
            raise TypeError("external control desired projection is invalid")
        desired_projection = normalize_external_schedule(
            desired_raw,
            owner=scheduler_name,
        )
        if (
            desired_projection["source_key"] != source_key
            or hashlib.sha256(
                canonical_external_json(desired_projection),
            ).hexdigest()
            != desired_projection_digest
            or len(expected_projection_digest) != 64
            or any(character not in "0123456789abcdef" for character in expected_projection_digest)
        ):
            raise ValueError("external control desired or prior projection digest mismatched")

        authority = getattr(
            self,
            "_external_schedule_authorities",
            {},
        ).get(scheduler_name)
        if authority is None or (
            authority.stream_id,
            authority.epoch_uuid,
            authority.epoch_number,
            authority.adapter_instance_id,
            authority.owner,
        ) != (
            stream_id,
            epoch_uuid,
            epoch_number,
            adapter_instance_id,
            scheduler_name,
        ):
            raise RuntimeError("external control does not match this adapter's stream authority")
        buffer = self._buffer
        if buffer is None or buffer.closed:
            raise RuntimeError("external control buffer is unavailable")
        lock = self._external_schedule_locks.setdefault(
            scheduler_name,
            asyncio.Lock(),
        )
        async with lock:
            reservation = buffer.reserve_external_schedule_control(
                operation_id=operation_id,
                owner=authority.owner,
                source_scope=authority.source_scope,
                stream_id=authority.stream_id,
                epoch_uuid=authority.epoch_uuid,
                epoch_number=authority.epoch_number,
                adapter_instance_id=authority.adapter_instance_id,
                expected_sequence=expected_sequence,
                desired_projection_digest=desired_projection_digest,
            )
            if reservation.already_published:
                return {
                    "operation_id": operation_id,
                    "sequence": reservation.sequence,
                    "projection_buffered": True,
                    "deduplicated": True,
                }

            prior = await scheduler.get_schedule(source_key)
            if prior is None:
                raise RuntimeError("external control source schedule is missing")
            prior_dump = prior.model_dump(mode="json")
            prior_projection = normalize_external_schedule(
                prior_dump,
                owner=scheduler_name,
            )
            if prior_projection["source_key"] != source_key:
                raise RuntimeError("external control prior source projection is stale")
            prior_projection_digest = hashlib.sha256(
                canonical_external_json(prior_projection),
            ).hexdigest()
            prior_is_expected = hmac.compare_digest(
                prior_projection_digest,
                expected_projection_digest,
            )
            prior_is_landed_result = external_control_result_matches_desired(
                desired_projection,
                prior_projection,
            )
            if prior_is_expected:
                desired_enabled = bool(
                    desired_projection["is_enabled"],
                )
                adapter_result = (
                    await scheduler.enable_schedule(source_key)
                    if desired_enabled
                    else await scheduler.disable_schedule(source_key)
                )
                if getattr(adapter_result, "status", None) != "success":
                    raise RuntimeError(
                        getattr(adapter_result, "error", None)
                        or "external scheduler rejected the set-to-state operation"
                    )

                observed = await scheduler.get_schedule(source_key)
                if observed is None:
                    raise RuntimeError("external control result schedule is missing")
                observed_projection = normalize_external_schedule(
                    observed.model_dump(mode="json"),
                    owner=scheduler_name,
                )
            elif prior_is_landed_result:
                # A prior attempt may have landed in the native scheduler and
                # crashed before publishing its reserved projection.  The
                # reservation binds this exact operation/sequence; observe and
                # publish the already-landed set-to-state result without
                # invoking the native side effect a second time.
                observed_projection = prior_projection
            else:
                raise RuntimeError("external control prior source projection is stale")
            observed_at = datetime.now(UTC)
            if not external_control_result_matches_desired(
                desired_projection,
                observed_projection,
            ):
                raise RuntimeError(
                    "external control result differs from the allowed desired projection"
                )

            def _build_payload(
                sequence: int,
                reserved_adapter_instance_id: str,
            ) -> bytes:
                body = external_projection_body(
                    stream_id=authority.stream_id,
                    epoch_uuid=authority.epoch_uuid,
                    epoch_number=authority.epoch_number,
                    sequence=sequence,
                    kind="control",
                    owner=authority.owner,
                    source_scope=authority.source_scope,
                    adapter_instance_id=reserved_adapter_instance_id,
                    schedules=[observed_projection],
                    deleted_source_keys=[],
                    complete=False,
                    stable_source=True,
                    operation_id=operation_id,
                )
                frame = EventBatchFrame(
                    id=f"ev_{_secrets.token_hex(16)}",
                    ts=observed_at,
                    payload=EventBatchPayload(
                        events=[
                            {
                                "id": str(uuid4()),
                                "kind": "schedule.updated",
                                "engine": scheduler_name,
                                "task_id": "",
                                "occurred_at": observed_at.isoformat(),
                                "data": {
                                    "external_projection": body,
                                    "payload_digest": (external_projection_digest(body)),
                                    "reason": "external-control",
                                },
                            },
                        ],
                    ),
                )
                return serialize_frame(frame)

            _, sequence, _, replayed = buffer.append_reserved_external_schedule_control(
                operation_id=operation_id,
                owner=authority.owner,
                source_scope=authority.source_scope,
                stream_id=authority.stream_id,
                epoch_uuid=authority.epoch_uuid,
                epoch_number=authority.epoch_number,
                adapter_instance_id=authority.adapter_instance_id,
                expected_sequence=expected_sequence,
                desired_projection_digest=desired_projection_digest,
                build_payload=_build_payload,
            )
            return {
                "operation_id": operation_id,
                "sequence": sequence,
                "projection_buffered": True,
                "deduplicated": replayed,
            }

    def _scheduler_sink(
        self,
        scheduler_name: str,
        action: str,
        schedule: object,
    ) -> None:
        """Turn a native signal into a fresh, sequenced source observation.

        The signal's schedule object is deliberately not serialized later and
        assigned a new sequence: that would re-stamp an old observation.
        Instead it only prompts a new complete ``list_schedules`` observation
        on the runtime loop.  Bursts coalesce per adapter.
        """
        del schedule
        if action not in {"created", "updated", "deleted"}:
            return
        scheduler = self.schedulers.get(scheduler_name)
        loop = self._loop
        if scheduler is None or loop is None or not loop.is_running():
            return

        def _schedule_fresh_observation() -> None:
            pending = getattr(self, "_snapshot_signal_pending", None)
            if pending is None:
                pending = set()
                self._snapshot_signal_pending = pending
            dirty = getattr(self, "_snapshot_signal_dirty", None)
            if dirty is None:
                dirty = set()
                self._snapshot_signal_dirty = dirty
            if scheduler_name in pending:
                # A signal that arrives while list_schedules() is in flight
                # represents a potentially newer source state.  Coalesce the
                # burst, but force one more fresh read after the current one.
                dirty.add(scheduler_name)
                return
            pending.add(scheduler_name)

            async def _run() -> None:
                try:
                    observation_reason = f"signal:{action}"
                    while True:
                        dirty.discard(scheduler_name)
                        await self._emit_schedule_snapshot(
                            scheduler,
                            reason=observation_reason,
                        )
                        if scheduler_name not in dirty:
                            break
                        observation_reason = "signal:coalesced"
                finally:
                    pending.discard(scheduler_name)
                    dirty.discard(scheduler_name)

            task = asyncio.create_task(
                _run(),
                name=f"z4j-schedule-observation-{scheduler_name}",
            )
            tasks = getattr(self, "_schedule_observation_tasks", None)
            if tasks is None:
                tasks = set()
                self._schedule_observation_tasks = tasks
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        loop.call_soon_threadsafe(_schedule_fresh_observation)

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
        # Streaks and per-class delays are instance state initialised
        # in __init__ and reset on successful handshake by
        # _connect_and_run. We re-initialise them here as well in case
        # _supervise is invoked more than once over the runtime's life
        # (it currently is not, but the contract should not depend on
        # call-count).
        self._reset_reconnect_state()
        stop_loop = False
        while not stop_loop and not self._stop_event.is_set():
            error_class: str | None = None
            err: BaseException | None = None

            def _classify(candidate: str, group: BaseExceptionGroup) -> None:
                """Record a failure class, keeping the most consequential one.

                ``except*`` is not ``except``: PEP 654 runs EVERY arm whose
                type appears in the group, in source order, so a plain
                assignment lets the LAST matching arm win regardless of which
                failure actually matters. That is not the subclass case, which
                ``except*`` splits correctly; it is the sibling case, where two
                tasks in the group fail differently.

                It happens on the path this ordering exists to protect. When
                the brain answers with a fatal error frame the receive task
                raises AgentIncompatibleError, and the transport clears its
                socket reference before awaiting the close, so a send in that
                window raises ConnectionError alongside it. Assigned in order,
                the cycle was binned as "connection" and retried on the 1s
                schedule, which is the reconnect storm the incompatible class
                was added to stop.

                Ranked instead, so the answer does not depend on which sibling
                happened to fail. A connection error raised while tearing down
                an incompatible session is a consequence of it, and
                reconnecting in a second does not make the version match.
                """
                nonlocal error_class, err
                current = (
                    (error_class, err) if error_class is not None and err is not None else None
                )
                error_class, err = _prefer_supervisor_failure(
                    current,
                    candidate,
                    group,
                )

            try:
                await self._connect_and_run()
            except* _StopRequested:
                # Watchdog cancelled the group on stop_event - clean exit.
                # PEP 654 forbids ``return`` inside an ``except*`` block,
                # so we set a flag and break out at the next loop guard.
                stop_loop = True
            except* AuthenticationError as eg:
                _classify("auth", eg)
            except* AgentIncompatibleError as eg:
                _classify("incompatible", eg)
            except* ProtocolError as eg:
                _classify("protocol", eg)
            except* ConnectionError as eg:
                _classify("connection", eg)
            except* Exception as eg:
                # Unknown failure class. Treat as connection-class
                # for backoff purposes; log full traceback so future
                # categorisation is possible. Never fatal.
                logger.exception(
                    "z4j agent unexpected supervisor error",
                    exc_info=eg,
                )
                _classify("connection", eg)

            if stop_loop:
                break

            # Count the failure and report it through the same key that
            # classified it. Splitting the increment across the ``except*``
            # arms and the lookup across a separate branch is what let the
            # two disagree, and a disagreement here is silent: the tiered
            # logger just sees the wrong number and picks the wrong tier.
            if err is not None and error_class is not None:
                self._failure_streaks[error_class] += 1
                _log_disconnect(error_class, err, self._failure_streaks[error_class])

            if self._stop_event.is_set():
                return

            # Pick the schedule for the most recent error class, by the same
            # key the streak above was counted under.
            retry_class = error_class or _DEFAULT_ERROR_CLASS
            schedule = _RECONNECT_SCHEDULES[retry_class]
            base = self._delays[retry_class]

            sleep_for = base + random.uniform(0, base * schedule.jitter)  # noqa: S311  non-security reconnect jitter
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
            self._delays[retry_class] = min(base * 2.0, schedule.maximum)

    async def _connect_and_run(self) -> None:
        """One supervisor cycle: connect, run tasks, until disconnect."""
        assert self._transport is not None
        assert self._stop_event is not None
        assert self._dispatcher is not None
        # NB: self._heartbeat may be None in heartbeat-less mode
        # (Z4J_HEARTBEAT=0); guarded at each use below.

        await self._transport.connect()

        # Connection established. Reset failure streaks AND the
        # per-class backoff delays so the next disconnect starts at
        # the floor again. This is the only reachable reset point in
        # the supervisor lifecycle: the supervise() task-group always
        # exits via _StopRequested, never by clean return, so a reset
        # placed there would never fire. Without this reset a runtime
        # that flapped once on startup and then stabilised for hours
        # would still be pinned at the 30s/60s/600s cap on its next
        # disconnect, which is worse than starting at 1s/1s/10s.
        prior_failures = sum(self._failure_streaks.values())
        if prior_failures > 0:
            logger.info(
                "z4j agent recovered after %d failed connect attempt(s)",
                prior_failures,
            )
        self._reset_reconnect_state()
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
        # no longer applies. If the new brain still 413s, it
        # shrinks again.
        self._send_batch_size = _SEND_BATCH_SIZE
        # Reset the retryable-outcome backoff too, so a backoff grown on a
        # dying session does not throttle the first sends of a fresh one.
        self._send_backoff = _SEND_BACKOFF_INITIAL
        # Fresh session -> the persistent-retryable reconnect counter starts
        # over.
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
        starving control frames or losing any event.
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
            # behind a full in-flight window. The excluded set is
            # the buffer-entry ids currently in ``_pending_acks``.
            in_flight = {entry_id for entry_id, _sent_at in self._pending_acks.values()}
            # Backpressure: STRICTLY cap concurrent un-acked event_batch
            # frames. When at the cap, keep draining CONTROL frames
            # (command acks/results confirm on send and never become
            # pending, so they must not be starved) but exclude
            # event_batch so ``_pending_acks`` never grows past the cap
            # Below the cap, drain everything.
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
                # pending could register 500 more and overshoot to 755.
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
                retained_causal_frame = self._handle_undeliverable_drop(
                    buffer,
                    entries,
                    exc,
                )
                if retained_causal_frame:
                    # Do not spin on an impossible local send.  Reconnect so a
                    # changed Brain-advertised frame cap can be negotiated,
                    # while preserving the causal entry losslessly.
                    raise ConnectionError(
                        "external schedule projection is locally "
                        "undeliverable; retained pending Brain acknowledgement",
                    ) from exc
                continue
            except (PartialSendError, ConnectionError):
                # TRANSPORT failure (socket dropped mid-batch, connection
                # error, retryable HTTP status like 3xx/5xx/429). The
                # batch was not delivered. We do NOT confirm anything, and
                # we do NOT increment any drop counter: a flaky connection
                # is not the batch's fault. Re-raise so the
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
                # budget). Blocking here is harmless: during a
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
                # persistently fails, after a bounded budget. Use a
                # SMALL FIXED delay (not the growing transient backoff) so an
                # isolated poison frame at the buffer HEAD is isolated and
                # dropped within seconds and does not starve control frames
                # queued behind it for minutes (head-of-line fix).
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
            # withheld an ack for a transient DB skip. On WS,
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
        await (recorded in ``_acks_seen_early``), in which case confirm
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
                # already passed.
                self._acks_seen_early.discard(frame_id)
                confirm_now.append(entry.id)
            elif frame_id not in self._pending_acks:
                # First send of this frame. Record sent_at ONCE. The
                # in-flight drain filter means we should not re-send a
                # pending frame, but guard the overwrite anyway so a
                # re-send can never slide the watchdog deadline (that was
                # the mechanism by which the deadline was never reached,
                # ).
                self._pending_acks[frame_id] = (entry.id, now)
        return confirm_now

    def _handle_undeliverable_drop(
        self,
        buffer: BufferStore,
        entries: list,
        exc: UndeliverableFrameError,
    ) -> bool:
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
        protected = [
            entries[i] for i in exc.drop_indices if entries[i].kind == EXTERNAL_SCHEDULE_ENTRY_KIND
        ]
        purge = confirm_now + [
            entries[i].id
            for i in exc.drop_indices
            if entries[i].kind != EXTERNAL_SCHEDULE_ENTRY_KIND
        ]
        if purge:
            buffer.confirm(purge)
        if protected:
            logger.critical(
                "retaining %d locally-undeliverable external schedule "
                "projection frame(s); causal entries require a Brain ack",
                len(protected),
            )
        if self._heartbeat is not None and exc.accepted:
            self._heartbeat.record_flush(now)
        return bool(protected)

    async def _backoff_retry(self) -> None:
        """Sleep the current TRANSIENT-retry backoff, then grow it.

        Called ONLY on a long-poll transient partial store
        (``UploadRetryableError``). The delay doubles per consecutive
        failure up to ``_SEND_BACKOFF_MAX`` (5s) so a struggling brain is
        not hammered while it recovers; a successful send
        resets it via :meth:`_reset_send_backoff`. The CONTENT-reject path
        does NOT use this -- it uses a small fixed ``_CONTENT_REJECT_DELAY``
        so an isolated poison frame is dropped fast instead of starving
        frames behind it.
        """
        await asyncio.sleep(self._send_backoff)
        self._send_backoff = min(self._send_backoff * 2, _SEND_BACKOFF_MAX)

    def _reset_send_backoff(self) -> None:
        """Reset the retryable-outcome backoff + progress counter after a
        successful send.

        The backoff and the ``_consecutive_retryable`` reconnect counter are
        reset (a successful send is confirmed forward progress). The
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
          while valid siblings keep delivering. Nothing is
          dropped here.
        * **Single-frame batch** -- the culprit is isolated. An
          ``event_batch`` frame gets its attempt counter bumped and is
          dropped ONLY once it crosses ``_MAX_SEND_ATTEMPTS`` (bounded,
          logged). A control frame (command_result / command_ack /
          agent_status / heartbeat) cannot be re-batched or bisected and
          would pin the queue forever, so it is dropped immediately with
          a warning -- losing it merely times the command out server
          side, which is recoverable, whereas pinning the queue loses
          everything behind it.

        Once the isolated offender is actually DROPPED, ``_send_batch_size``
        is restored to the full ``_SEND_BATCH_SIZE``: the shrink only
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
        # content-reject budget (not the shared ``attempts`` metric)
        # and drop only once THAT budget is exhausted.
        entry = entries[0]
        if entry.kind in {
            "event_batch",
            EXTERNAL_SCHEDULE_ENTRY_KIND,
        }:
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
                # buffer stops dribbling one frame per POST.
                self._send_batch_size = _SEND_BATCH_SIZE
            elif entry.kind == EXTERNAL_SCHEDULE_ENTRY_KIND:
                logger.critical(
                    "retaining content-rejected external schedule projection "
                    "entry %d; causal entries require a Brain ack",
                    entry.id,
                )
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
        # Offender gone -- restore full width.
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
            # bounded only by the buffer size.
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
            # Bounded: in normal operation this set is ~empty.
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
        capabilities = _advertised_capabilities(self.engines, self.schedulers)

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


def _prefer_supervisor_failure(
    current: tuple[str, BaseException] | None,
    candidate: str,
    group: BaseExceptionGroup[BaseException],
) -> tuple[str, BaseException]:
    """Merge one ``except*`` match into the supervisor classification.

    Every matching ``except*`` arm runs, so source order must not choose the
    reconnect schedule. The highest-ranked failure wins and retains one leaf
    from the corresponding exception group for logging. Unknown class names
    fail closed with ``KeyError`` instead of silently borrowing a schedule.
    """
    candidate_rank = _SUPERVISOR_FAILURE_RANK[candidate]
    if current is None or candidate_rank > _SUPERVISOR_FAILURE_RANK[current[0]]:
        return candidate, _first(group)
    return current


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
    error_class: str,
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
        error_class: A key of :data:`_RECONNECT_SCHEDULES`. The supervisor
            only calls this once it has classified a failure, so there is
            no "no error" case to encode here.
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
        elif error_class == "incompatible":
            # ERROR, not WARNING: nothing this agent does will clear it, and
            # the operator needs to see the version they have to change.
            logger.error(
                "z4j agent rejected as incompatible: %s. Reconnecting cannot "
                "fix this; upgrade the agent or the brain. Retrying hourly in "
                "case the deployment is corrected.",
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
