"""Command dispatcher.

When an inbound ``command`` frame arrives from the brain, the
dispatcher:

1. Looks up the target engine adapter by name. Envelope HMAC +
   replay-guard verification has already happened one layer up in
   :class:`~z4j_bare.transport.websocket.WebSocketTransport` (via
   :class:`~z4j_core.transport.framing.FrameVerifier`), so anything
   that reaches the dispatcher is already authenticated.
2. Translates the structured ``action`` into a method call on the
   adapter's :class:`z4j_core.protocols.QueueEngineAdapter` or
   :class:`z4j_core.protocols.SchedulerAdapter`.
3. Captures the result into a ``command_result`` frame and queues
   it on the outbound buffer.

The dispatcher never raises into the transport loop - every error is
caught and converted into a failed :class:`CommandResult`.
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from z4j_core.errors import Z4JError
from z4j_core.models import CommandResult
from z4j_core.protocols import QueueEngineAdapter, SchedulerAdapter
from z4j_core.transport.frames import (
    CommandAckFrame,
    CommandAckPayload,
    CommandFrame,
    CommandResultFrame,
    CommandResultPayload,
    serialize_frame,
)

from z4j_bare.orchestrator_detect import detect_orchestrator

if TYPE_CHECKING:
    from z4j_bare.buffer import BufferStore

logger = logging.getLogger("z4j.runtime.dispatcher")

#: Hard ceiling on the number of tasks a single ``bulk_retry`` command
#: may touch. Even if the brain (or a forged command that slipped past
#: HMAC verification somehow) requests more, we clamp here. Prevents
#: a single command from queueing millions of retries and DoS-ing the
#: customer's broker.
BULK_RETRY_HARD_MAX: int = 10_000


class CommandDispatcher:
    """Routes inbound command frames to the correct adapter.

    Constructed once per agent runtime. ``handle(frame)`` is called
    from the transport receive loop - it performs HMAC verification,
    action routing, and result capture.

    Attributes:
        engines: Map of engine name to :class:`QueueEngineAdapter`.
        schedulers: Map of scheduler name to :class:`SchedulerAdapter`.
        buffer: Outbound buffer that holds ``command_ack`` and
                ``command_result`` frames until the transport drains
                them back to the brain.
    """

    #: TTL for deduplication entries (seconds). Commands older than
    #: this are evicted from the seen set. v2's :class:`ReplayGuard`
    #: already rejects replayed seq values, so this cache only
    #: defends against the benign case of the brain re-issuing a
    #: command frame with a fresh envelope after an ack timeout.
    _DEDUP_TTL: float = 300.0
    #: Max entries in the dedup cache.
    _DEDUP_MAX: int = 10_000
    #: Fire actions carry a stable ``fire_id`` that identifies the LOGICAL
    #: fire across re-dispatch. The universal dedup keys these by
    #: ``fire:{fire_id}`` so the SAME fire re-dispatched under a NEW command_id
    #: (after recovery/reroute) is still recognised as a duplicate -- the plain
    #: command_id key misses that. All other commands key by
    #: ``cmd:{command_id}``. The prefixes never let two distinct commands
    #: collapse.
    _FIRE_ACTIONS: frozenset[str] = frozenset(
        {"schedule.fire", "schedule.trigger_now", "schedule.trigger_now.via_scheduler"}
    )

    def __init__(
        self,
        *,
        engines: dict[str, QueueEngineAdapter],
        schedulers: dict[str, SchedulerAdapter],
        buffer: BufferStore,
        resync_schedules: Callable[[str], Awaitable[int]] | None = None,
        activate_schedule_stream: Callable[
            [dict[str, Any], dict[str, Any]],
            Awaitable[dict[str, object]],
        ]
        | None = None,
        control_external_schedule: Callable[
            [dict[str, Any], dict[str, Any]],
            Awaitable[dict[str, object]],
        ]
        | None = None,
    ) -> None:
        self.engines = engines
        self.schedulers = schedulers
        self.buffer = buffer
        # 1.3.3: optional callback the runtime supplies so the
        # ``schedule.resync`` command (Phase C of the snapshot
        # feature) can drive the same drain code that the boot +
        # periodic paths use, without making the dispatcher know
        # about the Runtime class. Returns the number of schedulers
        # drained. May be ``None`` for unit tests / runtime
        # configurations that don't ship the snapshot feature
        # (older z4j-bare versions, hand-built dispatchers, etc.).
        self._resync_schedules = resync_schedules
        self._activate_schedule_stream = activate_schedule_stream
        self._control_external_schedule = control_external_schedule
        # Dedup cache: command_id -> monotonic timestamp of first processing.
        # Prevents duplicate execution if the brain resends a command
        # due to ack timeout.
        #: Keys whose execution SUCCEEDED in this process, newest last.
        self._seen_commands: OrderedDict[str, float] = OrderedDict()
        #: Keys currently executing. Separate from the above so a failure clears
        #: the guard instead of suppressing the retry.
        self._inflight_keys: set[str] = set()
        # Flap-guard anchor: how long has this process been alive?
        # Used by ``_self_exit_restart`` to refuse too-frequent
        # Restarts.
        self._process_start_monotonic = time.monotonic()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def handle(self, frame: CommandFrame) -> None:
        """Execute a single command frame and queue its ack + result.

        Never raises. Any failure becomes a ``status="failed"``
        :class:`CommandResult` with the error message.

        Duplicate commands (same frame.id seen within the TTL) are
        silently dropped after re-sending the ack. This prevents
        double execution when the brain resends after ack timeout.
        """
        # 0. Deduplication check (composite key: fire_id for fires, else
        #    command_id -- see ``_dedup_key``). The ack always uses the wire
        #    command_id, only the dedup identity is composite.
        dedup_key = self._dedup_key(frame)
        tier = self._duplicate_tier(dedup_key)
        # From here on, if _duplicate_tier claimed the key for us, we OWN it
        # and must release it on EVERY exit -- return, raise, or cancellation.
        # Releasing it inline after the result was written left the key retained
        # forever when the task was cancelled mid-await or when writing the ack
        # or the result itself failed, after which every re-delivery was answered
        # as an in-memory duplicate and never ran. Ownership is a scope, not a
        # pair of statements.
        claimed = tier is None
        if tier is not None:
            logger.info("z4j command %s: duplicate (%s), re-acking", frame.id, tier)
            self._queue_ack(
                frame.id,
                delivery_claim_token=frame.payload.delivery_claim_token,
            )
            action = getattr(frame.payload, "action", None)
            if tier == "durable" and action in self._FIRE_ACTIONS:
                # A DURABLE hit proves a PRIOR process already executed
                # this command SUCCESSFULLY -- the durable ledger is written only
                # after success (see below). Terminalize the replacement command
                # with that outcome instead of ack-only, which left it DISPATCHED
                # until the timeout worker retired it as a spurious TIMEOUT even
                # though the work had demonstrably completed. An in-memory-only
                # hit is NOT terminalized: the original may still be in flight in
                # this process and will report its own result.
                #
                # Only for FIRE actions, whose result is purely a status. A
                # generic success is NOT a substitute for a result that carries
                # data the brain projects: ``reconcile_task`` reports an
                # ``engine_state``, and the brain's projection returns early when
                # that is absent, so terminalizing here marked the command
                # complete while the task kept its stale state, with nothing left
                # to re-drive it. For those actions an ack-only duplicate is
                # better: the command stays open and is retired as a timeout, so
                # the brain never records a success it did not receive.
                self._queue_result(
                    frame.id,
                    CommandResult(
                        status="success",
                        result={"deduplicated": True, "dedup_key": dedup_key},
                    ),
                    delivery_claim_token=frame.payload.delivery_claim_token,
                )
            return

        try:
            await self._handle_claimed(frame, dedup_key)
        finally:
            if claimed:
                self._inflight_keys.discard(dedup_key)

    async def _handle_claimed(self, frame: CommandFrame, dedup_key: str) -> None:
        """Execute a command we hold the in-flight claim for.

        Split out so :meth:`handle` can guarantee the claim is released on every
        exit path, including cancellation, without the release being buried in
        the middle of the success path.
        """
        # 1. Immediate ack so the brain knows we saw it
        self._queue_ack(
            frame.id,
            delivery_claim_token=frame.payload.delivery_claim_token,
        )

        # 2. Execute
        try:
            result = await self._execute(frame)
        except Z4JError as exc:
            result = CommandResult(status="failed", error=f"{exc.code}: {exc.message}")
        except Exception as exc:
            logger.exception("z4j unexpected dispatcher error for command %s", frame.id)
            result = CommandResult(status="failed", error=f"internal error: {exc}")

        self._queue_result(
            frame.id,
            result,
            delivery_claim_token=frame.payload.delivery_claim_token,
        )

        # 3.: record the DURABLE dedup key only NOW, and only when the
        #    command actually SUCCEEDED. Recording it before execution (the
        #    previous behaviour) meant a crash between the record and the broker
        #    enqueue left a marker that made the RECOVERY delivery look like a
        #    duplicate -- so it was merely re-acked and never ran, LOSING the
        #    fire. Recording after success keeps the cross-restart guarantee (an
        #    already-executed fire re-delivered to a restarted agent is deduped)
        #    while a crash before the enqueue now correctly re-executes on
        #    recovery. That is the at-least-once direction we actually promise.
        #    The in-memory tier is recorded on the SAME condition, and the
        #    in-flight marker is released either way. Recording it before
        #    execution and never clearing it on failure meant a FAILED fire's
        #    re-delivery was acked and dropped within the TTL.
        if getattr(result, "status", None) == "success":
            self._record_success_key(dedup_key)
            try:
                self.buffer.mark_command_seen(dedup_key, ttl_seconds=self._DEDUP_TTL)
            except Exception:
                logger.debug("z4j dedup key %s: durable record failed", dedup_key)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def _execute(self, frame: CommandFrame) -> CommandResult:
        action = frame.payload.action
        target = frame.payload.target
        parameters = frame.payload.parameters

        # ``schedule.fire`` is a tick from the brain-side
        # z4j-scheduler asking the agent to enqueue a task right
        # now. The brain has already resolved schedule → task in
        # the payload (task_name + args + kwargs + queue + engine),
        # so the agent does NOT need a SchedulerAdapter, this is
        # just a plain enqueue against the QueueEngineAdapter.
        # We must NOT route this through ``_dispatch_scheduler``:
        # ``fire`` is not in the enable/disable/trigger_now/delete
        # switch, and a celery WORKER agent has no scheduler adapter
        # registered (celery-beat is a separate process), so the
        # lookup would return "no scheduler adapter registered for
        # None" and every scheduler tick would land as a
        # ``command.failed`` row.
        if action == "schedule.fire":
            return await self._dispatch_schedule_fire(target, parameters)

        if action == "schedule.external.activate":
            return await self._dispatch_schedule_external_activate(
                target,
                parameters,
            )

        if action == "schedule.external.control":
            return await self._dispatch_schedule_external_control(
                target,
                parameters,
            )

        # 1.3.3: ``schedule.resync`` is the dashboard's *Sync now*
        # button. The brain dispatches one command per online agent;
        # on receipt the agent drains EVERY scheduler adapter it has
        # registered and emits one ``schedule.snapshot`` event per
        # adapter. The runtime supplies the callback that knows how
        # to do the drain, the dispatcher itself doesn't need to
        # touch SchedulerAdapter or the buffer for this path.
        if action == "schedule.resync":
            return await self._dispatch_schedule_resync()

        # Other ``schedule.*`` actions (enable/disable/trigger_now/
        # delete) go to a SchedulerAdapter, those DO require a
        # scheduler-side mutation.
        if action.startswith("schedule."):
            return await self._dispatch_scheduler(action, target, parameters)

        # Everything else goes to a QueueEngineAdapter
        return await self._dispatch_engine(action, target, parameters)

    async def _dispatch_schedule_resync(self) -> CommandResult:
        """Drain every connected scheduler adapter on demand.

        The brain dispatches this when an operator clicks *Sync now*
        on the dashboard's Schedules page. The runtime injected a
        callback at construction time; we call it and return a
        ``CommandResult`` describing how many scheduler adapters
        were drained. The actual snapshot data flows through the
        normal event pipeline as
        :class:`~z4j_core.models.event.EventKind.SCHEDULE_SNAPSHOT`
        events, one per adapter, this command result only reports
        success / count, not the snapshot contents themselves.
        """
        if self._resync_schedules is None:
            return CommandResult(
                status="failed",
                error=(
                    "schedule.resync: this agent build does not "
                    "support on-demand resync (resync callback "
                    "missing). Upgrade z4j-bare to 1.3.1+."
                ),
            )
        try:
            count = await self._resync_schedules("command")
        except Exception as exc:
            logger.exception("z4j dispatcher: schedule.resync failed")
            return CommandResult(
                status="failed",
                error=f"schedule.resync: {type(exc).__name__}: {exc}",
            )
        return CommandResult(
            status="success",
            result={"schedulers_drained": count},
        )

    async def _dispatch_schedule_external_activate(
        self,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> CommandResult:
        """Install a Brain-issued stream epoch and publish sequence one."""
        if self._activate_schedule_stream is None:
            return CommandResult(
                status="failed",
                error=(
                    "schedule.external.activate: this agent build does not "
                    "support the sequenced external schedule protocol"
                ),
            )
        try:
            result = await self._activate_schedule_stream(target, parameters)
        except Exception as exc:
            logger.exception(
                "z4j dispatcher: schedule.external.activate failed",
            )
            return CommandResult(
                status="failed",
                error=(f"schedule.external.activate: {type(exc).__name__}: {exc}"),
            )
        return CommandResult(status="success", result=result)

    async def _dispatch_schedule_external_control(
        self,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> CommandResult:
        """Apply one reserved set-to-state operation and publish its truth."""

        if self._control_external_schedule is None:
            return CommandResult(
                status="failed",
                error=(
                    "schedule.external.control: this agent build does not "
                    "support sequenced external schedule controls"
                ),
            )
        try:
            result = await self._control_external_schedule(
                target,
                parameters,
            )
        except Exception as exc:
            logger.exception(
                "z4j dispatcher: schedule.external.control failed",
            )
            return CommandResult(
                status="failed",
                error=(f"schedule.external.control: {type(exc).__name__}: {exc}"),
            )
        return CommandResult(status="success", result=result)

    async def _dispatch_schedule_fire(
        self,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> CommandResult:
        """Enqueue the task that the brain-side scheduler decided to fire.

        The payload from the brain
        (:class:`z4j_brain.scheduler_grpc.handlers.SchedulerService`)
        carries:

        - ``task_name``  - dotted task name to enqueue
        - ``engine``     - which queue engine adapter to use
                           (e.g. ``"celery"``); falls back to the
                           sole registered engine if unique
        - ``queue``      - optional broker queue / routing key
        - ``args`` / ``kwargs`` - task arguments

        The schedule-id / fire-id metadata is informational only -
        the agent doesn't need them to enqueue.
        """
        task_name = parameters.get("task_name") or target.get("task_name")
        if not task_name:
            return CommandResult(
                status="failed",
                error="schedule.fire: task_name required in payload",
            )
        engine_name = parameters.get("engine") or target.get("engine") or self._single_engine_name()
        adapter = self.engines.get(engine_name) if engine_name else None
        if adapter is None:
            return CommandResult(
                status="failed",
                error=(f"schedule.fire: no engine adapter registered for {engine_name!r}"),
            )
        method = getattr(adapter, "submit_task", None)
        if method is None:
            return CommandResult(
                status="failed",
                error=(f"schedule.fire: engine {adapter.name!r} does not implement submit_task"),
            )
        args = parameters.get("args") or ()
        if isinstance(args, list):
            args = tuple(args)
        kwargs = parameters.get("kwargs") or {}
        return await method(
            task_name,
            args=args,
            kwargs=kwargs,
            queue=parameters.get("queue"),
        )

    async def _dispatch_engine(  # noqa: PLR0911, PLR0912, PLR0915  flat command dispatch
        self,
        action: str,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> CommandResult:
        # RH3: bulk_retry / requeue_dead_letter carry the engine the BRAIN
        # validated the target ids against inside filter["engine"] (the wire
        # frame's target has no engine field). Adapter selection MUST honour it:
        # otherwise a celery-validated batch delivered to an RQ-only agent would
        # requeue through RQ (cross-engine execution), and a multi-engine agent
        # -- with target.engine absent and no single engine to fall back to --
        # would fail with "no adapter". When the filter names an engine it is
        # AUTHORITATIVE: bind to THAT adapter and fail closed if this agent lacks
        # it, never falling back to target.engine or the sole engine.
        filter_engine: str | None = None
        if action in ("bulk_retry", "requeue_dead_letter"):
            filt = parameters.get("filter")
            if isinstance(filt, dict):
                fe = filt.get("engine")
                filter_engine = str(fe) if fe else None
        engine_name = filter_engine or target.get("engine") or self._single_engine_name()
        adapter = self.engines.get(engine_name) if engine_name else None
        if adapter is None:
            detail = (
                f"filter names engine {engine_name!r} but this agent has no such "
                "adapter (refusing to requeue the batch through a different "
                "engine)"
                if filter_engine
                else f"no engine adapter registered for {engine_name!r}"
            )
            return CommandResult(status="failed", error=detail)

        # ``submit_task`` is the universal v1.0+ enqueue primitive.
        # The brain calls it directly (and uses it as a polyfill for
        # retry / bulk_retry / requeue_dlq).
        if action == "submit_task":
            name = parameters.get("name") or target.get("name")
            if not name:
                return CommandResult(
                    status="failed",
                    error="submit_task: name required",
                )
            method = getattr(adapter, "submit_task", None)
            if method is None:
                return CommandResult(
                    status="failed",
                    error=f"adapter {adapter.name!r} does not implement submit_task",
                )
            args = parameters.get("args") or ()
            if isinstance(args, list):
                args = tuple(args)
            kwargs = parameters.get("kwargs") or {}
            return await method(
                name,
                args=args,
                kwargs=kwargs,
                queue=parameters.get("queue"),
                eta=parameters.get("eta"),
                priority=parameters.get("priority"),
            )

        # ``reconcile_task`` bypasses the capability check: every
        # adapter satisfies the Protocol's ``reconcile_task`` method
        # but never lists it in ``capabilities()`` (which gates UI
        # buttons, not background-worker probes).
        if action == "reconcile_task":
            task_id = parameters.get("task_id") or target.get("task_id") or target.get("id")
            if not task_id:
                return CommandResult(
                    status="failed",
                    error="target.task_id required",
                )
            method = getattr(adapter, "reconcile_task", None)
            if method is None:
                return CommandResult(
                    status="success",
                    result={
                        "task_id": task_id,
                        "engine_state": "unknown",
                    },
                )
            return await method(task_id)

        # ``restart_worker`` polyfill: when the engine has no native
        # remote-control channel (every engine except Celery), the
        # agent gracefully self-exits and lets the host orchestrator
        # (docker / k8s / systemd / supervisor) respawn it per its
        # restart policy. Zero shell exec, zero new exec surface;
        # the only new privilege is "kill own process" which is DoS-
        # equivalent to what a hijacked agent could already do via
        # broker-credential abuse.
        if action == "restart_worker" and "restart_worker" not in adapter.capabilities():
            return await self._self_exit_restart(parameters)

        # Brain-side polyfill bridge: if the brain asked for an
        # action the adapter doesn't natively advertise but it has a
        # universal lowering to ``submit_task``, do the lowering
        # transparently. The brain enriches the payload with the
        # original ``(task_name, args, kwargs)`` it captured on
        # ``task.received`` so we have everything we need.
        #
        # This is what lets the dashboard show the same "Retry"
        # button on every engine without per-engine UI gating.
        if (
            action == "retry_task"
            and "retry_task" not in adapter.capabilities()
            and "submit_task" in adapter.capabilities()
        ):
            name = parameters.get("task_name") or parameters.get("name")
            if not name:
                return CommandResult(
                    status="failed",
                    error=(
                        "retry_task polyfill needs task_name in payload "
                        "(brain forwards from its tasks table)"
                    ),
                )
            # 1.7.1 (H1): the brain stores task args REDACTED and forwards
            # args=None / kwargs=None, so for a polyfill engine (no native
            # retry_task; the retry is lowered to submit_task) the ORIGINAL
            # arguments cannot be replayed. Re-submitting with () / {} would
            # silently re-run the task with the wrong inputs. Require an
            # explicit operator override; fail closed otherwise. (Some
            # polyfill engines, e.g. arq/taskiq, do not even capture args,
            # so the original_had_args flag is not reliable enough to gate
            # on -- the only safe input is an explicit override.)
            override_args = parameters.get("override_args")
            override_kwargs = parameters.get("override_kwargs")
            # RH2 (+ H1): the brain stores the original arguments redacted and
            # cannot replay them for a polyfill engine, so BOTH override halves
            # must be supplied explicitly. A one-sided override (only args, or
            # only kwargs) would silently zero the OTHER half. Require both;
            # use () / {} for an explicitly empty half.
            if override_args is None or override_kwargs is None:
                return CommandResult(
                    status="failed",
                    error=(
                        "refusing polyfill retry: this engine has no native "
                        "retry and the brain stores the original arguments "
                        "redacted, so BOTH override_args and override_kwargs "
                        "must be supplied explicitly (use empty values for a "
                        "no-argument or no-keyword task)."
                    ),
                )
            return await adapter.submit_task(
                name,
                args=tuple(override_args),
                kwargs=dict(override_kwargs),
                queue=parameters.get("queue"),
                eta=parameters.get("eta") or parameters.get("eta_seconds"),
                priority=parameters.get("priority"),
            )

        if action not in adapter.capabilities():
            return CommandResult(
                status="failed",
                error=f"adapter {adapter.name!r} does not support action {action!r}",
            )

        if action == "retry_task":
            task_id = parameters.get("task_id") or target.get("task_id") or target.get("id")
            if not task_id:
                return CommandResult(status="failed", error="target.task_id required")
            # The adapter MUST receive every value it
            # needs to re-enqueue (task_name, args, kwargs) from the
            # brain rather than reading them off the broker itself.
            # Broker payloads are pickle on RQ + Dramatiq + Huey + arq,
            # and the agent process holds the HMAC signing key, so a
            # writable broker would otherwise be RCE on the agent.
            # Operator-supplied ``override_args`` / ``override_kwargs``
            # win; otherwise we fall back to the ``args`` / ``kwargs``
            # snapshot the brain captured at ``task.received`` and
            # forwarded in the command payload. Adapters that don't
            # honor overrides (celery natively re-reads the broker,
            # safe there) accept the kwargs and ignore them.
            #
            # RH1 (direction 2): NEVER fall back to brain-supplied args/kwargs.
            # The brain stores task arguments REDACTED (H3/M7), so replaying a
            # snapshot re-runs the task with scrubbed values (the literal
            # "[REDACTED]", or () on a default-config app). A 1.7.1 brain already
            # forwards args=None/kwargs=None, but an N-1 (1.7.0) brain still
            # sends the redacted snapshot -- using it would EXECUTE the
            # redactions. Only operator overrides are trusted; without them the
            # adapter re-runs the ORIGINAL broker job by reference (celery) or
            # fails closed (rq/dramatiq/huey), the safe contract at any brain age.
            override_args = _maybe_tuple(parameters.get("override_args"))
            override_kwargs = parameters.get("override_kwargs")
            if not isinstance(override_kwargs, dict):
                override_kwargs = None

            # RH2: huey / dramatiq re-run by name/reference and cannot faithfully
            # replay a ONE-SIDED override -- supplying only override_args (or only
            # override_kwargs) would silently zero the OTHER half (kwargs={} /
            # args ()), e.g. running charge(order_id) with the tenant kwarg
            # dropped. Require BOTH halves or NEITHER (neither == a by-reference
            # retry). Checked HERE, before the task_name smuggling below collapses
            # the None-vs-{} distinction inside override_kwargs.
            _engine_name = getattr(adapter, "name", "")
            if _engine_name in ("huey", "dramatiq") and (
                (override_args is None) != (override_kwargs is None)
            ):
                return CommandResult(
                    status="failed",
                    error=(
                        f"refusing one-sided override retry on {_engine_name!r}: "
                        "supply BOTH override_args and override_kwargs (use empty "
                        "values for a no-argument or no-keyword task), or NEITHER "
                        "to re-run the original task by reference."
                    ),
                )

            adapter_name = getattr(adapter, "name", "")

            # P1-1: ATTEST THE EFFECTIVE RETRY STACK. The huey / dramatiq retry
            # smuggles a control key (``__z4j_task_name__`` / ``__z4j_actor_name__``)
            # into override_kwargs (below). That is only SAFE if the loaded
            # adapter STRIPS the control key and fails closed on an empty override
            # -- the 1.7.1 contract. A pre-1.7.1 adapter does neither: it reads
            # the control-key-only dict as a real operator override and re-runs
            # the task with empty args, reporting success. z4j-bare and the engine
            # adapters are versioned and INSTALLED SEPARATELY, so a 1.7.1
            # dispatcher can be paired with a 1.7.0 z4j-huey / z4j-dramatiq. The
            # runtime's own version does not prove the adapter honours the
            # contract, so we check the ADAPTER's attestation flag and fail closed
            # (with an upgrade message, like the RQ refusal) when absent.
            if adapter_name in ("huey", "dramatiq") and not getattr(
                adapter, "safe_retry_by_reference", False
            ):
                return CommandResult(
                    status="failed",
                    error=(
                        f"refusing retry: this z4j-bare dispatcher requires "
                        f"z4j-{adapter_name} 1.7.1 or newer to retry safely. An "
                        f"older adapter mis-reads the internal task-name key as a "
                        f"real argument and would re-run the task with empty "
                        f"arguments. Upgrade with: pip install --upgrade "
                        f"'z4j-{adapter_name}>=1.7.1'"
                    ),
                )

            # Brain-supplied task_name replaces job.func_name
            # for adapters that would otherwise lazy-pickle-load on
            # attribute access (RQ is the documented case; arq /
            # dramatiq / taskiq read their own envelopes safely).
            task_name = parameters.get("task_name")

            # Huey's adapter expects task_name inside
            # override_kwargs at the magic key ``__z4j_task_name__``
            # (its retry_task signature pre-dates the task_name kwarg
            # contract). Inject it here so the Huey engine looks up
            # the registered callable correctly. We only inject for
            # Huey to avoid surprising other adapters that might
            # pass override_kwargs straight to the user function.
            #
            # Brain-derived task_name MUST win over any
            # operator-supplied ``__z4j_task_name__`` in override_kwargs.
            # Previously this used ``setdefault`` which would preserve
            # an operator-supplied value and let it slip through to
            # Huey's registry lookup. Direct assignment closes that.
            if task_name and adapter_name == "huey":
                override_kwargs = dict(override_kwargs or {})
                override_kwargs["__z4j_task_name__"] = task_name
            elif task_name and adapter_name == "dramatiq":
                # Dramatiq's retry_task pre-dates the task_name kwarg and
                # reads the actor name from
                # override_kwargs["__z4j_actor_name__"]. Nothing wrote that
                # key, and the dispatcher's task_name= kwarg raises
                # TypeError on dramatiq's signature -> the fallback call
                # dropped the name -> the retry failed closed "requires
                # actor_name", so the advertised Dramatiq retry button was
                # 100% dead. Inject it here (mirrors the Huey special-case)
                # so the fallback call carries the actor name.
                override_kwargs = dict(override_kwargs or {})
                override_kwargs["__z4j_actor_name__"] = task_name

            # Pass task_name as an explicit kwarg for adapters that
            # accept it. z4j-rq 1.6.7+ requires task_name (the action
            # layer fails closed if absent); the dispatcher must NOT
            # silently fall back to the legacy no-task_name call for
            # RQ specifically because mixed-version installs
            # (new z4j-bare 1.6.7+ + old z4j-rq <=1.6.6) would
            # otherwise re-expose the pickle RCE -- old z4j-rq
            # retry_task_action still reads ``job.func_name`` /
            # ``job.args`` / ``job.kwargs`` on the broker-stored Job.
            #
            # Hard-refuse the RQ retry instead of falling back.
            # The operator gets a clear "upgrade z4j-rq" message; the
            # alternative is a silent CVE re-opening, which is the
            # exact mistake the 1.6.7 CHANGELOG inadvertently claimed
            # was safe.
            #
            # For other adapters (celery natively re-reads the broker
            # safely; dramatiq's retry path doesn't read pickle
            # attributes; huey reads from its registry by name; arq
            # and taskiq decline retry without explicit overrides),
            # the legacy TypeError fallback is safe and preserves
            # mixed-version operator compat.
            try:
                return await adapter.retry_task(
                    task_id,
                    task_name=task_name,
                    override_args=override_args,
                    override_kwargs=override_kwargs,
                    eta=parameters.get("eta"),
                    # Brain looks up the original task's priority and
                    # forwards it so high-priority work doesn't get
                    # silently demoted on retry. ``None`` falls back
                    # to the broker's default priority slot.
                    priority=parameters.get("priority"),
                )
            except TypeError:
                if adapter_name == "rq":
                    return CommandResult(
                        status="failed",
                        error=(
                            "refusing retry: this z4j-bare dispatcher "
                            "(1.6.8+) requires z4j-rq 1.6.7 or newer "
                            "to thread brain-supplied task_name through "
                            "the retry path. Older z4j-rq adapters "
                            "ignore the kwarg and fall back to reading "
                            "the broker-stored job.func_name, which "
                            "re-opens the pickle-deserialization RCE. Upgrade "
                            "with: pip install --upgrade 'z4j-rq>=1.6.7'"
                        ),
                    )
                return await adapter.retry_task(
                    task_id,
                    override_args=override_args,
                    override_kwargs=override_kwargs,
                    eta=parameters.get("eta"),
                    priority=parameters.get("priority"),
                )

        if action == "cancel_task":
            task_id = parameters.get("task_id") or target.get("task_id") or target.get("id")
            if not task_id:
                return CommandResult(status="failed", error="target.task_id required")
            return await adapter.cancel_task(task_id)

        if action == "bulk_retry":
            filt = parameters.get("filter", {})
            try:
                requested = int(parameters.get("max", 1000))
            except (TypeError, ValueError):
                return CommandResult(
                    status="failed",
                    error="bulk_retry max must be an integer",
                )
            if requested <= 0:
                return CommandResult(
                    status="failed",
                    error="bulk_retry max must be positive",
                )
            bounded = min(requested, BULK_RETRY_HARD_MAX)
            # M12: same attestation as retry_task / requeue_dead_letter. A bulk
            # retry of a huey/dramatiq batch is by-reference-only under the 1.7.1
            # contract; a pre-1.7.1 adapter selects by a control key the 1.7.1
            # brain no longer sends (e.g. filter["actors"]), so it would skip
            # EVERY requested id and report status="success" -- a silent no-op
            # that looks like the retries happened. z4j-bare and the engine
            # adapters are versioned + installed separately, so a 1.7.1 dispatcher
            # can be paired with a 1.7.0 adapter; check the ADAPTER's attestation
            # and fail closed (actionable upgrade message) rather than silently
            # succeed with zero work. Do NOT inject an actors map instead -- that
            # would reintroduce the empty-arg re-run hazard.
            adapter_name = getattr(adapter, "name", "")
            if adapter_name in ("huey", "dramatiq") and not getattr(
                adapter, "safe_retry_by_reference", False
            ):
                return CommandResult(
                    status="failed",
                    error=(
                        f"refusing bulk_retry: this z4j-bare dispatcher requires "
                        f"z4j-{adapter_name} 1.7.1 or newer to retry safely. An "
                        f"older adapter would skip every requested task and report "
                        f"success. Upgrade with: pip install --upgrade "
                        f"'z4j-{adapter_name}>=1.7.1'"
                    ),
                )
            # Per-task overrides live in
            # ``filter["overrides"]`` and per-task task_names live in
            # ``filter["task_names"]`` (both {task_id: ...} maps
            # populated by the brain). The dispatcher passes ``filter``
            # through verbatim so the action gets every override AND
            # task_name the brain captured; no extra threading needed
            # here. We forward batch-wide ``override_args`` /
            # ``override_kwargs`` too when the brain sets them as a
            # default for the whole batch (uncommon, but used by
            # scripted bulk retries that want a single rewritten
            # payload across all ids). Older adapters without those
            # kwargs fall back via TypeError.
            override_args = _maybe_tuple(parameters.get("override_args"))
            override_kwargs = parameters.get("override_kwargs")
            if override_args is not None or isinstance(override_kwargs, dict):
                try:
                    return await adapter.bulk_retry(
                        filt,
                        max=bounded,
                        override_args=override_args,
                        override_kwargs=(
                            override_kwargs if isinstance(override_kwargs, dict) else None
                        ),
                    )
                except TypeError:
                    # Older adapter signature: bulk_retry(filter, max).
                    # Fall through to the unenriched call.
                    pass
            return await adapter.bulk_retry(filt, max=bounded)

        if action == "purge_queue":
            queue = parameters.get("queue") or target.get("queue") or target.get("id")
            if not queue:
                return CommandResult(status="failed", error="target.queue required")
            confirm_token = parameters.get("confirm_token")
            force = bool(parameters.get("force", False))
            return await adapter.purge_queue(
                queue,
                confirm_token=confirm_token,
                force=force,
            )

        if action == "requeue_dead_letter":
            task_id = parameters.get("task_id") or target.get("task_id") or target.get("id")
            if not task_id:
                return CommandResult(status="failed", error="target.task_id required")
            # (DLQ fallback path): some adapters (rq)
            # fall back to ``retry_task_action`` when the broker's
            # native dead-letter API is unreachable, and that path
            # requires brain-supplied task_name AND override_args /
            # override_kwargs for the same pickle-safety reason as
            # ``retry_task`` above. Forward operator overrides first,
            # RH1 (direction 2): operator overrides ONLY -- never replay the
            # brain's redacted args/kwargs snapshot (an N-1 1.7.0 brain still
            # sends it; executing it re-runs the DLQ task with scrubbed values).
            # Without overrides the adapter resurrects the ORIGINAL message by
            # reference (the DLQ preserves it) or fails closed.
            override_args = _maybe_tuple(parameters.get("override_args"))
            override_kwargs = parameters.get("override_kwargs")
            if not isinstance(override_kwargs, dict):
                override_kwargs = None
            task_name = parameters.get("task_name")
            adapter_name = getattr(adapter, "name", "")
            # P1-1: same attestation as retry_task -- the DLQ path also smuggles a
            # control key into override_kwargs for huey/dramatiq, so a pre-1.7.1
            # adapter would mis-read it and resurrect with an empty payload.
            # Refuse (fail closed) unless the loaded adapter attests the contract.
            if adapter_name in ("huey", "dramatiq") and not getattr(
                adapter, "safe_retry_by_reference", False
            ):
                return CommandResult(
                    status="failed",
                    error=(
                        f"refusing dead-letter requeue: this z4j-bare dispatcher "
                        f"requires z4j-{adapter_name} 1.7.1 or newer to requeue "
                        f"safely. An older adapter mis-reads the internal task-name "
                        f"key as a real argument and would resurrect the message "
                        f"with empty arguments. Upgrade with: pip install "
                        f"--upgrade 'z4j-{adapter_name}>=1.7.1'"
                    ),
                )
            # RH2: the DLQ fallback delegates to the same by-reference retry, so
            # a one-sided override on huey/dramatiq zeros the other half here
            # too. Require both halves or neither (checked before the smuggling).
            if adapter_name in ("huey", "dramatiq") and (
                (override_args is None) != (override_kwargs is None)
            ):
                return CommandResult(
                    status="failed",
                    error=(
                        f"refusing one-sided override requeue on {adapter_name!r}: "
                        "supply BOTH override_args and override_kwargs, or NEITHER "
                        "to resurrect the original message by reference."
                    ),
                )
            # Same per-adapter name-threading as the retry_task branch:
            # Huey / Dramatiq read the name from a magic override_kwargs
            # key, and dramatiq's signature rejects the task_name= kwarg,
            # so inject before the (fallback) call.
            if task_name and adapter_name == "huey":
                override_kwargs = dict(override_kwargs or {})
                override_kwargs["__z4j_task_name__"] = task_name
            elif task_name and adapter_name == "dramatiq":
                override_kwargs = dict(override_kwargs or {})
                override_kwargs["__z4j_actor_name__"] = task_name
            try:
                return await adapter.requeue_dead_letter(
                    task_id,
                    task_name=task_name,
                    override_args=override_args,
                    override_kwargs=override_kwargs,
                )
            except TypeError:
                # Same RQ-specific fail-closed posture as the
                # retry_task path above. Old z4j-rq adapters silently
                # read job.func_name / args / kwargs on the registry
                # fallback - refuse rather than silently re-open the
                # pickle RCE.
                if adapter_name == "rq":
                    return CommandResult(
                        status="failed",
                        error=(
                            "refusing DLQ requeue: this z4j-bare "
                            "dispatcher (1.6.8+) requires z4j-rq "
                            "1.6.7 or newer for safe registry-fallback "
                            "behavior. Older z4j-rq adapters would "
                            "re-open the pickle-deserialization RCE. Upgrade "
                            "with: pip install --upgrade 'z4j-rq>=1.6.7'"
                        ),
                    )
                # Mid-version adapter signature: accepts overrides but
                # not task_name. Try without task_name; the action layer
                # still fails closed if task_name is required.
                try:
                    return await adapter.requeue_dead_letter(
                        task_id,
                        override_args=override_args,
                        override_kwargs=override_kwargs,
                    )
                except TypeError:
                    # Oldest adapter signature: requeue_dead_letter(task_id)
                    # only. Fall back so a brand-new brain doesn't break
                    # a pinned older agent. The pickle-safety bound still
                    # applies inside the action; this is just protocol
                    # tolerance.
                    return await adapter.requeue_dead_letter(task_id)

        if action == "restart_worker":
            worker_name = (
                parameters.get("worker_name")
                or target.get("worker_name")
                or target.get("worker_id")
                or target.get("id")
            )
            if not worker_name:
                return CommandResult(status="failed", error="target.worker_name required")
            return await adapter.restart_worker(worker_name)

        if action in ("pool_grow", "pool_shrink"):
            worker_name = parameters.get("worker_name") or target.get("id")
            delta = int(parameters.get("delta", 1))
            if not worker_name:
                return CommandResult(status="failed", error="target.worker_name required")
            method = getattr(adapter, action, None)
            if method is None:
                return CommandResult(status="failed", error=f"adapter does not support {action!r}")
            return await method(worker_name, delta)

        if action in ("add_consumer", "cancel_consumer"):
            worker_name = parameters.get("worker_name") or target.get("id")
            queue = parameters.get("queue")
            if not worker_name or not queue:
                return CommandResult(status="failed", error="worker_name and queue required")
            method = getattr(adapter, action, None)
            if method is None:
                return CommandResult(status="failed", error=f"adapter does not support {action!r}")
            return await method(worker_name, queue)

        if action == "rate_limit":
            task_name = parameters.get("task_name")
            rate = parameters.get("rate")
            # ``worker_name`` is intentionally optional: an empty /
            # missing value means "broadcast to every worker"
            # (emergency-throttle path). The action layer logs at
            # CRITICAL when it sees a global broadcast.
            worker_name = parameters.get("worker_name") or target.get("id") or None
            if not task_name or not rate:
                return CommandResult(
                    status="failed",
                    error="rate_limit: task_name and rate required",
                )
            method = getattr(adapter, "rate_limit", None)
            if method is None:
                return CommandResult(
                    status="failed",
                    error="adapter does not support 'rate_limit'",
                )
            return await method(
                task_name,
                rate,
                worker_name=worker_name,
            )

        return CommandResult(status="failed", error=f"unrecognized action {action!r}")

    async def _dispatch_scheduler(  # noqa: PLR0911  flat command dispatch
        self,
        action: str,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> CommandResult:
        scheduler_name = target.get("scheduler") or self._single_scheduler_name()
        adapter = self.schedulers.get(scheduler_name) if scheduler_name else None
        if adapter is None:
            return CommandResult(
                status="failed",
                error=f"no scheduler adapter registered for {scheduler_name!r}",
            )

        sub_action = action.split(".", 1)[1] if "." in action else ""
        schedule_id = parameters.get("schedule_id") or target.get("schedule_id") or target.get("id")
        if not schedule_id:
            return CommandResult(status="failed", error="target.schedule_id required")

        if sub_action == "enable":
            return await adapter.enable_schedule(schedule_id)
        if sub_action == "disable":
            return await adapter.disable_schedule(schedule_id)
        if sub_action == "trigger_now":
            return await adapter.trigger_now(schedule_id)
        if sub_action == "delete":
            return await adapter.delete_schedule(schedule_id)

        return CommandResult(
            status="failed",
            error=f"unrecognized schedule action {action!r}",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _dedup_key(self, frame: CommandFrame) -> str:
        """Compute the composite dedup identity for a command frame.

        ``fire:{fire_id}`` for a fire action carrying a ``fire_id`` (so the same
        fire re-dispatched under a new command_id still dedups --), else
        ``cmd:{command_id}``. The fallback also covers a fire frame missing its
        fire_id, so distinct commands never collapse onto a shared key.
        """
        action = getattr(frame.payload, "action", None)
        if action in self._FIRE_ACTIONS:
            params = getattr(frame.payload, "parameters", None)
            fire_id = params.get("fire_id") if isinstance(params, dict) else None
            if fire_id:
                return f"fire:{fire_id}"
        return f"cmd:{frame.id}"

    def _is_duplicate(self, key: str) -> bool:
        """Bool wrapper over :meth:`_duplicate_tier` (kept for callers/tests)."""
        return self._duplicate_tier(key) is not None

    def _duplicate_tier(self, key: str) -> str | None:
        """Which dedup tier recognised ``key``: ``"memory"``, ``"durable"``, or None.

        The distinction matters for the reply: a ``durable`` hit proves a PRIOR
        process completed the command successfully (that ledger is written only
        after success), so the replacement can be terminalized; a ``memory`` hit
        may be a still-in-flight duplicate inside this process, which will report
        its own result.

        Two-tier. The in-memory OrderedDict is the fast path within a
        single process lifetime; the buffer's DURABLE dedup
        (:meth:`BufferStore.mark_command_seen`) survives an agent RESTART, so a
        command re-delivered to a restarted agent (its in-memory cache empty) is
        still recognised as a duplicate instead of re-executed -- which for a
        scheduled fire would be a DUPLICATED fire. The ``key`` is the composite
        identity from :meth:`_dedup_key` (fire_id-scoped for fires), so this
        holds across a re-dispatch that assigns a new command_id.
        """
        now = time.monotonic()
        # Evict expired entries from the front of the OrderedDict. This sweep
        # assumes front-to-back is oldest-to-newest, which only holds if every
        # write moves its key to the end -- see _record_success_key.
        while self._seen_commands:
            oldest_id, oldest_ts = next(iter(self._seen_commands.items()))
            if now - oldest_ts > self._DEDUP_TTL:
                self._seen_commands.pop(oldest_id)
            else:
                break

        # SUCCESS-ONLY suppression. This used to write the key here, before
        # execution, and never remove it when the command failed -- so a fire
        # whose first attempt FAILED was suppressed on re-delivery within the TTL
        # and never ran. That is the same work-loss the durable tier was fixed
        # for; the fix was applied to one tier and not the other. The key is now
        # recorded only after a successful execution (_record_success_key).
        #
        # In-flight keys are tracked separately so a duplicate arriving while the
        # original is still running is still absorbed. Inbound frames are handled
        # serially, so this cannot currently interleave, but it must not become a
        # double-execution the moment that changes.
        in_memory_dup = key in self._seen_commands or key in self._inflight_keys
        # Durable cross-restart dedup, CHECK ONLY. The
        # recording half runs in ``handle`` after a SUCCESSFUL execution, so a
        # crash between this check and the broker enqueue leaves no marker and
        # the recovery delivery correctly re-executes (at-least-once) instead of
        # being suppressed (which lost the fire).
        #
        # The durable tier is consulted even when the in-memory tier already hit:
        # a durable record PROVES the earlier run completed successfully, which
        # lets the caller terminalize the replacement. Reporting "memory" there
        # would leave a duplicate of an already-COMPLETED command sitting
        # DISPATCHED until it timed out.
        try:
            if self.buffer.has_command_seen(key, ttl_seconds=self._DEDUP_TTL):
                return "durable"
        except Exception:
            logger.debug("z4j dedup key %s: durable dedup check failed", key)
        if in_memory_dup:
            return "memory"
        # Not a duplicate: this execution now owns the key until it reports.
        self._inflight_keys.add(key)
        return None

    def _record_success_key(self, key: str) -> None:
        """Mark ``key`` as SUCCESSFULLY executed in this process.

        Called only after a successful execution. ``move_to_end`` keeps the
        OrderedDict genuinely ordered by write time -- re-assigning an existing
        key updates its value WITHOUT reordering it, which left the TTL sweep
        walking a front that was no longer the oldest entry, so it stopped early
        and left expired keys suppressing work. The cap is enforced AFTER the
        insert, so the map can no longer sit one over its bound.
        """
        self._seen_commands[key] = time.monotonic()
        self._seen_commands.move_to_end(key)
        while len(self._seen_commands) > self._DEDUP_MAX:
            self._seen_commands.popitem(last=False)

    def _single_engine_name(self) -> str | None:
        return next(iter(self.engines), None) if len(self.engines) == 1 else None

    def _single_scheduler_name(self) -> str | None:
        return next(iter(self.schedulers), None) if len(self.schedulers) == 1 else None

    def _queue_ack(
        self,
        command_id: str,
        *,
        delivery_claim_token: str | None,
    ) -> None:
        frame = CommandAckFrame(
            id=command_id,
            payload=CommandAckPayload(
                delivery_claim_token=delivery_claim_token,
            ),
        )
        self.buffer.append("command_ack", serialize_frame(frame))

    def _queue_result(
        self,
        command_id: str,
        result: CommandResult,
        *,
        delivery_claim_token: str | None,
    ) -> None:
        frame = CommandResultFrame(
            id=command_id,
            payload=CommandResultPayload(
                status=result.status,  # type: ignore[arg-type]
                result=result.result,
                error=result.error,
                delivery_claim_token=delivery_claim_token,
            ),
        )
        self.buffer.append("command_result", serialize_frame(frame))

    async def _self_exit_restart(
        self,
        parameters: dict[str, Any],
    ) -> CommandResult:
        """Graceful self-exit polyfill for ``restart_worker``.

        Emits a ``worker.offline`` event tagged with
        ``data.reason="restart"`` so the brain (and the dashboard's
        worker list) knows the exit is intentional, then schedules
        ``os._exit(0)`` after a short delay so the result + event
        frames have a chance to flush over the WebSocket.

        The host's process supervisor (docker / k8s / systemd /
        supervisor) respawns the process per its restart policy.
        Zero shell exec; only new privilege is the agent killing
        its own host process - DoS-equivalent to capabilities the
        agent already has via broker-credential abuse.

        PREFLIGHT (audit-driven): refuses to exit if no process
        supervisor can be detected. Without this guard the agent
        would silently kill bare-shell workers with nothing to
        respawn them. Users with custom supervisors can set
        ``Z4J_ORCHESTRATED=1`` to force the check to pass.
        """
        import asyncio
        import os
        import secrets as _secrets
        from datetime import UTC, datetime

        from z4j_core.transport.frames import (
            EventBatchFrame,
            EventBatchPayload,
        )

        worker_name = parameters.get("worker_name") or parameters.get("worker_id") or "self"

        # Orchestration preflight. Refuses when no filesystem-
        # anchored supervisor signal is present - prevents an
        # unprivileged attacker who can only set env vars on the
        # worker process from turning the call into a non-
        # Respawning self-exit.
        detection = detect_orchestrator()
        if not detection.detected:
            logger.warning(
                "z4j: restart_worker refused - no supervisor detected (worker=%s)",
                worker_name,
            )
            return CommandResult(
                status="failed",
                error=(
                    "restart_worker refused: no process supervisor "
                    "detected. Running under docker / k8s / systemd "
                    "/ supervisord is auto-detected via /.dockerenv "
                    "or /proc/1/cgroup. If you're in an exotic "
                    "supervisor setup, create /etc/z4j-orchestrated "
                    "AND set Z4J_ORCHESTRATED=1 on the worker."
                ),
            )

        # Flap guard: refuse a restart within the first
        # ``_RESTART_MIN_UPTIME_SECONDS`` of the process's life so
        # a compromised brain can't loop the worker into a meltdown
        # against the orchestrator's restart policy.
        import time as _time

        process_age = _time.monotonic() - self._process_start_monotonic
        if process_age < self._RESTART_MIN_UPTIME_SECONDS:
            return CommandResult(
                status="failed",
                error=(
                    f"restart_worker refused: worker uptime is "
                    f"{process_age:.0f}s, below the flap-guard floor "
                    f"of {self._RESTART_MIN_UPTIME_SECONDS}s."
                ),
            )

        # Best-effort lifecycle event into the buffer.
        try:
            event_frame = EventBatchFrame(
                # 128-bit id: event_batch ids key the agent's _pending_acks
                # map, so a 48-bit (token_hex(6)) collision could let a real
                # ack for one entry confirm-and-delete a different, unstored
                # entry. 35 chars, within the 64-char id cap.
                id=f"ev_{_secrets.token_hex(16)}",
                ts=datetime.now(UTC),
                payload=EventBatchPayload(
                    events=[
                        {
                            "id": _secrets.token_hex(16),
                            "kind": "worker.offline",
                            "engine": "",
                            "task_id": "",
                            "occurred_at": datetime.now(UTC).isoformat(),
                            "data": {
                                "reason": "restart",
                                "worker_name": worker_name,
                            },
                        },
                    ],
                ),
            )
            self.buffer.append("event_batch", serialize_frame(event_frame))
        except Exception:
            logger.exception(
                "z4j: restart self-exit event emit failed; exiting anyway",
            )

        # Schedule the exit AFTER the result frame leaves the wire.
        # 250ms is the budget for the buffer flush + WS write - well
        # above the typical <10ms send cost, well under any user-
        # visible delay. Tests inject a faster exit via the
        # ``_RESTART_EXIT_DELAY`` class attribute.
        delay = self._RESTART_EXIT_DELAY
        try:
            loop = asyncio.get_running_loop()
            loop.call_later(delay, os._exit, 0)
            logger.info(
                "z4j: self-exit scheduled in %.2fs (worker=%s)",
                delay,
                worker_name,
            )
        except RuntimeError:
            # No running loop - exit immediately. The host should be
            # in shutdown already if there's no loop.
            logger.warning("z4j: no running loop; exiting immediately")
            os._exit(0)

        return CommandResult(
            status="success",
            result={
                "restarted_via": "self_exit",
                "worker_name": worker_name,
                "exit_in_seconds": delay,
            },
        )

    # Tests override this to skip the actual ``os._exit`` call. The
    # default delay is generous enough for production WS flush; lower
    # numbers risk the result frame being lost in the kernel buffer
    # at exit.
    _RESTART_EXIT_DELAY: float = 0.25

    # Flap guard: the agent refuses to self-exit within
    # this many seconds of its own startup. Prevents a compromised
    # brain from putting the worker into a restart loop that burns
    # through the orchestrator's restart budget.
    _RESTART_MIN_UPTIME_SECONDS: float = 60.0

    # Set once at process start; used by the flap guard above.
    # Each dispatcher instance stamps this in __init__.
    _process_start_monotonic: float = 0.0


def _maybe_tuple(value: object) -> tuple[Any, ...] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return None


__all__ = ["CommandDispatcher"]
