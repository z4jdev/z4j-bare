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
from typing import TYPE_CHECKING, Any

from z4j_bare.orchestrator_detect import detect_orchestrator
from z4j_core.errors import Z4JError
from z4j_core.models import CommandResult
from z4j_core.protocols import QueueEngineAdapter, SchedulerAdapter
from z4j_core.transport.frames import (
    CommandAckFrame,
    CommandFrame,
    CommandResultFrame,
    CommandResultPayload,
    serialize_frame,
)

if TYPE_CHECKING:
    from z4j_bare.buffer import BufferStore

logger = logging.getLogger("z4j.agent.dispatcher")

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

    def __init__(
        self,
        *,
        engines: dict[str, QueueEngineAdapter],
        schedulers: dict[str, SchedulerAdapter],
        buffer: BufferStore,
    ) -> None:
        self.engines = engines
        self.schedulers = schedulers
        self.buffer = buffer
        # Dedup cache: command_id -> monotonic timestamp of first processing.
        # Prevents duplicate execution if the brain resends a command
        # due to ack timeout.
        self._seen_commands: OrderedDict[str, float] = OrderedDict()
        # Flap-guard anchor: how long has this process been alive?
        # Used by ``_self_exit_restart`` to refuse too-frequent
        # restarts (audit M3).
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
        # 0. Deduplication check
        if self._is_duplicate(frame.id):
            logger.info("z4j command %s: duplicate, re-acking", frame.id)
            self._queue_ack(frame.id)
            return

        # 1. Immediate ack so the brain knows we saw it
        self._queue_ack(frame.id)

        # 2. Execute
        try:
            result = await self._execute(frame)
        except Z4JError as exc:
            result = CommandResult(status="failed", error=f"{exc.code}: {exc.message}")
        except Exception as exc:  # noqa: BLE001
            logger.exception("z4j unexpected dispatcher error for command %s", frame.id)
            result = CommandResult(status="failed", error=f"internal error: {exc}")

        self._queue_result(frame.id, result)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def _execute(self, frame: CommandFrame) -> CommandResult:
        action = frame.payload.action
        target = frame.payload.target
        parameters = frame.payload.parameters

        # Schedule actions go to a SchedulerAdapter
        if action.startswith("schedule."):
            return await self._dispatch_scheduler(action, target, parameters)

        # Everything else goes to a QueueEngineAdapter
        return await self._dispatch_engine(action, target, parameters)

    async def _dispatch_engine(
        self,
        action: str,
        target: dict[str, Any],
        parameters: dict[str, Any],
    ) -> CommandResult:
        engine_name = target.get("engine") or self._single_engine_name()
        adapter = self.engines.get(engine_name) if engine_name else None
        if adapter is None:
            return CommandResult(
                status="failed",
                error=f"no engine adapter registered for {engine_name!r}",
            )

        # ``submit_task`` is the universal v1.0+ enqueue primitive.
        # The brain calls it directly (and uses it as a polyfill for
        # retry / bulk_retry / requeue_dlq).
        if action == "submit_task":
            name = parameters.get("name") or target.get("name")
            if not name:
                return CommandResult(
                    status="failed", error="submit_task: name required",
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
            task_id = (
                parameters.get("task_id")
                or target.get("task_id")
                or target.get("id")
            )
            if not task_id:
                return CommandResult(
                    status="failed", error="target.task_id required",
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
        if (
            action == "restart_worker"
            and "restart_worker" not in adapter.capabilities()
        ):
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
            return await adapter.submit_task(
                name,
                args=tuple(
                    parameters.get("override_args")
                    or parameters.get("args")
                    or (),
                ),
                kwargs=(
                    parameters.get("override_kwargs")
                    or parameters.get("kwargs")
                    or {}
                ),
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
            return await adapter.retry_task(
                task_id,
                override_args=_maybe_tuple(parameters.get("override_args")),
                override_kwargs=parameters.get("override_kwargs"),
                eta=parameters.get("eta"),
                # Brain looks up the original task's priority and
                # forwards it so high-priority work doesn't get
                # silently demoted on retry. ``None`` falls back
                # to the broker's default priority slot.
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
            return await adapter.bulk_retry(filt, max=bounded)

        if action == "purge_queue":
            queue = parameters.get("queue") or target.get("queue") or target.get("id")
            if not queue:
                return CommandResult(status="failed", error="target.queue required")
            confirm_token = parameters.get("confirm_token")
            force = bool(parameters.get("force", False))
            return await adapter.purge_queue(
                queue, confirm_token=confirm_token, force=force,
            )

        if action == "requeue_dead_letter":
            task_id = parameters.get("task_id") or target.get("task_id") or target.get("id")
            if not task_id:
                return CommandResult(status="failed", error="target.task_id required")
            return await adapter.requeue_dead_letter(task_id)

        if action == "restart_worker":
            worker_name = parameters.get("worker_name") or target.get("worker_name") or target.get("worker_id") or target.get("id")
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
                task_name, rate, worker_name=worker_name,
            )

        return CommandResult(status="failed", error=f"unrecognized action {action!r}")

    async def _dispatch_scheduler(
        self,
        action: str,
        target: dict[str, Any],
        parameters: dict[str, Any],  # noqa: ARG002
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

    def _is_duplicate(self, command_id: str) -> bool:
        """Check if we've already processed this command. Thread-safe."""
        now = time.monotonic()
        # Evict expired entries from the front of the OrderedDict.
        while self._seen_commands:
            oldest_id, oldest_ts = next(iter(self._seen_commands.items()))
            if now - oldest_ts > self._DEDUP_TTL:
                self._seen_commands.pop(oldest_id)
            else:
                break
        # Cap size.
        while len(self._seen_commands) > self._DEDUP_MAX:
            self._seen_commands.popitem(last=False)

        if command_id in self._seen_commands:
            return True
        self._seen_commands[command_id] = now
        return False

    def _single_engine_name(self) -> str | None:
        return next(iter(self.engines), None) if len(self.engines) == 1 else None

    def _single_scheduler_name(self) -> str | None:
        return next(iter(self.schedulers), None) if len(self.schedulers) == 1 else None

    def _queue_ack(self, command_id: str) -> None:
        frame = CommandAckFrame(id=command_id, payload={})
        self.buffer.append("command_ack", serialize_frame(frame))

    def _queue_result(self, command_id: str, result: CommandResult) -> None:
        frame = CommandResultFrame(
            id=command_id,
            payload=CommandResultPayload(
                status=result.status,  # type: ignore[arg-type]
                result=result.result,
                error=result.error,
            ),
        )
        self.buffer.append("command_result", serialize_frame(frame))

    async def _self_exit_restart(
        self, parameters: dict[str, Any],
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

        worker_name = (
            parameters.get("worker_name")
            or parameters.get("worker_id")
            or "self"
        )

        # Orchestration preflight. Refuses when no filesystem-
        # anchored supervisor signal is present - prevents an
        # unprivileged attacker who can only set env vars on the
        # worker process from turning the call into a non-
        # respawning self-exit (audit H2).
        detection = detect_orchestrator()
        if not detection.detected:
            logger.warning(
                "z4j: restart_worker refused - no supervisor detected "
                "(worker=%s)", worker_name,
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

        # Flap guard (audit M3): refuse a restart within the first
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
                id=f"ev_{_secrets.token_hex(6)}",
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
        except Exception:  # noqa: BLE001
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
                delay, worker_name,
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

    # Flap guard (audit M3): the agent refuses to self-exit within
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
