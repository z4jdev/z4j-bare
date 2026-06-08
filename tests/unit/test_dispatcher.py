"""Unit tests for ``z4j_bare.dispatcher.CommandDispatcher``.

Uses fake engine + scheduler adapters so we don't need a real Celery
installation to exercise the routing logic.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Schedule,
    ScheduleKind,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.transport.frames import (
    CommandFrame,
    CommandPayload,
    parse_frame,
)

from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher


class FakeEngine:
    """Minimal QueueEngineAdapter implementation for tests."""

    name = "fake"
    protocol_version = "1"

    def __init__(self) -> None:
        self.retry_calls: list[tuple[str, tuple | None, dict | None, float | None]] = []
        self.cancel_calls: list[str] = []
        self.bulk_calls: list[tuple[dict, int]] = []
        self.purge_calls: list[str] = []
        self.dlq_calls: list[str] = []
        self.restart_calls: list[str] = []
        self._capabilities = {
            "retry_task",
            "cancel_task",
            "bulk_retry",
            "purge_queue",
            "requeue_dead_letter",
            "restart_worker",
        }

    async def discover_tasks(
        self, hints: DiscoveryHints | None = None,  # noqa: ARG002
    ) -> list[TaskDefinition]:
        return []

    async def subscribe_registry_changes(self) -> AsyncIterator[TaskRegistryDelta]:
        if False:
            yield  # pragma: no cover  - empty async iterator

    async def subscribe_events(self) -> AsyncIterator[Event]:
        if False:
            yield  # pragma: no cover

    async def list_queues(self) -> list[Queue]:
        return []

    async def list_workers(self) -> list[Worker]:
        return []

    async def get_task(self, task_id: str) -> Task | None:  # noqa: ARG002
        return None

    async def retry_task(
        self,
        task_id: str,
        *,
        override_args: tuple | None = None,
        override_kwargs: dict | None = None,
        eta: float | None = None,
        priority: object = None,  # noqa: ARG002
    ) -> CommandResult:
        # ``priority`` is accepted but not asserted on by this
        # fake - the real preservation contract is exercised by
        # the celery action tests in z4j-celery. Adding it here
        # just keeps the kwarg shape consistent with the
        # production adapter so the dispatcher's call-through
        # doesn't TypeError.
        self.retry_calls.append((task_id, override_args, override_kwargs, eta))
        return CommandResult(status="success", result={"new_task_id": f"new-{task_id}"})

    async def cancel_task(self, task_id: str) -> CommandResult:
        self.cancel_calls.append(task_id)
        return CommandResult(status="success")

    async def bulk_retry(self, filter: dict, *, max: int = 1000) -> CommandResult:
        self.bulk_calls.append((filter, max))
        return CommandResult(status="success", result={"retried": 42})

    async def purge_queue(
        self,
        queue_name: str,
        *,
        confirm_token: str | None = None,
        force: bool = False,
    ) -> CommandResult:
        self.purge_calls.append(queue_name)
        return CommandResult(status="success")

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:
        self.dlq_calls.append(task_id)
        return CommandResult(status="success")

    async def restart_worker(self, worker_id: str) -> CommandResult:
        self.restart_calls.append(worker_id)
        return CommandResult(status="success")

    def capabilities(self) -> set[str]:
        return set(self._capabilities)


class FakeScheduler:
    name = "celery-beat"

    def __init__(self) -> None:
        self.enable_calls: list[str] = []
        self.disable_calls: list[str] = []
        self.trigger_calls: list[str] = []
        self.delete_calls: list[str] = []

    async def list_schedules(self) -> list[Schedule]:
        return []

    async def get_schedule(self, schedule_id: str) -> Schedule | None:  # noqa: ARG002
        return None

    async def create_schedule(self, spec: Schedule) -> Schedule:
        return spec

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:  # noqa: ARG002
        return spec

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        self.delete_calls.append(schedule_id)
        return CommandResult(status="success")

    async def enable_schedule(self, schedule_id: str) -> CommandResult:
        self.enable_calls.append(schedule_id)
        return CommandResult(status="success")

    async def disable_schedule(self, schedule_id: str) -> CommandResult:
        self.disable_calls.append(schedule_id)
        return CommandResult(status="success")

    async def trigger_now(self, schedule_id: str) -> CommandResult:
        self.trigger_calls.append(schedule_id)
        return CommandResult(status="success")

    def capabilities(self) -> set[str]:
        return {"list", "create", "update", "delete", "enable", "disable", "trigger_now"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.fixture
def engine() -> FakeEngine:
    return FakeEngine()


@pytest.fixture
def scheduler() -> FakeScheduler:
    return FakeScheduler()


@pytest.fixture
def dispatcher(
    buf: BufferStore,
    engine: FakeEngine,
    scheduler: FakeScheduler,
) -> CommandDispatcher:
    return CommandDispatcher(
        engines={"fake": engine},
        schedulers={"celery-beat": scheduler},
        buffer=buf,
    )


def _make_command(
    *,
    action: str,
    target: dict[str, Any] | None = None,
    parameters: dict[str, Any] | None = None,
) -> CommandFrame:
    return CommandFrame(
        id="cmd_test_01",
        payload=CommandPayload(
            action=action,
            target=target or {},
            parameters=parameters or {},
        ),
        hmac="deadbeef" * 8,
    )


def _decode_frame(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAck:
    async def test_ack_is_queued_before_execute(
        self, dispatcher: CommandDispatcher, engine: FakeEngine, buf: BufferStore,
    ) -> None:
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "abc"},
        )
        await dispatcher.handle(cmd)

        entries = buf.drain(10)
        kinds = [e.kind for e in entries]
        assert "command_ack" in kinds
        assert "command_result" in kinds
        # Ack must come first in the queue.
        assert kinds.index("command_ack") < kinds.index("command_result")
        assert engine.retry_calls == [("abc", None, None, None)]


class TestRetryTask:
    async def test_retry_happy_path(
        self, dispatcher: CommandDispatcher, engine: FakeEngine, buf: BufferStore,
    ) -> None:
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "xyz"},
        )
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result_frames = [e for e in entries if e.kind == "command_result"]
        assert len(result_frames) == 1
        parsed = _decode_frame(result_frames[0].payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"] == {"new_task_id": "new-xyz"}

    async def test_retry_with_overrides(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "xyz"},
            parameters={"override_args": [1, 2], "override_kwargs": {"k": "v"}},
        )
        await dispatcher.handle(cmd)
        task_id, args, kwargs, _ = engine.retry_calls[0]
        assert task_id == "xyz"
        assert args == (1, 2)
        assert kwargs == {"k": "v"}

    async def test_retry_missing_task_id(
        self, dispatcher: CommandDispatcher, buf: BufferStore,
    ) -> None:
        cmd = _make_command(action="retry_task", target={"engine": "fake"})
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result = [e for e in entries if e.kind == "command_result"][0]
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        assert "task_id" in (parsed["payload"]["error"] or "")

    async def test_single_engine_default_engine(
        self, buf: BufferStore, engine: FakeEngine,
    ) -> None:
        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(action="retry_task", target={"task_id": "abc"})
        await dispatcher.handle(cmd)
        assert engine.retry_calls == [("abc", None, None, None)]

    async def test_retry_threads_brain_snapshot_args_kwargs_as_overrides(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        """R7 H-2: when the operator did NOT supply ``override_args`` /
        ``override_kwargs``, the dispatcher MUST forward the brain's
        ``args`` / ``kwargs`` snapshot (captured at ``task.received``)
        as override_args=/override_kwargs= so the adapter never has to
        read the broker (which would be a pickle load on RQ / Dramatiq
        / Huey / arq). This is the H-2 thread-through contract.
        """
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "snap-1"},
            parameters={
                # No operator overrides; only the brain snapshot.
                "task_name": "myapp.do_thing",
                "args": [10, 20],
                "kwargs": {"flag": True},
            },
        )
        await dispatcher.handle(cmd)
        task_id, args, kwargs, _ = engine.retry_calls[0]
        assert task_id == "snap-1"
        # Snapshot args/kwargs must reach the adapter as overrides,
        # NOT as None - otherwise the adapter falls back to reading
        # job.args (pickle path) and H-2 is still open.
        assert args == (10, 20)
        assert kwargs == {"flag": True}

    async def test_retry_operator_override_beats_brain_snapshot(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        """When BOTH the operator's overrides AND the brain's snapshot
        are present, the operator wins - that's the whole point of the
        override surface."""
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "snap-2"},
            parameters={
                "args": [99, 99],
                "kwargs": {"snapshot": True},
                "override_args": [1, 2],
                "override_kwargs": {"operator": True},
            },
        )
        await dispatcher.handle(cmd)
        _, args, kwargs, _ = engine.retry_calls[0]
        assert args == (1, 2)
        assert kwargs == {"operator": True}

    async def test_retry_snapshot_kwargs_only_args_missing(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        """Mixed shape: brain only forwarded kwargs (the original task
        had no positional args). Snapshot kwargs still thread through
        as override_kwargs while override_args remains None."""
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "snap-3"},
            parameters={"kwargs": {"only_kwargs": "yes"}},
        )
        await dispatcher.handle(cmd)
        _, args, kwargs, _ = engine.retry_calls[0]
        assert args is None
        assert kwargs == {"only_kwargs": "yes"}


class TestCancelAndOthers:
    async def test_cancel(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="cancel_task",
            target={"engine": "fake", "task_id": "abc"},
        )
        await dispatcher.handle(cmd)
        assert engine.cancel_calls == ["abc"]

    async def test_reconcile_task_calls_adapter_method(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # Patch reconcile_task onto the fake engine for this test -
        # reconciliation isn't part of the FakeEngine's default
        # capability set but the dispatcher calls it via getattr().
        async def fake_reconcile(task_id: str):
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "success",
                    "finished_at": None,
                    "exception": None,
                },
            )

        engine.reconcile_task = fake_reconcile  # type: ignore[attr-defined]
        cmd = _make_command(
            action="reconcile_task",
            target={"engine": "fake", "task_id": "stuck-1"},
        )
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result_frames = [e for e in entries if e.kind == "command_result"]
        assert len(result_frames) == 1
        parsed = _decode_frame(result_frames[0].payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["engine_state"] == "success"
        assert parsed["payload"]["result"]["task_id"] == "stuck-1"

    async def test_reconcile_task_missing_method_returns_unknown(
        self, dispatcher: CommandDispatcher, buf: BufferStore,
    ) -> None:
        # FakeEngine doesn't ship with reconcile_task - the dispatcher
        # should detect the absence and return engine_state="unknown"
        # without crashing.
        cmd = _make_command(
            action="reconcile_task",
            target={"engine": "fake", "task_id": "x"},
        )
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result_frames = [e for e in entries if e.kind == "command_result"]
        parsed = _decode_frame(result_frames[0].payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["engine_state"] == "unknown"

    async def test_submit_task_routes_to_adapter(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # Add submit_task to FakeEngine for this test (the universal
        # primitive every adapter declares from v1.0+).
        engine._capabilities.add("submit_task")
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None,
                              eta=None, priority=None):
            engine.submit_calls.append((name, args, kwargs, queue))  # type: ignore[attr-defined]
            return CommandResult(
                status="success",
                result={"task_id": f"new-{name}", "engine": "fake"},
            )

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

        cmd = _make_command(
            action="submit_task",
            target={"engine": "fake"},
            parameters={
                "name": "myapp.send_email",
                "args": ["alice@example.com"],
                "kwargs": {"template": "welcome"},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.submit_calls == [  # type: ignore[attr-defined]
            ("myapp.send_email", ("alice@example.com",), {"template": "welcome"}, None),
        ]
        entries = buf.drain(10)
        result = next(e for e in entries if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["task_id"] == "new-myapp.send_email"

    async def test_restart_worker_native_path_when_capability_present(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        # FakeEngine already advertises restart_worker → the native
        # adapter method runs (no self-exit polyfill).
        cmd = _make_command(
            action="restart_worker",
            target={"engine": "fake", "id": "celery@hostA"},
        )
        await dispatcher.handle(cmd)
        assert engine.restart_calls == ["celery@hostA"]

    async def test_restart_worker_refused_without_supervisor(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore, monkeypatch,
    ) -> None:
        # Strip native restart + force "no orchestrator detected".
        engine._capabilities.discard("restart_worker")
        monkeypatch.setenv("Z4J_ORCHESTRATED", "0")

        cmd = _make_command(
            action="restart_worker",
            target={"engine": "fake"},
            parameters={"worker_name": "bare-shell-rq"},
        )
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result = next(e for e in entries if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        assert "supervisor" in parsed["payload"]["error"]
        assert "Z4J_ORCHESTRATED" in parsed["payload"]["error"]
        # No event_batch and no exit scheduled - worker stays alive.
        assert not any(e.kind == "event_batch" for e in entries)

    async def test_restart_worker_self_exit_polyfill(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore, monkeypatch,
    ) -> None:
        # Strip restart_worker from caps to simulate huey/arq/etc.
        engine._capabilities.discard("restart_worker")

        # Force orchestration detection by patching the imported
        # symbol in the dispatcher module. Env-var-only no longer
        # passes after the H2 fix (requires a filesystem marker).
        from z4j_bare import dispatcher as _dispatcher_mod
        from z4j_bare.orchestrator_detect import OrchestratorDetection

        monkeypatch.setattr(
            _dispatcher_mod, "detect_orchestrator",
            lambda: OrchestratorDetection(True, "test-injected"),
        )
        # Bypass the flap guard by backdating the process start.
        dispatcher._process_start_monotonic -= 120

        # Replace os._exit so the test process survives.
        exit_calls: list[int] = []
        import os as _os
        monkeypatch.setattr(_os, "_exit", lambda code: exit_calls.append(code))
        monkeypatch.setattr(
            CommandDispatcher, "_RESTART_EXIT_DELAY", 0.0,
        )

        cmd = _make_command(
            action="restart_worker",
            target={"engine": "fake"},
            parameters={"worker_name": "rq-worker-1"},
        )
        await dispatcher.handle(cmd)

        # Let the call_later(0, os._exit, 0) callback fire.
        import asyncio as _aio
        await _aio.sleep(0)
        await _aio.sleep(0)

        entries = buf.drain(20)
        kinds = [e.kind for e in entries]
        assert "command_result" in kinds
        assert "event_batch" in kinds
        result = next(e for e in entries if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["restarted_via"] == "self_exit"
        assert parsed["payload"]["result"]["worker_name"] == "rq-worker-1"

        event = next(e for e in entries if e.kind == "event_batch")
        parsed_evt = _decode_frame(event.payload)
        evt0 = parsed_evt["payload"]["events"][0]
        assert evt0["kind"] == "worker.offline"
        assert evt0["data"]["reason"] == "restart"
        assert evt0["data"]["worker_name"] == "rq-worker-1"
        assert exit_calls == [0]

    async def test_retry_polyfills_to_submit_task_when_no_native(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # Strip retry_task from caps + add submit_task to simulate a
        # huey/arq/taskiq-like adapter that lacks native retry.
        engine._capabilities.discard("retry_task")
        engine._capabilities.add("submit_task")
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None,
                              eta=None, priority=None):
            engine.submit_calls.append((name, args, kwargs))  # type: ignore[attr-defined]
            return CommandResult(
                status="success",
                result={"task_id": "polyfill-id", "engine": "fake"},
            )

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

        # Brain forwards the original task name + args alongside the
        # retry_task action - the dispatcher reroutes to submit_task.
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "old-id"},
            parameters={
                "task_name": "myapp.flaky",
                "args": [1, 2],
                "kwargs": {"flag": True},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.submit_calls == [  # type: ignore[attr-defined]
            ("myapp.flaky", (1, 2), {"flag": True}),
        ]
        entries = buf.drain(10)
        result = next(e for e in entries if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["task_id"] == "polyfill-id"

    async def test_bulk_retry(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="bulk_retry",
            target={"engine": "fake"},
            parameters={"filter": {"state": "failure"}, "max": 500},
        )
        await dispatcher.handle(cmd)
        assert engine.bulk_calls == [({"state": "failure"}, 500)]

    async def test_bulk_retry_forwards_per_task_overrides_in_filter(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        """R7 H-2: per-task overrides ride inside ``filter["overrides"]``
        (a {task_id: {args, kwargs}} map populated by the brain). The
        dispatcher MUST pass the filter through verbatim so the action
        sees every override the brain captured. This is the bulk-retry
        side of the H-2 thread-through contract."""
        overrides = {
            "j1": {"args": [1], "kwargs": {"a": 1}},
            "j2": {"args": [2], "kwargs": {"a": 2}},
        }
        cmd = _make_command(
            action="bulk_retry",
            target={"engine": "fake"},
            parameters={
                "filter": {
                    "state": "failure",
                    "task_ids": ["j1", "j2"],
                    "overrides": overrides,
                },
                "max": 100,
            },
        )
        await dispatcher.handle(cmd)
        # Filter must arrive at the adapter byte-for-byte; the action
        # layer relies on filter["overrides"] for its pickle-safety
        # refusal.
        forwarded_filter, forwarded_max = engine.bulk_calls[0]
        assert forwarded_max == 100
        assert forwarded_filter["overrides"] == overrides
        assert forwarded_filter["task_ids"] == ["j1", "j2"]

    async def test_bulk_retry_batch_wide_override_fallback_on_old_adapter(
        self, buf: BufferStore,
    ) -> None:
        """The dispatcher forwards batch-wide ``override_args`` /
        ``override_kwargs`` to ``adapter.bulk_retry`` when the brain
        sets them. If the installed adapter is older (signature only
        accepts ``filter`` + ``max``) the TypeError must trigger a
        clean fall-back to the bare call; the bulk retry must still
        proceed, not fail."""

        class OldBulkEngine(FakeEngine):
            async def bulk_retry(
                self, filter: dict, *, max: int = 1000,  # noqa: A002
            ) -> CommandResult:
                # No override_args / override_kwargs kwargs - old shape.
                self.bulk_calls.append((filter, max))
                return CommandResult(status="success", result={"retried": 0})

        old_engine = OldBulkEngine()
        d = CommandDispatcher(
            engines={"fake": old_engine}, schedulers={}, buffer=buf,
        )
        cmd = _make_command(
            action="bulk_retry",
            target={"engine": "fake"},
            parameters={
                "filter": {"task_ids": ["j1"]},
                "max": 10,
                "override_args": [42],
                "override_kwargs": {"forced": True},
            },
        )
        await d.handle(cmd)
        # Old adapter still received the call (via fallback).
        assert old_engine.bulk_calls == [({"task_ids": ["j1"]}, 10)]
        # And the result frame reports success, not a 'unexpected
        # keyword' crash.
        result_frames = [
            e for e in buf.drain(10) if e.kind == "command_result"
        ]
        parsed = _decode_frame(result_frames[0].payload)
        assert parsed["payload"]["status"] == "success"

    async def test_purge_queue(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="purge_queue",
            target={"engine": "fake", "queue": "emails"},
        )
        await dispatcher.handle(cmd)
        assert engine.purge_calls == ["emails"]

    async def test_requeue_dead_letter(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "fake", "task_id": "abc"},
        )
        await dispatcher.handle(cmd)
        # FakeEngine's old-shape signature triggers the TypeError
        # fallback path; the task_id still lands in dlq_calls.
        assert engine.dlq_calls == ["abc"]

    async def test_requeue_dead_letter_threads_overrides_to_new_adapter(
        self, buf: BufferStore,
    ) -> None:
        """R7 H-2 (DLQ fallback): when an adapter advertises the H-2
        overload (override_args / override_kwargs kwargs) the dispatcher
        must thread brain-supplied overrides through so the rq DLQ
        fallback path (delegates to retry_task_action under the hood)
        receives operator-vetted inputs instead of unpickling broker
        bytes."""

        class NewDLQEngine(FakeEngine):
            def __init__(self) -> None:
                super().__init__()
                self.dlq_kwargs_calls: list[tuple] = []

            async def requeue_dead_letter(
                self,
                task_id: str,
                *,
                override_args: tuple | None = None,
                override_kwargs: dict | None = None,
            ) -> CommandResult:
                self.dlq_kwargs_calls.append(
                    (task_id, override_args, override_kwargs),
                )
                return CommandResult(status="success")

        new_engine = NewDLQEngine()
        d = CommandDispatcher(
            engines={"fake": new_engine}, schedulers={}, buffer=buf,
        )
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "fake", "task_id": "dead-1"},
            parameters={
                "args": [7, 8],
                "kwargs": {"reason": "from-dlq"},
            },
        )
        await d.handle(cmd)
        assert new_engine.dlq_kwargs_calls == [
            ("dead-1", (7, 8), {"reason": "from-dlq"}),
        ]

    async def test_restart_worker(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="restart_worker",
            target={"engine": "fake", "worker_name": "celery@w1"},
        )
        await dispatcher.handle(cmd)
        assert engine.restart_calls == ["celery@w1"]


class TestUnknownEngine:
    async def test_unknown_engine_fails_cleanly(
        self, dispatcher: CommandDispatcher, buf: BufferStore,
    ) -> None:
        cmd = _make_command(
            action="retry_task",
            target={"engine": "ghost", "task_id": "abc"},
        )
        await dispatcher.handle(cmd)
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"
        assert "ghost" in (parsed["payload"]["error"] or "")


class TestScheduleActions:
    async def test_enable_schedule(
        self, dispatcher: CommandDispatcher, scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.enable",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.enable_calls == ["sched-1"]

    async def test_disable_schedule(
        self, dispatcher: CommandDispatcher, scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.disable",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.disable_calls == ["sched-1"]

    async def test_trigger_now(
        self, dispatcher: CommandDispatcher, scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.trigger_now",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.trigger_calls == ["sched-1"]

    async def test_delete_schedule(
        self, dispatcher: CommandDispatcher, scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.delete",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.delete_calls == ["sched-1"]

    async def test_missing_schedule_id_fails(
        self, dispatcher: CommandDispatcher, buf: BufferStore,
    ) -> None:
        cmd = _make_command(
            action="schedule.enable",
            target={"scheduler": "celery-beat"},
        )
        await dispatcher.handle(cmd)
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"


class TestScheduleResync:
    """``schedule.resync`` calls the runtime-supplied resync callback
    and reports the count back via the CommandResult payload.

    Added in 1.3.3. The brain dispatches this when an operator
    clicks *Sync now* on the Schedules page. The dispatcher itself
    doesn't know how to drain SchedulerAdapter.list_schedules, it
    delegates to the callback the runtime injects at construction.
    """

    async def test_resync_invokes_callback_and_reports_count(
        self, buf: BufferStore, engine: FakeEngine, scheduler: FakeScheduler,
    ) -> None:
        calls: list[str] = []

        async def fake_resync(reason: str) -> int:
            calls.append(reason)
            return 2  # pretend two scheduler adapters drained

        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={"celery-beat": scheduler},
            buffer=buf,
            resync_schedules=fake_resync,
        )
        cmd = _make_command(action="schedule.resync", target={})
        await dispatcher.handle(cmd)

        assert calls == ["command"]
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        assert len(results) == 1
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"] == {"schedulers_drained": 2}

    async def test_resync_without_callback_fails_with_clear_message(
        self, buf: BufferStore, engine: FakeEngine, scheduler: FakeScheduler,
    ) -> None:
        """A dispatcher built without ``resync_schedules`` (e.g. an
        old runtime, a hand-built one in tests, or a future op
        deciding to disable the feature) must NOT crash on
        ``schedule.resync``, it must return a clean ``failed`` result
        with a message that points at the upgrade path."""
        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={"celery-beat": scheduler},
            buffer=buf,
            # no resync_schedules
        )
        cmd = _make_command(action="schedule.resync", target={})
        await dispatcher.handle(cmd)

        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"
        assert "1.3.1" in parsed["payload"]["error"]

    async def test_resync_callback_exception_becomes_failed_result(
        self, buf: BufferStore, engine: FakeEngine, scheduler: FakeScheduler,
    ) -> None:
        async def boom(reason: str) -> int:
            raise RuntimeError("boom")

        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={"celery-beat": scheduler},
            buffer=buf,
            resync_schedules=boom,
        )
        cmd = _make_command(action="schedule.resync", target={})
        await dispatcher.handle(cmd)

        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"
        assert "RuntimeError" in parsed["payload"]["error"]
        assert "boom" in parsed["payload"]["error"]


class TestUnrecognizedAction:
    async def test_unknown_action_fails_cleanly(
        self, dispatcher: CommandDispatcher, buf: BufferStore,
    ) -> None:
        cmd = _make_command(
            action="do_magic",
            target={"engine": "fake"},
        )
        await dispatcher.handle(cmd)
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"


class TestScheduleFire:
    """Regression tests for the v1.1.0 ``schedule.fire`` dispatcher fix.

    Pre-1.1 every brain-side scheduler tick produced a ``command.failed``
    audit row with one of two errors:
      - ``unrecognized schedule action 'schedule.fire'`` (the
        ``_dispatch_scheduler`` switch had no ``fire`` handler), or
      - ``no scheduler adapter registered for None`` (a Celery WORKER
        agent doesn't have a SchedulerAdapter, celery-beat is a
        separate process).
    Both modes were observed in docker on 2026-04-28. Fix: route
    ``schedule.fire`` to the QueueEngineAdapter's ``submit_task``
    using the task payload the brain already populated.
    """

    async def test_schedule_fire_routes_to_engine_submit_task(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None,
                              eta=None, priority=None):  # noqa: ARG001
            engine.submit_calls.append((name, args, kwargs, queue))  # type: ignore[attr-defined]
            return CommandResult(
                status="success",
                result={"task_id": f"new-{name}"},
            )

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

        cmd = _make_command(
            action="schedule.fire",
            target={"id": "sched-uuid"},
            parameters={
                "schedule_id": "sched-uuid",
                "schedule_name": "nightly-cleanup",
                "task_name": "myapp.tasks.cleanup",
                "engine": "fake",
                "queue": "default",
                "args": ["arg1"],
                "kwargs": {"k": "v"},
                "fire_id": "fire-uuid",
            },
        )
        await dispatcher.handle(cmd)

        assert engine.submit_calls == [  # type: ignore[attr-defined]
            ("myapp.tasks.cleanup", ("arg1",), {"k": "v"}, "default"),
        ]
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "success"

    async def test_schedule_fire_works_without_scheduler_adapter(
        self, buf: BufferStore, engine: FakeEngine,
    ) -> None:
        """Celery worker agent has zero SchedulerAdapters, must still fire."""
        engine.submit_called = False  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None,  # noqa: ARG001
                              queue=None, eta=None, priority=None):
            engine.submit_called = True  # type: ignore[attr-defined]
            return CommandResult(status="success", result={"task_id": "ok"})

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

        # No schedulers={}, exactly the celery-worker shape.
        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={},
            buffer=buf,
        )

        cmd = _make_command(
            action="schedule.fire",
            target={},
            parameters={
                "task_name": "t",
                "engine": "fake",
                "args": [],
                "kwargs": {},
            },
        )
        await dispatcher.handle(cmd)

        assert engine.submit_called is True  # type: ignore[attr-defined]
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "success"

    async def test_schedule_fire_falls_back_to_sole_engine(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        """If payload omits ``engine``, dispatch to the only registered one."""
        engine.submit_called = False  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None,  # noqa: ARG001
                              queue=None, eta=None, priority=None):
            engine.submit_called = True  # type: ignore[attr-defined]
            return CommandResult(status="success", result={"task_id": "ok"})

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

        cmd = _make_command(
            action="schedule.fire",
            target={},
            parameters={"task_name": "t"},  # no engine, no args/kwargs
        )
        await dispatcher.handle(cmd)
        assert engine.submit_called is True  # type: ignore[attr-defined]

    async def test_schedule_fire_missing_task_name_fails_cleanly(
        self, dispatcher: CommandDispatcher, buf: BufferStore,
    ) -> None:
        cmd = _make_command(
            action="schedule.fire",
            target={},
            parameters={"engine": "fake"},  # no task_name
        )
        await dispatcher.handle(cmd)
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"
        assert "task_name" in (parsed["payload"]["error"] or "")


class TestCapabilityGating:
    async def test_action_rejected_if_not_in_capabilities(
        self, buf: BufferStore,
    ) -> None:
        class LimitedEngine(FakeEngine):
            def capabilities(self) -> set[str]:
                return {"retry_task"}  # only retry

        engine = LimitedEngine()
        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="purge_queue",
            target={"engine": "fake", "queue": "q"},
        )
        await dispatcher.handle(cmd)
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"
        assert "not support" in (parsed["payload"]["error"] or "")


# ---------------------------------------------------------------------------
# R8 H-1 and R8 L-1: task_name thread-through across the dispatcher
# ---------------------------------------------------------------------------


class FakeEngineWithTaskName(FakeEngine):
    """Adapter that accepts the 1.6.7 ``task_name`` retry kwarg.

    Models the post-R8-H1 z4j-rq adapter shape so we can assert the
    dispatcher actually threads the brain-supplied task_name through
    rather than silently dropping it on the TypeError fallback path.
    """

    name = "fake_with_task_name"

    def __init__(self) -> None:
        super().__init__()
        self.task_name_calls: list[str | None] = []

    async def retry_task(  # type: ignore[override]
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        override_args: tuple | None = None,
        override_kwargs: dict | None = None,
        eta: float | None = None,
        priority: object = None,  # noqa: ARG002
    ) -> CommandResult:
        self.retry_calls.append((task_id, override_args, override_kwargs, eta))
        self.task_name_calls.append(task_name)
        return CommandResult(status="success", result={"new_task_id": f"new-{task_id}"})

    async def requeue_dead_letter(  # type: ignore[override]
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        override_args: tuple | None = None,
        override_kwargs: dict | None = None,
    ) -> CommandResult:
        self.dlq_calls.append(task_id)
        self.task_name_calls.append(task_name)
        return CommandResult(status="success")


class FakeHueyEngine(FakeEngine):
    """Adapter advertising ``name = "huey"`` so the dispatcher injects
    ``__z4j_task_name__`` into ``override_kwargs`` per R8 L-1."""

    name = "huey"

    def __init__(self) -> None:
        super().__init__()
        # Capture override_kwargs that reach the adapter so the test
        # can assert __z4j_task_name__ landed there.
        self.last_override_kwargs: dict | None = None

    async def retry_task(  # type: ignore[override]
        self,
        task_id: str,
        *,
        override_args: tuple | None = None,
        override_kwargs: dict | None = None,
        eta: float | None = None,
        priority: object = None,  # noqa: ARG002
    ) -> CommandResult:
        self.retry_calls.append((task_id, override_args, override_kwargs, eta))
        self.last_override_kwargs = (
            dict(override_kwargs) if override_kwargs is not None else None
        )
        return CommandResult(status="success", result={"new_task_id": f"new-{task_id}"})


class TestR8H1TaskNameThreadThrough:
    """Dispatcher MUST forward the brain-supplied task_name when the
    adapter signature accepts it (post-1.6.7 z4j-rq shape)."""

    async def test_retry_task_threads_task_name_to_modern_adapter(
        self, buf: BufferStore,
    ) -> None:
        engine = FakeEngineWithTaskName()
        dispatcher = CommandDispatcher(
            engines={"fake_with_task_name": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake_with_task_name", "task_id": "xyz"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.task_name_calls == ["myapp.tasks.send_email"]

    async def test_retry_task_falls_back_for_legacy_adapter(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        """FakeEngine doesn't accept task_name= kwarg. The dispatcher
        catches the TypeError and falls back to the legacy signature
        so a brand-new brain doesn't break a pinned older agent. The
        action-layer fail-closed check is what preserves the security
        guarantee when adapters lag behind."""
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "legacy-1"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        # Fallback path lands the retry without task_name; tuple still
        # records the call so we know it executed (vs raising).
        task_ids = [c[0] for c in engine.retry_calls]
        assert "legacy-1" in task_ids

    async def test_dlq_threads_task_name_to_modern_adapter(
        self, buf: BufferStore,
    ) -> None:
        engine = FakeEngineWithTaskName()
        dispatcher = CommandDispatcher(
            engines={"fake_with_task_name": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "fake_with_task_name", "task_id": "dlq-1"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.task_name_calls == ["myapp.tasks.send_email"]


class TestR8L1HueyInjection:
    """Dispatcher injects ``__z4j_task_name__`` into override_kwargs
    for adapters with name == 'huey'. R8 L-1 regression."""

    async def test_huey_retry_receives_magic_key_in_override_kwargs(
        self, buf: BufferStore,
    ) -> None:
        engine = FakeHueyEngine()
        dispatcher = CommandDispatcher(
            engines={"huey": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="retry_task",
            target={"engine": "huey", "task_id": "huey-1"},
            parameters={
                "task_name": "myapp.tasks.send_sms",
                "override_args": [],
                "override_kwargs": {"existing": "value"},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.last_override_kwargs is not None
        # Magic key landed at the path the Huey engine pops from.
        assert engine.last_override_kwargs.get("__z4j_task_name__") == (
            "myapp.tasks.send_sms"
        )
        # Existing operator-supplied kwarg preserved.
        assert engine.last_override_kwargs.get("existing") == "value"

    async def test_huey_retry_does_not_clobber_operator_supplied_magic_key(
        self, buf: BufferStore,
    ) -> None:
        """If the operator already supplied a __z4j_task_name__ in
        override_kwargs (unusual but possible), the dispatcher must
        not overwrite it. ``setdefault`` is the correct shape."""
        engine = FakeHueyEngine()
        dispatcher = CommandDispatcher(
            engines={"huey": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="retry_task",
            target={"engine": "huey", "task_id": "huey-2"},
            parameters={
                "task_name": "myapp.brain.choice",
                "override_args": [],
                "override_kwargs": {"__z4j_task_name__": "myapp.operator.choice"},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.last_override_kwargs is not None
        assert engine.last_override_kwargs["__z4j_task_name__"] == (
            "myapp.operator.choice"
        )

    async def test_non_huey_adapter_does_not_get_magic_key_injection(
        self, dispatcher: CommandDispatcher, engine: FakeEngine,
    ) -> None:
        """The injection is Huey-specific. Other adapters must NOT
        receive an unexpected __z4j_task_name__ key in their
        override_kwargs (would otherwise reach the user function)."""
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "non-huey-1"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {"user_key": "value"},
            },
        )
        await dispatcher.handle(cmd)
        _, _, kwargs, _ = engine.retry_calls[0]
        assert kwargs == {"user_key": "value"}
        assert "__z4j_task_name__" not in (kwargs or {})
