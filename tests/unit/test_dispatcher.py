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
        assert engine.dlq_calls == ["abc"]

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
