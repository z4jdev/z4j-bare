"""Unit tests for ``z4j_bare.dispatcher.CommandDispatcher``.

Uses fake engine + scheduler adapters so we don't need a real Celery
installation to exercise the routing logic.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Schedule,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.transport.frames import (
    CommandFrame,
    CommandPayload,
)


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
        self,
        hints: DiscoveryHints | None = None,
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

    async def get_task(self, task_id: str) -> Task | None:
        return None

    async def retry_task(
        self,
        task_id: str,
        *,
        override_args: tuple | None = None,
        override_kwargs: dict | None = None,
        eta: float | None = None,
        priority: object = None,
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

    async def bulk_retry(self, filter: dict, *, max: int = 1000) -> CommandResult:  # noqa: A002  mirrors QueueEngineAdapter.bulk_retry signature
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

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        return None

    async def create_schedule(self, spec: Schedule) -> Schedule:
        return spec

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
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

    async def test_delivery_claim_token_is_echoed_on_every_fire_exit(
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
    ) -> None:
        def command(
            command_id: str,
            token: str,
            *,
            fire_id: str = "fire-claim-token",
        ) -> CommandFrame:
            return CommandFrame(
                id=command_id,
                payload=CommandPayload(
                    action="schedule.fire",
                    target={"id": "schedule-1"},
                    parameters={
                        "task_name": "jobs.cleanup",
                        "engine": "fake",
                        "fire_id": fire_id,
                    },
                    delivery_claim_token=token,
                ),
                hmac="deadbeef" * 8,
            )

        async def handle_and_decode(
            command_id: str,
            token: str,
            *,
            fire_id: str = "fire-claim-token",
        ) -> list[dict[str, Any]]:
            await dispatcher.handle(
                command(command_id, token, fire_id=fire_id),
            )
            return [
                decoded
                for row in buf.drain(20)
                if (decoded := _decode_frame(row.payload))["id"] == command_id
            ]

        ordinary = await handle_and_decode(
            "cmd-ordinary",
            "11111111-1111-4111-8111-111111111111",
        )
        assert {row["payload"]["delivery_claim_token"] for row in ordinary} == {
            "11111111-1111-4111-8111-111111111111"
        }

        dispatcher._inflight_keys.add("fire:fire-memory-claim-token")
        in_memory = await handle_and_decode(
            "cmd-memory",
            "22222222-2222-4222-8222-222222222222",
            fire_id="fire-memory-claim-token",
        )
        dispatcher._inflight_keys.discard("fire:fire-memory-claim-token")
        assert [row["type"] for row in in_memory] == ["command_ack"]
        assert (
            in_memory[0]["payload"]["delivery_claim_token"]
            == "22222222-2222-4222-8222-222222222222"
        )

        dispatcher._seen_commands.clear()
        durable = await handle_and_decode(
            "cmd-durable",
            "33333333-3333-4333-8333-333333333333",
        )
        assert {row["type"] for row in durable} == {
            "command_ack",
            "command_result",
        }
        assert {row["payload"]["delivery_claim_token"] for row in durable} == {
            "33333333-3333-4333-8333-333333333333"
        }


class TestRetryTask:
    async def test_retry_happy_path(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
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
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
    ) -> None:
        cmd = _make_command(action="retry_task", target={"engine": "fake"})
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result = next(e for e in entries if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        assert "task_id" in (parsed["payload"]["error"] or "")

    async def test_single_engine_default_engine(
        self,
        buf: BufferStore,
        engine: FakeEngine,
    ) -> None:
        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(action="retry_task", target={"task_id": "abc"})
        await dispatcher.handle(cmd)
        assert engine.retry_calls == [("abc", None, None, None)]

    async def test_retry_ignores_brain_snapshot_args_kwargs_rh1(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        """RH1 (direction 2): the brain stores task args/kwargs REDACTED, so the
        dispatcher must NEVER forward the brain snapshot as overrides -- doing so
        re-runs the task with scrubbed values (the literal "[REDACTED]"), and an
        N-1 1.7.0 brain still sends that snapshot. Without operator overrides the
        adapter receives None and re-runs the ORIGINAL job by reference (the H-2
        pickle path is closed by the adapter's by-reference retry / fail-closed,
        not by replaying brain-stored values).
        """
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "snap-1"},
            parameters={
                # No operator overrides; only the (redacted) brain snapshot.
                "task_name": "myapp.do_thing",
                "args": [10, 20],
                "kwargs": {"flag": True},
            },
        )
        await dispatcher.handle(cmd)
        task_id, args, kwargs, _ = engine.retry_calls[0]
        assert task_id == "snap-1"
        # Snapshot args/kwargs must NOT reach the adapter.
        assert args is None
        assert kwargs is None

    async def test_retry_operator_override_beats_brain_snapshot(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
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

    async def test_retry_snapshot_kwargs_only_are_ignored_rh1(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        """RH1: even a kwargs-only brain snapshot is ignored (it is redacted).
        The adapter receives None and retries by reference."""
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "snap-3"},
            parameters={"kwargs": {"only_kwargs": "yes"}},
        )
        await dispatcher.handle(cmd)
        _, args, kwargs, _ = engine.retry_calls[0]
        assert args is None
        assert kwargs is None


class TestCancelAndOthers:
    async def test_cancel(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="cancel_task",
            target={"engine": "fake", "task_id": "abc"},
        )
        await dispatcher.handle(cmd)
        assert engine.cancel_calls == ["abc"]

    async def test_reconcile_task_calls_adapter_method(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
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
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # Add submit_task to FakeEngine for this test (the universal
        # primitive every adapter declares from v1.0+).
        engine._capabilities.add("submit_task")
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
        monkeypatch,
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
        monkeypatch,
    ) -> None:
        # Strip restart_worker from caps to simulate huey/arq/etc.
        engine._capabilities.discard("restart_worker")

        # Force orchestration detection by patching the imported
        # symbol in the dispatcher module. Env-var-only no longer
        # passes after the H2 fix (requires a filesystem marker).
        from z4j_bare import dispatcher as _dispatcher_mod
        from z4j_bare.orchestrator_detect import OrchestratorDetection

        monkeypatch.setattr(
            _dispatcher_mod,
            "detect_orchestrator",
            lambda: OrchestratorDetection(True, "test-injected"),
        )
        # Bypass the flap guard by backdating the process start.
        dispatcher._process_start_monotonic -= 120

        # Replace os._exit so the test process survives.
        exit_calls: list[int] = []
        import os as _os

        monkeypatch.setattr(_os, "_exit", exit_calls.append)
        monkeypatch.setattr(
            CommandDispatcher,
            "_RESTART_EXIT_DELAY",
            0.0,
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

    async def test_retry_polyfill_fails_closed_without_override(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # Strip retry_task from caps + add submit_task to simulate a
        # huey/arq/taskiq-like adapter that lacks native retry. 1.7.1 (H1):
        # the brain stores task args REDACTED and forwards args=None, so a
        # no-override polyfill retry MUST fail closed rather than re-submit
        # with empty/wrong inputs. The ``args``/``kwargs`` snapshot in the
        # payload is ignored -- only explicit overrides are trusted.
        engine._capabilities.discard("retry_task")
        engine._capabilities.add("submit_task")
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            engine.submit_calls.append((name, args, kwargs))  # type: ignore[attr-defined]
            return CommandResult(
                status="success",
                result={"task_id": "polyfill-id", "engine": "fake"},
            )

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

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
        # submit_task was NEVER called -- the retry failed closed.
        assert engine.submit_calls == []  # type: ignore[attr-defined]
        entries = buf.drain(10)
        result = next(e for e in entries if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        assert "override" in parsed["payload"]["error"].lower()

    async def test_retry_polyfill_uses_explicit_overrides(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # With explicit operator overrides the polyfill re-submits with THOSE
        # (the operator is the authority on the retry inputs).
        engine._capabilities.discard("retry_task")
        engine._capabilities.add("submit_task")
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            engine.submit_calls.append((name, args, kwargs))  # type: ignore[attr-defined]
            return CommandResult(
                status="success",
                result={"task_id": "polyfill-id", "engine": "fake"},
            )

        engine.submit_task = fake_submit  # type: ignore[attr-defined]

        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "old-id"},
            parameters={
                "task_name": "myapp.flaky",
                "override_args": [1, 2],
                "override_kwargs": {"flag": True},
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="bulk_retry",
            target={"engine": "fake"},
            parameters={"filter": {"state": "failure"}, "max": 500},
        )
        await dispatcher.handle(cmd)
        assert engine.bulk_calls == [({"state": "failure"}, 500)]

    async def test_bulk_retry_forwards_per_task_overrides_in_filter(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        """Per-task overrides ride inside ``filter["overrides"]``
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
        self,
        buf: BufferStore,
    ) -> None:
        """The dispatcher forwards batch-wide ``override_args`` /
        ``override_kwargs`` to ``adapter.bulk_retry`` when the brain
        sets them. If the installed adapter is older (signature only
        accepts ``filter`` + ``max``) the TypeError must trigger a
        clean fall-back to the bare call; the bulk retry must still
        proceed, not fail."""

        class OldBulkEngine(FakeEngine):
            async def bulk_retry(
                self,
                filter: dict,  # noqa: A002  mirrors QueueEngineAdapter.bulk_retry signature
                *,
                max: int = 1000,  # noqa: A002
            ) -> CommandResult:
                # No override_args / override_kwargs kwargs - old shape.
                self.bulk_calls.append((filter, max))
                return CommandResult(status="success", result={"retried": 0})

        old_engine = OldBulkEngine()
        d = CommandDispatcher(
            engines={"fake": old_engine},
            schedulers={},
            buffer=buf,
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
        result_frames = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(result_frames[0].payload)
        assert parsed["payload"]["status"] == "success"

    async def test_purge_queue(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="purge_queue",
            target={"engine": "fake", "queue": "emails"},
        )
        await dispatcher.handle(cmd)
        assert engine.purge_calls == ["emails"]

    async def test_requeue_dead_letter(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "fake", "task_id": "abc"},
        )
        await dispatcher.handle(cmd)
        # FakeEngine's old-shape signature triggers the TypeError
        # fallback path; the task_id still lands in dlq_calls.
        assert engine.dlq_calls == ["abc"]

    class _NewDLQEngine(FakeEngine):
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
            self.dlq_kwargs_calls.append((task_id, override_args, override_kwargs))
            return CommandResult(status="success")

    async def test_requeue_dead_letter_ignores_brain_snapshot_rh1(
        self,
        buf: BufferStore,
    ) -> None:
        """RH1 (direction 2): the DLQ-fallback path must IGNORE the brain's
        redacted args/kwargs snapshot -- executing it re-runs the DLQ task with
        scrubbed values (an N-1 1.7.0 brain still sends it). Without operator
        overrides the adapter resurrects the ORIGINAL message by reference."""
        new_engine = self._NewDLQEngine()
        d = CommandDispatcher(engines={"fake": new_engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "fake", "task_id": "dead-1"},
            parameters={"args": [7, 8], "kwargs": {"reason": "from-dlq"}},
        )
        await d.handle(cmd)
        assert new_engine.dlq_kwargs_calls == [("dead-1", None, None)]

    async def test_requeue_dead_letter_threads_operator_overrides_rh1(
        self,
        buf: BufferStore,
    ) -> None:
        """The legitimate path: OPERATOR-supplied overrides ARE threaded to the
        DLQ adapter (the explicit 'retry with different inputs' surface)."""
        new_engine = self._NewDLQEngine()
        d = CommandDispatcher(engines={"fake": new_engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "fake", "task_id": "dead-2"},
            parameters={"override_args": [1], "override_kwargs": {"op": True}},
        )
        await d.handle(cmd)
        assert new_engine.dlq_kwargs_calls == [("dead-2", (1,), {"op": True})]

    async def test_restart_worker(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        cmd = _make_command(
            action="restart_worker",
            target={"engine": "fake", "worker_name": "celery@w1"},
        )
        await dispatcher.handle(cmd)
        assert engine.restart_calls == ["celery@w1"]


class TestUnknownEngine:
    async def test_unknown_engine_fails_cleanly(
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
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
        self,
        dispatcher: CommandDispatcher,
        scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.enable",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.enable_calls == ["sched-1"]

    async def test_disable_schedule(
        self,
        dispatcher: CommandDispatcher,
        scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.disable",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.disable_calls == ["sched-1"]

    async def test_trigger_now(
        self,
        dispatcher: CommandDispatcher,
        scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.trigger_now",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.trigger_calls == ["sched-1"]

    async def test_delete_schedule(
        self,
        dispatcher: CommandDispatcher,
        scheduler: FakeScheduler,
    ) -> None:
        cmd = _make_command(
            action="schedule.delete",
            target={"scheduler": "celery-beat", "schedule_id": "sched-1"},
        )
        await dispatcher.handle(cmd)
        assert scheduler.delete_calls == ["sched-1"]

    async def test_missing_schedule_id_fails(
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
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
        self,
        buf: BufferStore,
        engine: FakeEngine,
        scheduler: FakeScheduler,
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
        self,
        buf: BufferStore,
        engine: FakeEngine,
        scheduler: FakeScheduler,
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
        self,
        buf: BufferStore,
        engine: FakeEngine,
        scheduler: FakeScheduler,
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


class TestExternalScheduleActivationBoundaryD:
    async def test_activation_routes_to_runtime_callback(
        self,
        buf: BufferStore,
        engine: FakeEngine,
        scheduler: FakeScheduler,
    ) -> None:
        calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

        async def activate(
            target: dict[str, Any],
            parameters: dict[str, Any],
        ) -> dict[str, object]:
            calls.append((target, parameters))
            return {"sequence": 1, "stream_id": parameters["stream_id"]}

        dispatcher = CommandDispatcher(
            engines={"fake": engine},
            schedulers={"celery-beat": scheduler},
            buffer=buf,
            activate_schedule_stream=activate,
        )
        parameters = {
            "stream_id": "stream-1",
            "epoch_uuid": "epoch-1",
            "epoch_number": 3,
        }
        await dispatcher.handle(
            _make_command(
                action="schedule.external.activate",
                target={"scheduler": "celery-beat"},
                parameters=parameters,
            )
        )

        assert calls == [({"scheduler": "celery-beat"}, parameters)]
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["sequence"] == 1

    async def test_activation_without_callback_fails_closed(
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
    ) -> None:
        await dispatcher.handle(
            _make_command(
                action="schedule.external.activate",
                target={"scheduler": "celery-beat"},
            )
        )
        results = [e for e in buf.drain(10) if e.kind == "command_result"]
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "failed"
        assert "sequenced external schedule protocol" in parsed["payload"]["error"]


class TestUnrecognizedAction:
    async def test_unknown_action_fails_cleanly(
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        engine.submit_calls: list[tuple] = []  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
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
        self,
        buf: BufferStore,
        engine: FakeEngine,
    ) -> None:
        """Celery worker agent has zero SchedulerAdapters, must still fire."""
        engine.submit_called = False  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        """If payload omits ``engine``, dispatch to the only registered one."""
        engine.submit_called = False  # type: ignore[attr-defined]

        async def fake_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
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
        self,
        dispatcher: CommandDispatcher,
        buf: BufferStore,
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
        self,
        buf: BufferStore,
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
# And: task_name thread-through across the dispatcher
# ---------------------------------------------------------------------------


class FakeEngineWithTaskName(FakeEngine):
    """Adapter that accepts the 1.6.7 ``task_name`` retry kwarg.

    Models the post- z4j-rq adapter shape so we can assert the
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
        priority: object = None,
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
    ``__z4j_task_name__`` into ``override_kwargs`` per. Attests the
    1.7.1 safe-retry contract like the real adapter (P1-1)."""

    name = "huey"
    safe_retry_by_reference = True

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
        priority: object = None,
    ) -> CommandResult:
        self.retry_calls.append((task_id, override_args, override_kwargs, eta))
        self.last_override_kwargs = dict(override_kwargs) if override_kwargs is not None else None
        return CommandResult(status="success", result={"new_task_id": f"new-{task_id}"})


class TestR8H1TaskNameThreadThrough:
    """Dispatcher MUST forward the brain-supplied task_name when the
    adapter signature accepts it (post-1.6.7 z4j-rq shape)."""

    async def test_retry_task_threads_task_name_to_modern_adapter(
        self,
        buf: BufferStore,
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
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
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
        self,
        buf: BufferStore,
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


class FakeDramatiqEngine(FakeHueyEngine):
    """Adapter advertising ``name = "dramatiq"``. Like the real dramatiq
    adapter, its retry_task signature does NOT accept ``task_name``; it
    reads the actor name from ``override_kwargs["__z4j_actor_name__"]``."""

    name = "dramatiq"


class TestB5DramatiqActorNameInjection:
    """B5 regression: dramatiq's retry_task sources the actor name ONLY
    from override_kwargs["__z4j_actor_name__"], which nothing wrote, so
    the advertised Dramatiq retry button failed closed. The dispatcher now
    injects it (mirroring the Huey special-case)."""

    async def test_dramatiq_retry_receives_actor_name_magic_key(
        self,
        buf: BufferStore,
    ) -> None:
        engine = FakeDramatiqEngine()
        dispatcher = CommandDispatcher(
            engines={"dramatiq": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="retry_task",
            target={"engine": "dramatiq", "task_id": "dq-1"},
            parameters={
                "task_name": "myapp.actors.process_payment",
                "override_args": [],
                "override_kwargs": {"existing": "value"},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.last_override_kwargs is not None
        assert engine.last_override_kwargs.get("__z4j_actor_name__") == (
            "myapp.actors.process_payment"
        )
        assert engine.last_override_kwargs.get("existing") == "value"


class _OldHueyEngine(FakeHueyEngine):
    """A pre-1.7.1 huey adapter: advertises name == 'huey' but does NOT attest
    the safe-retry contract (no strip of the control key)."""

    safe_retry_by_reference = False


class _OldDramatiqEngine(FakeDramatiqEngine):
    """A pre-1.7.1 dramatiq adapter: no safe-retry attestation."""

    safe_retry_by_reference = False


class TestP1RetryStackAttestation:
    """P1-1: a huey/dramatiq retry must be REFUSED (fail closed) when the loaded
    adapter does not attest the 1.7.1 safe-retry contract. z4j-bare's own version
    does not prove the separately-installed adapter strips the smuggled control
    key, so an un-attesting adapter would re-run the task with empty arguments."""

    async def test_old_huey_adapter_retry_is_refused(self, buf: BufferStore) -> None:
        engine = _OldHueyEngine()
        dispatcher = CommandDispatcher(engines={"huey": engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="retry_task",
            target={"engine": "huey", "task_id": "h-1"},
            parameters={"task_name": "app.t"},  # by-reference retry, no overrides
        )
        await dispatcher.handle(cmd)
        # Refused before ever calling the adapter's retry_task.
        assert engine.retry_calls == []
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        parsed = _decode_frame(result.payload)["payload"]
        assert parsed["status"] == "failed"
        assert "z4j-huey" in (parsed["error"] or "")
        assert "1.7.1" in (parsed["error"] or "")

    async def test_old_dramatiq_adapter_retry_is_refused(self, buf: BufferStore) -> None:
        engine = _OldDramatiqEngine()
        dispatcher = CommandDispatcher(engines={"dramatiq": engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="retry_task",
            target={"engine": "dramatiq", "task_id": "d-1"},
            parameters={"task_name": "app.actor"},
        )
        await dispatcher.handle(cmd)
        assert engine.retry_calls == []
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        parsed = _decode_frame(result.payload)["payload"]
        assert parsed["status"] == "failed"
        assert "z4j-dramatiq" in (parsed["error"] or "")

    async def test_attesting_huey_adapter_retry_proceeds(self, buf: BufferStore) -> None:
        # The 1.7.1 fake DOES attest -> the retry reaches the adapter.
        engine = FakeHueyEngine()
        dispatcher = CommandDispatcher(engines={"huey": engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="retry_task",
            target={"engine": "huey", "task_id": "h-2"},
            parameters={
                "task_name": "app.t",
                "override_args": [],
                "override_kwargs": {"k": "v"},
            },
        )
        await dispatcher.handle(cmd)
        assert [c[0] for c in engine.retry_calls] == ["h-2"]

    async def test_old_dramatiq_adapter_bulk_retry_is_refused_m12(self, buf: BufferStore) -> None:
        # M12: a pre-1.7.1 dramatiq adapter selects a bulk retry by a control key
        # the 1.7.1 brain no longer sends, so it would skip every id and report
        # success -- a silent no-op. Refuse (fail closed) with an upgrade message.
        engine = _OldDramatiqEngine()
        dispatcher = CommandDispatcher(engines={"dramatiq": engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="bulk_retry",
            target={"engine": "dramatiq"},
            parameters={"filter": {"state": "failure"}, "max": 100},
        )
        await dispatcher.handle(cmd)
        assert engine.bulk_calls == []  # refused before the adapter ran
        parsed = _decode_frame(
            next(e for e in buf.drain(10) if e.kind == "command_result").payload
        )["payload"]
        assert parsed["status"] == "failed"
        assert "z4j-dramatiq" in (parsed["error"] or "")
        assert "1.7.1" in (parsed["error"] or "")

    async def test_attesting_dramatiq_adapter_bulk_retry_proceeds_m12(
        self, buf: BufferStore
    ) -> None:
        engine = FakeDramatiqEngine()  # attests safe_retry_by_reference
        dispatcher = CommandDispatcher(engines={"dramatiq": engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="bulk_retry",
            target={"engine": "dramatiq"},
            parameters={"filter": {"state": "failure"}, "max": 100},
        )
        await dispatcher.handle(cmd)
        assert engine.bulk_calls == [({"state": "failure"}, 100)]


class TestR8L1HueyInjection:
    """Dispatcher injects ``__z4j_task_name__`` into override_kwargs
    for adapters with name == 'huey'. regression."""

    async def test_huey_retry_receives_magic_key_in_override_kwargs(
        self,
        buf: BufferStore,
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
        assert engine.last_override_kwargs.get("__z4j_task_name__") == ("myapp.tasks.send_sms")
        # Existing operator-supplied kwarg preserved.
        assert engine.last_override_kwargs.get("existing") == "value"

    async def test_huey_retry_brain_name_overrides_operator_supplied_magic_key_r9_l1(
        self,
        buf: BufferStore,
    ) -> None:
        """Regression: brain-derived task_name MUST win over an
        operator-supplied ``__z4j_task_name__`` in override_kwargs.

        Pre- the dispatcher used ``setdefault`` which preserved
        the operator-supplied value and let it slip through to Huey's
        registry lookup. The fix is direct assignment so the brain's
        canonical task name always wins. This test asserts the new
        safe behavior; the previous test asserted the unsafe behavior
        as expected and was flipped as part of the closure.
        """
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
        # Brain-derived wins, operator-supplied magic key is
        # silently replaced (not echoed back to the audit trail at
        # this layer; the brain audit log records the operator's
        # retry click separately).
        assert engine.last_override_kwargs["__z4j_task_name__"] == ("myapp.brain.choice"), (
            " regression: dispatcher reverted to setdefault, "
            "letting operator-supplied __z4j_task_name__ slip through "
            "to Huey's registry lookup. Must be direct assignment."
        )

    async def test_non_huey_adapter_does_not_get_magic_key_injection(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
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


# ---------------------------------------------------------------------------
# Dispatcher must FAIL CLOSED for RQ on the TypeError fallback.
# Mixed-version (new z4j-bare 1.6.8+ + old z4j-rq <=1.6.6) would
# otherwise silently re-open the pickle RCE because old z4j-rq
# retry_task_action reads job.func_name / args / kwargs on the
# broker-stored Job.
# ---------------------------------------------------------------------------


class FakeLegacyRqEngine(FakeEngine):
    """Simulates an old z4j-rq adapter (<=1.6.6) that doesn't accept
    the ``task_name`` kwarg the 1.6.8+ dispatcher passes.

    ``name = "rq"`` so the dispatcher's RQ-specific fail-closed
    branch engages on the TypeError. ``retry_task`` and
    ``requeue_dead_letter`` both REFUSE the new-shape call by raising
    TypeError, matching the legacy 1.6.0 signature.
    """

    name = "rq"

    async def retry_task(  # type: ignore[override]
        self,
        task_id: str,
        *,
        override_args: tuple | None = None,
        override_kwargs: dict | None = None,
        eta: float | None = None,
        priority: object = None,
    ) -> CommandResult:
        # Legacy signature: no task_name kwarg. The dispatcher's
        # try/except on TypeError engages when the new shape is
        # rejected.
        self.retry_calls.append((task_id, override_args, override_kwargs, eta))
        return CommandResult(status="success", result={"new_task_id": f"new-{task_id}"})

    async def requeue_dead_letter(  # type: ignore[override]
        self,
        task_id: str,
    ) -> CommandResult:
        # Legacy: only task_id, no overrides, no task_name.
        self.dlq_calls.append(task_id)
        return CommandResult(status="success")


class TestR9H2RqFailClosed:
    """Dispatcher must refuse retry / DLQ against a legacy RQ adapter
    rather than fall back to a signature that re-opens."""

    async def test_retry_against_legacy_rq_fails_closed_r9_h2(
        self,
        buf: BufferStore,
    ) -> None:
        engine = FakeLegacyRqEngine()
        dispatcher = CommandDispatcher(
            engines={"rq": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="retry_task",
            target={"engine": "rq", "task_id": "rq-legacy-1"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result_frames = [e for e in entries if e.kind == "command_result"]
        assert len(result_frames) == 1
        parsed = json.loads(result_frames[0].payload.decode("utf-8"))
        assert parsed["payload"]["status"] == "failed", (
            " regression: dispatcher fell back to the legacy "
            "no-task_name retry call against an old z4j-rq adapter. "
            "Old z4j-rq reads job.func_name from the broker on retry "
            "(the pickle RCE). Must fail closed instead."
        )
        err = parsed["payload"]["error"] or ""
        assert "z4j-rq" in err and "1.6.7" in err, (
            f" regression: failure message must name z4j-rq + "
            f"the required floor so operators know what to upgrade. "
            f"Got: {err!r}"
        )
        # Verify the legacy adapter's retry_task was NOT called.
        assert engine.retry_calls == [], (
            " CRITICAL: dispatcher invoked the legacy adapter's "
            "retry_task despite the fail-closed branch. The "
            "pickle RCE is re-opened on this code path."
        )

    async def test_dlq_against_legacy_rq_fails_closed_r9_h2(
        self,
        buf: BufferStore,
    ) -> None:
        engine = FakeLegacyRqEngine()
        dispatcher = CommandDispatcher(
            engines={"rq": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "rq", "task_id": "rq-legacy-dlq-1"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        entries = buf.drain(10)
        result_frames = [e for e in entries if e.kind == "command_result"]
        assert len(result_frames) == 1
        parsed = json.loads(result_frames[0].payload.decode("utf-8"))
        assert parsed["payload"]["status"] == "failed", (
            " regression: DLQ dispatcher fell back to the legacy "
            "requeue_dead_letter against an old z4j-rq adapter. The "
            "DLQ-registry fallback in old z4j-rq routes through "
            "retry_task_action which reads job.func_name. Must fail "
            "closed."
        )
        assert "z4j-rq" in (parsed["payload"]["error"] or "")
        assert engine.dlq_calls == [], (
            " CRITICAL: dispatcher invoked the legacy adapter's "
            "requeue_dead_letter despite the fail-closed branch."
        )

    async def test_retry_against_modern_rq_still_succeeds(
        self,
        buf: BufferStore,
    ) -> None:
        """Sanity: the fail-closed posture is RQ-specific to
        legacy adapters. A modern z4j-rq 1.6.7+ that accepts task_name
        must still receive the call cleanly."""
        # Reuse the FakeEngineWithTaskName from the earlier tests.
        engine = FakeEngineWithTaskName()
        engine.name = "rq"  # type: ignore[assignment]  # pretend to be RQ
        dispatcher = CommandDispatcher(
            engines={"rq": engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="retry_task",
            target={"engine": "rq", "task_id": "rq-modern-1"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        assert engine.task_name_calls == ["myapp.tasks.send_email"], (
            "Modern adapter retry must still get task_name via the "
            "normal kwarg path; fail-closed must NOT engage "
            "when the adapter accepts the new signature."
        )

    async def test_retry_against_legacy_non_rq_still_falls_back(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        """Sanity: the fail-closed posture is RQ-ONLY. Other legacy
        adapters (celery, dramatiq, huey, arq, taskiq) keep the
        TypeError fallback because they have different security
        models (celery natively re-reads broker; dramatiq doesn't
        read pickle attributes; etc.)."""
        cmd = _make_command(
            action="retry_task",
            target={"engine": "fake", "task_id": "non-rq-legacy"},
            parameters={
                "task_name": "myapp.tasks.send_email",
                "override_args": [],
                "override_kwargs": {},
            },
        )
        await dispatcher.handle(cmd)
        # FakeEngine (name="fake") falls back via TypeError to the
        # legacy signature; the call lands on the adapter.
        task_ids = [c[0] for c in engine.retry_calls]
        assert "non-rq-legacy" in task_ids, (
            "Non-RQ legacy adapters must keep the TypeError fallback "
            "behavior; the fail-closed gate is RQ-specific."
        )


class TestBulkRetryEngineBindingRH3:
    """RH3: bulk_retry / requeue_dead_letter must bind adapter selection to the
    engine the brain validated the ids against (filter["engine"]) -- the wire
    frame's target carries no engine. Binding to the sole engine (or failing
    with no adapter on a multi-engine agent) would misroute a validated batch.
    """

    async def test_filter_engine_mismatch_fails_closed(self, buf: BufferStore) -> None:
        # Agent has ONLY rq; a celery-validated batch must NOT execute on rq.
        rq_engine = FakeEngine()
        d = CommandDispatcher(engines={"rq": rq_engine}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="bulk_retry",
            target={},  # no target.engine
            parameters={
                "filter": {"engine": "celery", "task_ids": ["j1"]},
                "max": 10,
            },
        )
        await d.handle(cmd)
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        assert "celery" in (parsed["payload"]["error"] or "")
        assert rq_engine.bulk_calls == []  # the rq adapter was NEVER invoked

    async def test_binds_to_filter_engine_on_multi_engine_agent(self, buf: BufferStore) -> None:
        # Multi-engine agent + no target.engine: the OLD code resolved no single
        # engine and failed; filter.engine must select the right adapter.
        celery_engine = FakeEngine()
        rq_engine = FakeEngine()
        d = CommandDispatcher(
            engines={"celery": celery_engine, "rq": rq_engine},
            schedulers={},
            buffer=buf,
        )
        cmd = _make_command(
            action="bulk_retry",
            target={},
            parameters={
                "filter": {"engine": "celery", "task_ids": ["j1"]},
                "max": 10,
            },
        )
        await d.handle(cmd)
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "success"
        assert len(celery_engine.bulk_calls) == 1  # celery ran it
        assert rq_engine.bulk_calls == []  # rq did not


class TestOneSidedOverrideRefusedRH2:
    """RH2: huey/dramatiq re-run by name/reference and cannot faithfully replay
    a ONE-SIDED override; the dispatcher refuses it (require both halves or
    neither) before the task_name smuggling collapses the None-vs-{} signal."""

    @staticmethod
    def _engine_named(name: str) -> FakeEngine:
        eng = FakeEngine()
        eng.name = name  # the RH2 check reads adapter.name
        # These tests exercise RH2 on a MODERN (1.7.1) adapter, so it attests
        # the safe-retry contract; otherwise the P1-1 gate would refuse first.
        eng.safe_retry_by_reference = True  # type: ignore[attr-defined]
        return eng

    async def _run(self, buf: BufferStore, engine_name: str, params: dict) -> dict:
        eng = self._engine_named(engine_name)
        d = CommandDispatcher(engines={engine_name: eng}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="retry_task",
            target={"engine": engine_name, "task_id": "x1"},
            parameters=params,
        )
        await d.handle(cmd)
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        return _decode_frame(result.payload)

    async def test_huey_one_sided_args_refused(self, buf: BufferStore) -> None:
        parsed = await self._run(buf, "huey", {"override_args": [1]})
        assert parsed["payload"]["status"] == "failed"
        assert "one-sided" in (parsed["payload"]["error"] or "")

    async def test_dramatiq_one_sided_kwargs_refused(self, buf: BufferStore) -> None:
        parsed = await self._run(buf, "dramatiq", {"override_kwargs": {"k": 1}})
        assert parsed["payload"]["status"] == "failed"
        assert "one-sided" in (parsed["payload"]["error"] or "")

    async def test_huey_both_overrides_allowed(self, buf: BufferStore) -> None:
        parsed = await self._run(buf, "huey", {"override_args": [1], "override_kwargs": {"k": 1}})
        assert parsed["payload"]["status"] == "success"

    async def test_huey_no_override_allowed_by_reference(self, buf: BufferStore) -> None:
        parsed = await self._run(buf, "huey", {})
        assert parsed["payload"]["status"] == "success"


class _PolyfillEngine(FakeEngine):
    """An engine with submit_task but NO native retry_task, so a retry is lowered
    to submit_task (the polyfill path)."""

    def __init__(self) -> None:
        super().__init__()
        self.name = "arqlike"
        self._capabilities = {"submit_task"}  # no retry_task -> polyfill
        self.submit_calls: list[tuple] = []

    async def submit_task(
        self, name, *, args=(), kwargs=None, queue=None, eta=None, priority=None
    ) -> CommandResult:
        self.submit_calls.append((name, tuple(args), dict(kwargs or {})))
        return CommandResult(status="success")


class TestOneSidedOverrideRefusedRH2More:
    """RH2 coverage for the two paths the first test class missed: the polyfill
    (submit_task lowering) one-sided refusal, and the requeue_dead_letter
    (DLQ) one-sided refusal on huey/dramatiq."""

    async def test_polyfill_one_sided_override_refused(self, buf: BufferStore) -> None:
        eng = _PolyfillEngine()
        d = CommandDispatcher(engines={"arqlike": eng}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="retry_task",
            target={"engine": "arqlike", "task_id": "t1"},
            parameters={"task_name": "app.t", "override_args": [1]},  # one-sided
        )
        await d.handle(cmd)
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        # Assert the GUARD's specific message so the test isolates the one-sided
        # refusal from an incidental failure (e.g. dict(None) raising downstream).
        assert (
            "both override_args and override_kwargs" in (parsed["payload"]["error"] or "").lower()
        )
        assert eng.submit_calls == []  # never lowered to submit_task

    async def test_polyfill_both_overrides_allowed(self, buf: BufferStore) -> None:
        eng = _PolyfillEngine()
        d = CommandDispatcher(engines={"arqlike": eng}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="retry_task",
            target={"engine": "arqlike", "task_id": "t2"},
            parameters={
                "task_name": "app.t",
                "override_args": [1],
                "override_kwargs": {"k": 1},
            },
        )
        await d.handle(cmd)
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        assert _decode_frame(result.payload)["payload"]["status"] == "success"
        assert len(eng.submit_calls) == 1

    async def test_dlq_one_sided_override_refused_on_huey(self, buf: BufferStore) -> None:
        eng = FakeEngine()
        eng.name = "huey"  # the DLQ RH2 guard reads adapter.name
        eng.safe_retry_by_reference = True  # type: ignore[attr-defined]  # modern adapter (P1-1)
        d = CommandDispatcher(engines={"huey": eng}, schedulers={}, buffer=buf)
        cmd = _make_command(
            action="requeue_dead_letter",
            target={"engine": "huey", "task_id": "d1"},
            parameters={"override_kwargs": {"k": 1}},  # one-sided (no override_args)
        )
        await d.handle(cmd)
        result = next(e for e in buf.drain(10) if e.kind == "command_result")
        parsed = _decode_frame(result.payload)
        assert parsed["payload"]["status"] == "failed"
        assert "one-sided" in (parsed["payload"]["error"] or "")
        assert eng.dlq_calls == []  # never reached requeue_dead_letter


class TestDurableDedupOrderingR11High4:
    """The DURABLE dedup record must not be written before the task
    actually executes.

    Recording ``fire:{fire_id}`` up-front means a crash between the record and
    the broker enqueue makes the recovery delivery look like a duplicate, so it
    is merely re-acked and NEVER executed: the fire is LOST. The prior
    command_id-keyed ledger did not have this failure mode (a recovery arrives
    under a new command_id, so it re-executed). Trading a duplicate for a loss
    is the wrong direction: the stated guarantee is at-least-once delivery.

    Rule: record durably only once the execution actually SUCCEEDED.
    """

    async def test_no_durable_record_when_execution_never_reached_broker(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        async def failing_submit(
            name, *, args=(), kwargs=None, queue=None, eta=None, priority=None
        ):
            raise RuntimeError("worker died before the broker enqueue")

        engine.submit_task = failing_submit  # type: ignore[attr-defined]

        cmd = _make_command(
            action="schedule.fire",
            target={"id": "sched-uuid"},
            parameters={
                "task_name": "myapp.tasks.cleanup",
                "engine": "fake",
                "fire_id": "fire-crash",
                "kwargs": {},
            },
        )
        await dispatcher.handle(cmd)

        # mark_command_seen returns True only if the key was ALREADY recorded.
        assert buf.mark_command_seen("fire:fire-crash") is False, (
            "durable dedup was written BEFORE execution: a crash in that window "
            "would suppress the recovery delivery and LOSE the fire"
        )

    async def test_durable_record_written_after_a_successful_execution(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # The other half: once the fire really was enqueued, the durable record
        # must exist so a cross-restart redelivery is recognised as a duplicate.
        async def ok_submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            return CommandResult(status="success", result={"task_id": "t1"})

        engine.submit_task = ok_submit  # type: ignore[attr-defined]

        cmd = _make_command(
            action="schedule.fire",
            target={"id": "sched-uuid"},
            parameters={
                "task_name": "myapp.tasks.cleanup",
                "engine": "fake",
                "fire_id": "fire-ok",
                "kwargs": {},
            },
        )
        await dispatcher.handle(cmd)

        assert buf.mark_command_seen("fire:fire-ok") is True, (
            "a successfully executed fire must be recorded durably so a "
            "cross-restart redelivery is deduped"
        )


class TestInMemoryDedupSuccessOnlyR13:
    """The IN-MEMORY tier must suppress only successes.

    HIGH-4 moved the DURABLE record to after a successful execution, so a crash
    before the broker enqueue no longer suppressed the recovery delivery. The
    same rule was never applied to the in-memory tier: it wrote the key at CHECK
    time, before execution, and removed it on neither failure nor exception. So
    a fire whose first attempt FAILED, re-delivered to the same still-running
    agent under a second command id within the TTL, was recognised as a
    duplicate, acked, and never executed. Identical work loss, one tier over.
    """

    async def test_failed_fire_is_re_executed_on_redelivery(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        calls: list[str] = []

        async def submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            calls.append(name)
            if len(calls) == 1:
                raise RuntimeError("broker refused the first attempt")
            return CommandResult(status="success", result={"task_id": "t1"})

        engine.submit_task = submit  # type: ignore[attr-defined]

        def _frame(cmd_id: str):
            return CommandFrame(
                id=cmd_id,
                payload=CommandPayload(
                    action="schedule.fire",
                    target={"id": "sched-uuid"},
                    parameters={
                        "task_name": "myapp.tasks.cleanup",
                        "engine": "fake",
                        "fire_id": "fire-retry",
                        "kwargs": {},
                    },
                ),
                hmac="deadbeef" * 8,
            )

        # Two DIFFERENT command ids for the SAME fire: a re-drive after the
        # first attempt's failure. Both share the dedup key ``fire:fire-retry``.
        first, second = _frame("cmd_first"), _frame("cmd_second")
        assert first.id != second.id
        await dispatcher.handle(first)
        await dispatcher.handle(second)

        assert len(calls) == 2, (
            "the re-delivery of a FAILED fire was suppressed by the in-memory "
            "dedup and never executed: the fire is lost"
        )

    async def test_successful_fire_is_still_suppressed(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
    ) -> None:
        # The other half: success must still suppress, or every re-drive of an
        # already-executed fire would double-run it.
        calls: list[str] = []

        async def submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            calls.append(name)
            return CommandResult(status="success", result={"task_id": "t1"})

        engine.submit_task = submit  # type: ignore[attr-defined]

        def _frame(cmd_id: str):
            return CommandFrame(
                id=cmd_id,
                payload=CommandPayload(
                    action="schedule.fire",
                    target={"id": "sched-uuid"},
                    parameters={
                        "task_name": "myapp.tasks.cleanup",
                        "engine": "fake",
                        "fire_id": "fire-once",
                        "kwargs": {},
                    },
                ),
                hmac="deadbeef" * 8,
            )

        await dispatcher.handle(_frame("cmd_a"))
        await dispatcher.handle(_frame("cmd_b"))
        assert len(calls) == 1, "an already-succeeded fire must not run twice"

    async def test_seen_map_is_ordered_by_write_time_and_capped_r13(
        self,
        dispatcher: CommandDispatcher,
    ) -> None:
        # The TTL sweep walks the OrderedDict front-to-back and stops at the
        # first unexpired entry, which is only sound if writes move a key to the
        # END. Re-assigning an existing key updates its value without reordering
        # it, so a refreshed key stayed at the front and stalled the sweep. The
        # cap is also enforced after the insert, not before.
        dispatcher._record_success_key("a")
        dispatcher._record_success_key("b")
        dispatcher._record_success_key("a")  # refreshed -> must move to the end
        assert list(dispatcher._seen_commands) == ["b", "a"]

        for i in range(dispatcher._DEDUP_MAX + 50):
            dispatcher._record_success_key(f"k{i}")
        assert len(dispatcher._seen_commands) <= dispatcher._DEDUP_MAX


class TestDuplicateTerminalizationR11Med:
    """A deduplicated replacement command must terminalize.

    Ack-only left the replacement DISPATCHED until the brain's timeout worker
    retired it as a spurious TIMEOUT, even though the work had demonstrably
    completed. A DURABLE dedup hit proves a prior process executed it
    successfully (that ledger is only written after success), so the replacement
    is answered with that outcome.
    """

    async def test_durable_duplicate_emits_a_success_result(
        self,
        dispatcher: CommandDispatcher,
        engine: FakeEngine,
        buf: BufferStore,
    ) -> None:
        # Pretend a PRIOR process already completed this fire.
        buf.mark_command_seen("fire:fire-done")

        cmd = _make_command(
            action="schedule.fire",
            target={"id": "sched-uuid"},
            parameters={
                "task_name": "myapp.tasks.cleanup",
                "engine": "fake",
                "fire_id": "fire-done",
                "kwargs": {},
            },
        )
        await dispatcher.handle(cmd)

        drained = buf.drain(10)
        assert [e.kind for e in drained if e.kind == "command_ack"], "must still ack"
        results = [e for e in drained if e.kind == "command_result"]
        assert results, "a durable duplicate must terminalize, not sit DISPATCHED"
        parsed = _decode_frame(results[0].payload)
        assert parsed["payload"]["status"] == "success"
        assert parsed["payload"]["result"]["deduplicated"] is True


class TestInflightClaimIsScopedR14:
    """The in-flight claim must be released on EVERY exit path.

    It was discarded inline after the result was written, so any exit that did
    not reach that line -- cancellation while awaiting the adapter, or a failure
    writing the ack or the result -- retained the key forever. Every later
    re-delivery of that fire was then answered as an in-memory duplicate and
    never executed. Ownership is a scope, not a pair of statements.
    """

    async def _fire(self, cmd_id: str, fire_id: str) -> CommandFrame:
        return CommandFrame(
            id=cmd_id,
            payload=CommandPayload(
                action="schedule.fire",
                target={"id": "sched-uuid"},
                parameters={
                    "task_name": "myapp.tasks.cleanup",
                    "engine": "fake",
                    "fire_id": fire_id,
                    "kwargs": {},
                },
            ),
            hmac="deadbeef" * 8,
        )

    async def test_cancellation_releases_the_claim(
        self, dispatcher: CommandDispatcher, engine: FakeEngine
    ) -> None:
        import asyncio

        calls: list[str] = []

        async def submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            calls.append(name)
            if len(calls) == 1:
                raise asyncio.CancelledError
            return CommandResult(status="success", result={"task_id": "t1"})

        engine.submit_task = submit  # type: ignore[attr-defined]

        with contextlib.suppress(asyncio.CancelledError):
            await dispatcher.handle(await self._fire("cmd_a", "fire-cancel"))
        assert "fire:fire-cancel" not in dispatcher._inflight_keys, (
            "a cancelled execution kept its in-flight claim; every re-delivery "
            "would be acked as a duplicate and never run"
        )

        await dispatcher.handle(await self._fire("cmd_b", "fire-cancel"))
        assert len(calls) == 2, "the re-delivery after a cancellation must execute"

    async def test_result_write_failure_releases_the_claim(
        self, dispatcher: CommandDispatcher, engine: FakeEngine
    ) -> None:
        async def submit(name, *, args=(), kwargs=None, queue=None, eta=None, priority=None):
            return CommandResult(status="success", result={"task_id": "t1"})

        engine.submit_task = submit  # type: ignore[attr-defined]
        boom = {"armed": True}
        real_queue_result = dispatcher._queue_result

        def _queue_result(*a, **kw):
            if boom["armed"]:
                boom["armed"] = False
                raise RuntimeError("buffer append failed")
            return real_queue_result(*a, **kw)

        dispatcher._queue_result = _queue_result  # type: ignore[assignment]
        with contextlib.suppress(RuntimeError):
            await dispatcher.handle(await self._fire("cmd_c", "fire-writefail"))
        assert "fire:fire-writefail" not in dispatcher._inflight_keys, (
            "a failure writing the result kept the claim, permanently "
            "suppressing every retry of this fire"
        )
