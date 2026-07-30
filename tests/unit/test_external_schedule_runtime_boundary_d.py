"""End-to-end agent emission for Boundary-D external schedule epochs."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from z4j_bare.buffer import BufferStore
from z4j_bare.runtime import AgentRuntime
from z4j_bare.transport.longpoll import UploadContentRejectedError
from z4j_bare.transport.websocket import UndeliverableFrameError
from z4j_core.models import CommandResult
from z4j_core.schedule_external import (
    canonical_external_json,
    external_projection_body,
    external_projection_digest,
    external_snapshot_frame_digest,
    normalize_external_schedule,
)


class _Schedule:
    def __init__(self, name: str) -> None:
        self.name = name
        self.is_enabled = True

    def model_dump(self, mode: str = "python") -> dict[str, object]:
        assert mode == "json"
        return {
            "name": self.name,
            "task_name": f"jobs.{self.name}",
            "kind": "interval",
            "expression": "60",
            "engine": "apscheduler",
            "scheduler": "apscheduler",
            "args": [],
            "kwargs": {},
            "is_enabled": self.is_enabled,
        }


class _Scheduler:
    name = "apscheduler"

    def __init__(self) -> None:
        self.calls = 0
        self.rows: list[Any] = [_Schedule("alpha")]

    async def list_schedules(self) -> list[Any]:
        self.calls += 1
        return list(self.rows)

    async def get_schedule(self, schedule_id: str) -> Any | None:
        return next(
            (row for row in self.rows if getattr(row, "name", None) == schedule_id),
            None,
        )

    async def enable_schedule(self, schedule_id: str) -> CommandResult:
        row = await self.get_schedule(schedule_id)
        if row is None:
            return CommandResult(status="failed", error="missing")
        row.is_enabled = True
        return CommandResult(status="success")

    async def disable_schedule(self, schedule_id: str) -> CommandResult:
        row = await self.get_schedule(schedule_id)
        if row is None:
            return CommandResult(status="failed", error="missing")
        row.is_enabled = False
        return CommandResult(status="success")


def _runtime(store: BufferStore, scheduler: _Scheduler) -> AgentRuntime:
    runtime = AgentRuntime.__new__(AgentRuntime)
    runtime.schedulers = {scheduler.name: scheduler}
    runtime._connected_schedulers_ref = [scheduler]
    runtime._buffer = store
    runtime._external_schedule_authorities = {}
    runtime._external_schedule_locks = {}
    runtime._snapshot_signal_pending = set()
    runtime._snapshot_signal_dirty = set()
    runtime._heartbeat = None
    runtime._pending_acks = {}
    runtime._acks_seen_early = set()
    runtime._send_backoff = 0.1
    runtime._consecutive_retryable = 0
    runtime._send_batch_size = 500
    runtime._transport = SimpleNamespace(confirm_on_send=False)
    return runtime


def _activation() -> dict[str, object]:
    return {
        "scheduler": "apscheduler",
        "owner": "apscheduler",
        "source_scope": "default",
        "stream_id": "11111111-1111-4111-8111-111111111111",
        "epoch_uuid": "22222222-2222-4222-8222-222222222222",
        "epoch_number": 7,
        "adapter_instance_id": "brain-issued-adapter-1",
        "stable_source": True,
    }


def _projection_entries(store: BufferStore) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for entry in store.drain(20):
        if entry.kind != "external_schedule_projection":
            continue
        frame = json.loads(entry.payload)
        data = frame["payload"]["events"][0]["data"]
        body = data["external_snapshot_frame"]
        assert data["frame_digest"] == external_snapshot_frame_digest(body)
        grouped.setdefault(body["sequence"], []).append(body)
    result = []
    for sequence, frames in sorted(grouped.items()):
        terminal = next(frame for frame in frames if frame["frame_kind"] == "terminal")
        row_frames = sorted(
            (frame for frame in frames if frame["frame_kind"] == "rows"),
            key=lambda frame: frame["frame_index"],
        )
        rows = [row for frame in row_frames for row in frame["schedules"]]
        projection = external_projection_body(
            stream_id=terminal["stream_id"],
            epoch_uuid=terminal["epoch_uuid"],
            epoch_number=terminal["epoch_number"],
            sequence=sequence,
            kind="snapshot",
            owner=terminal["owner"],
            source_scope=terminal["source_scope"],
            adapter_instance_id=terminal["adapter_instance_id"],
            schedules=rows,
            complete=True,
            stable_source=terminal["stable_source"],
        )
        result.append(
            {
                "external_projection": projection,
                "payload_digest": terminal["snapshot_digest"],
            },
        )
    return result


async def test_unsequenced_boot_snapshot_is_not_observed_or_emitted(
    tmp_path: Path,
) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    runtime = _runtime(store, scheduler)
    try:
        assert await runtime._emit_schedule_snapshot(scheduler, reason="boot") is None
        assert scheduler.calls == 0
        assert store.size() == 0
    finally:
        store.close()


async def test_activation_and_later_snapshot_share_one_durable_sequence(
    tmp_path: Path,
) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    runtime = _runtime(store, scheduler)
    try:
        first = await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )
        scheduler.rows.append(_Schedule("beta"))
        second = await runtime._emit_schedule_snapshot(
            scheduler,
            reason="periodic",
        )

        assert first["sequence"] == 1
        assert second is not None
        assert second["sequence"] == 2
        assert first["adapter_instance_id"] == second["adapter_instance_id"]
        projections = _projection_entries(store)
        assert [item["external_projection"]["sequence"] for item in projections] == [1, 2]
        assert [len(item["external_projection"]["schedules"]) for item in projections] == [1, 2]
        for item in projections:
            assert item["payload_digest"] == external_projection_digest(
                item["external_projection"],
            )
    finally:
        store.close()


async def test_failed_snapshot_serialization_does_not_advance_sequence(
    tmp_path: Path,
) -> None:
    class _Bad:
        def model_dump(self, mode: str = "python") -> dict[str, object]:
            raise TypeError("not JSON safe")

    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    runtime = _runtime(store, scheduler)
    try:
        await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )
        scheduler.rows = [_Bad()]
        assert await runtime._emit_schedule_snapshot(scheduler, reason="signal") is None
        scheduler.rows = [_Schedule("beta")]
        result = await runtime._emit_schedule_snapshot(scheduler, reason="retry")

        assert result is not None
        assert result["sequence"] == 2
        assert [item["external_projection"]["sequence"] for item in _projection_entries(store)] == [
            1,
            2,
        ]
    finally:
        store.close()


async def test_large_snapshot_is_split_into_bounded_frames(
    tmp_path: Path,
) -> None:
    class _LargeSchedule(_Schedule):
        def model_dump(self, mode: str = "python") -> dict[str, object]:
            row = super().model_dump(mode)
            row["args"] = ["x" * 400_000]
            return row

    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    scheduler.rows = [
        _LargeSchedule("alpha"),
        _LargeSchedule("beta"),
        _LargeSchedule("gamma"),
    ]
    runtime = _runtime(store, scheduler)
    try:
        result = await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )

        assert result["sequence"] == 1
        entries = store.drain(20)
        assert len(entries) == 4
        assert max(len(entry.payload) for entry in entries) <= 768 * 1024
        assert {entry.kind for entry in entries} == {"external_schedule_projection"}
        projections = _projection_entries(store)
        assert len(projections) == 1
        assert len(projections[0]["external_projection"]["schedules"]) == 3
    finally:
        store.close()


async def test_signal_during_observation_forces_one_more_fresh_read(
    tmp_path: Path,
) -> None:
    class _BlockingScheduler(_Scheduler):
        def __init__(self) -> None:
            super().__init__()
            self.block_next = False
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.rerun = asyncio.Event()

        async def list_schedules(self) -> list[Any]:
            self.calls += 1
            if self.calls >= 3:
                self.rerun.set()
            if self.block_next:
                self.block_next = False
                self.entered.set()
                await self.release.wait()
            return list(self.rows)

    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _BlockingScheduler()
    runtime = _runtime(store, scheduler)
    runtime._loop = asyncio.get_running_loop()
    try:
        await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )
        scheduler.block_next = True
        runtime._scheduler_sink(
            "apscheduler",
            "updated",
            object(),
        )
        await asyncio.wait_for(scheduler.entered.wait(), timeout=2)
        scheduler.rows.append(_Schedule("beta"))
        runtime._scheduler_sink(
            "apscheduler",
            "updated",
            object(),
        )
        scheduler.release.set()

        await asyncio.wait_for(scheduler.rerun.wait(), timeout=2)
        assert [item["external_projection"]["sequence"] for item in _projection_entries(store)] == [
            1,
            2,
            3,
        ]
    finally:
        for task in getattr(runtime, "_schedule_observation_tasks", set()):
            await task
        store.close()


async def test_content_reject_cannot_delete_unacknowledged_projection(
    tmp_path: Path,
) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    runtime = _runtime(store, scheduler)
    try:
        await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )
        for _ in range(6):
            runtime._handle_content_reject(
                store,
                store.drain(1),
                UploadContentRejectedError("rejected"),
            )

        remaining = store.drain(10)
        assert len(remaining) == 2
        assert {entry.kind for entry in remaining} == {"external_schedule_projection"}
    finally:
        store.close()


async def test_local_undeliverable_path_retains_projection(
    tmp_path: Path,
) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    runtime = _runtime(store, scheduler)
    try:
        await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )
        entries = store.drain(1)
        retained = runtime._handle_undeliverable_drop(
            store,
            entries,
            UndeliverableFrameError(
                "oversize",
                accepted=[],
                drop_indices=[0],
            ),
        )

        assert retained is True
        remaining = store.drain(10)
        assert entries[0].id in {entry.id for entry in remaining}
        assert len(remaining) == 2
        assert runtime._pending_acks == {}
    finally:
        store.close()


async def test_control_reserves_then_buffers_exact_result_projection(
    tmp_path: Path,
) -> None:
    import hashlib
    import uuid

    store = BufferStore(tmp_path / "buffer.sqlite")
    scheduler = _Scheduler()
    runtime = _runtime(store, scheduler)
    try:
        await runtime.activate_external_schedule_stream(
            {"scheduler": "apscheduler"},
            _activation(),
        )
        prior = normalize_external_schedule(
            scheduler.rows[0].model_dump(mode="json"),
            owner="apscheduler",
        )
        desired = dict(prior)
        desired["is_enabled"] = False
        operation_id = str(uuid.uuid4())
        result = await runtime.control_external_schedule(
            {"schedule_id": str(uuid.uuid4())},
            {
                "operation_id": operation_id,
                "scheduler": "apscheduler",
                "schedule_id": "alpha",
                "source_key": "alpha",
                "z4j_schedule_id": str(uuid.uuid4()),
                "stream_id": _activation()["stream_id"],
                "epoch_uuid": _activation()["epoch_uuid"],
                "epoch_number": 7,
                "adapter_instance_id": "brain-issued-adapter-1",
                "expected_accepted_sequence": 1,
                "expected_projection_digest": hashlib.sha256(
                    canonical_external_json(prior),
                ).hexdigest(),
                "desired_projection": desired,
                "desired_projection_digest": hashlib.sha256(
                    canonical_external_json(desired),
                ).hexdigest(),
                "registry_owner_id": str(uuid.uuid4()),
                "session_generation": str(uuid.uuid4()),
            },
        )

        assert result["sequence"] == 2
        assert scheduler.rows[0].is_enabled is False
        control_events = []
        for entry in store.drain(20):
            frame = json.loads(entry.payload)
            data = frame["payload"]["events"][0]["data"]
            if "external_projection" in data:
                control_events.append(data)
        assert len(control_events) == 1
        body = control_events[0]["external_projection"]
        assert body["kind"] == "control"
        assert body["operation_id"] == operation_id
        assert body["sequence"] == 2
        assert body["schedules"] == [desired]
        assert control_events[0]["payload_digest"] == (external_projection_digest(body))
    finally:
        store.close()
