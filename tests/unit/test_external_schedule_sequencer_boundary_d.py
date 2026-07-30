"""Boundary-D durable source sequencing in the agent buffer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from z4j_bare.buffer import (
    BufferStore,
    ExternalScheduleAuthorityError,
)
from z4j_core.errors import BufferStorageError


def _payload(sequence: int, adapter_instance_id: str) -> bytes:
    return json.dumps(
        {
            "sequence": sequence,
            "adapter_instance_id": adapter_instance_id,
        },
        sort_keys=True,
    ).encode()


def _append(store: BufferStore) -> tuple[int, int, str]:
    return store.append_external_schedule_projection(
        owner="apscheduler",
        source_scope="default",
        stream_id="11111111-1111-4111-8111-111111111111",
        epoch_uuid="22222222-2222-4222-8222-222222222222",
        epoch_number=7,
        adapter_instance_id="brain-issued-adapter-1",
        build_payload=_payload,
    )


def test_sequence_and_exact_event_are_committed_together(tmp_path: Path) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        first_id, first_sequence, adapter_id = _append(store)
        second_id, second_sequence, same_adapter_id = _append(store)

        assert second_id > first_id
        assert (first_sequence, second_sequence) == (1, 2)
        assert same_adapter_id == adapter_id
        decoded = [json.loads(entry.payload) for entry in store.drain(10)]
        assert [item["sequence"] for item in decoded] == [1, 2]
        assert {item["adapter_instance_id"] for item in decoded} == {
            adapter_id,
        }
    finally:
        store.close()


def test_payload_failure_does_not_burn_a_sequence(tmp_path: Path) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:

        def fail(_sequence: int, _adapter_instance_id: str) -> bytes:
            raise TypeError("cannot serialize observation")

        with pytest.raises(TypeError, match="cannot serialize observation"):
            store.append_external_schedule_projection(
                owner="apscheduler",
                source_scope="default",
                stream_id="11111111-1111-4111-8111-111111111111",
                epoch_uuid="22222222-2222-4222-8222-222222222222",
                epoch_number=7,
                adapter_instance_id="brain-issued-adapter-1",
                build_payload=fail,
            )

        assert store.size() == 0
        _entry_id, sequence, _adapter_id = _append(store)
        assert sequence == 1
    finally:
        store.close()


def test_scope_cannot_switch_epoch_in_place(tmp_path: Path) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        _append(store)
        with pytest.raises(
            ExternalScheduleAuthorityError,
            match="already owns this source scope",
        ):
            store.append_external_schedule_projection(
                owner="apscheduler",
                source_scope="default",
                stream_id="33333333-3333-4333-8333-333333333333",
                epoch_uuid="44444444-4444-4444-8444-444444444444",
                epoch_number=8,
                adapter_instance_id="brain-issued-adapter-2",
                build_payload=_payload,
            )
        assert store.size() == 1
    finally:
        store.close()


def test_reopened_buffer_cannot_continue_an_old_process_epoch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "buffer.sqlite"
    first = BufferStore(path)
    _append(first)
    first.close()

    reopened = BufferStore(path)
    try:
        with pytest.raises(
            ExternalScheduleAuthorityError,
            match="earlier process generation",
        ):
            _append(reopened)
        assert reopened.size() == 1
    finally:
        reopened.close()


def test_bounded_eviction_never_breaks_a_reserved_sequence_prefix(
    tmp_path: Path,
) -> None:
    store = BufferStore(
        tmp_path / "buffer.sqlite",
        max_entries=2,
        max_bytes=100_000,
    )
    try:
        _append(store)
        store.append("event_batch", b"ordinary")
        _append(store)

        entries = store.drain(10)
        assert [entry.kind for entry in entries] == [
            "external_schedule_projection",
            "external_schedule_projection",
        ]
        assert [json.loads(entry.payload)["sequence"] for entry in entries] == [
            1,
            2,
        ]

        with pytest.raises(BufferStorageError, match="causally protected"):
            store.append("event_batch", b"cannot-displace-the-chain")
        assert [json.loads(entry.payload)["sequence"] for entry in store.drain(10)] == [
            1,
            2,
        ]
    finally:
        store.close()


def test_content_reject_budget_cannot_delete_a_projection(tmp_path: Path) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    try:
        entry_id, _sequence, _adapter_id = _append(store)
        for _ in range(5):
            store.increment_content_rejects([entry_id])
        assert store.evict_if_exhausted([entry_id], 5) == 0
        assert store.size() == 1
    finally:
        store.close()


def test_control_reservation_blocks_observations_until_exact_publish(
    tmp_path: Path,
) -> None:
    store = BufferStore(tmp_path / "buffer.sqlite")
    operation_id = "33333333-3333-4333-8333-333333333333"
    desired_digest = "a" * 64
    try:
        _append(store)
        reservation = store.reserve_external_schedule_control(
            operation_id=operation_id,
            owner="apscheduler",
            source_scope="default",
            stream_id="11111111-1111-4111-8111-111111111111",
            epoch_uuid="22222222-2222-4222-8222-222222222222",
            epoch_number=7,
            adapter_instance_id="brain-issued-adapter-1",
            expected_sequence=1,
            desired_projection_digest=desired_digest,
        )
        assert reservation.sequence == 2
        assert reservation.already_published is False

        with pytest.raises(
            ExternalScheduleAuthorityError,
            match="blocks later observations",
        ):
            _append(store)

        entry_id, sequence, adapter_id, replayed = store.append_reserved_external_schedule_control(
            operation_id=operation_id,
            owner="apscheduler",
            source_scope="default",
            stream_id="11111111-1111-4111-8111-111111111111",
            epoch_uuid="22222222-2222-4222-8222-222222222222",
            epoch_number=7,
            adapter_instance_id="brain-issued-adapter-1",
            expected_sequence=1,
            desired_projection_digest=desired_digest,
            build_payload=_payload,
        )
        assert entry_id > 0
        assert sequence == 2
        assert adapter_id == "brain-issued-adapter-1"
        assert replayed is False
        assert [json.loads(entry.payload)["sequence"] for entry in store.drain(10)] == [1, 2]

        replay = store.reserve_external_schedule_control(
            operation_id=operation_id,
            owner="apscheduler",
            source_scope="default",
            stream_id="11111111-1111-4111-8111-111111111111",
            epoch_uuid="22222222-2222-4222-8222-222222222222",
            epoch_number=7,
            adapter_instance_id="brain-issued-adapter-1",
            expected_sequence=1,
            desired_projection_digest=desired_digest,
        )
        assert replay.already_published is True
        _entry_id, next_sequence, _adapter_id = _append(store)
        assert next_sequence == 3
    finally:
        store.close()
