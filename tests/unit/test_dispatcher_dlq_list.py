"""``dlq.list`` routing in :class:`z4j_bare.dispatcher.CommandDispatcher`.

The dispatcher maps the command onto ``adapter.list_dead_letters`` under the
``list_dead_letters`` capability gate, validates and clamps the parameters,
and serialises the page into ``command_result.result``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.errors import AdapterError, ValidationError
from z4j_core.models import (
    DLQ_LIST_ACTION,
    DLQ_LIST_DEFAULT_LIMIT,
    DLQ_LIST_MAX_LIMIT,
    LIST_DEAD_LETTERS_CAPABILITY,
    DeadLetterEntry,
    DeadLetterPage,
)
from z4j_core.transport.frames import CommandFrame, CommandPayload


class FakeEngine:
    """Minimal engine the dispatcher can route to (self-contained: the sibling
    ``test_dispatcher`` module is not importable next to the repo-root
    ``tests`` package)."""

    name = "fake"
    protocol_version = "2"

    def __init__(self) -> None:
        self._capabilities = {"retry_task", "cancel_task", "requeue_dead_letter"}

    def capabilities(self) -> set[str]:
        return set(self._capabilities)

    async def requeue_dead_letter(self, task_id: str) -> Any:
        return None


class DlqEngine(FakeEngine):
    """FakeEngine that advertises and implements dead-letter listing."""

    def __init__(self) -> None:
        super().__init__()
        self._capabilities.add(LIST_DEAD_LETTERS_CAPABILITY)
        self.list_calls: list[tuple[str | None, int, str | None]] = []
        self.page = DeadLetterPage(
            entries=[
                DeadLetterEntry(
                    task_id="dead-1",
                    task_name="myapp.tasks.work",
                    queue="default",
                    failed_at=datetime(2026, 10, 2, 12, 0, tzinfo=UTC),
                    error_excerpt="ValueError: boom",
                    attempts=4,
                ),
            ],
            next_cursor="1",
            total=2,
            engine="fake",
        )
        self.raise_with: Exception | None = None

    async def list_dead_letters(
        self,
        queue: str | None = None,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> DeadLetterPage:
        self.list_calls.append((queue, limit, cursor))
        if self.raise_with is not None:
            raise self.raise_with
        return self.page


class AdvertisesButLacksMethod(FakeEngine):
    def capabilities(self) -> set[str]:
        return {LIST_DEAD_LETTERS_CAPABILITY}


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.fixture
def engine() -> DlqEngine:
    return DlqEngine()


@pytest.fixture
def dispatcher(buf: BufferStore, engine: DlqEngine) -> CommandDispatcher:
    return CommandDispatcher(engines={"fake": engine}, schedulers={}, buffer=buf)


def _command(
    *,
    target: dict[str, Any] | None = None,
    parameters: dict[str, Any] | None = None,
    command_id: str = "cmd_dlq_01",
) -> CommandFrame:
    return CommandFrame(
        id=command_id,
        payload=CommandPayload(
            action=DLQ_LIST_ACTION,
            target=target if target is not None else {"engine": "fake"},
            parameters=parameters or {},
        ),
        hmac="deadbeef" * 8,
    )


def _result(buf: BufferStore) -> dict[str, Any]:
    results = [e for e in buf.drain(10) if e.kind == "command_result"]
    assert len(results) == 1
    return json.loads(results[0].payload.decode("utf-8"))["payload"]


class TestHappyPath:
    async def test_page_is_serialised_into_result(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(
            _command(parameters={"queue": "default", "limit": 25, "cursor": "0"})
        )
        payload = _result(buf)
        assert payload["status"] == "success"
        assert payload["error"] is None
        assert payload["result"] == engine.page.model_dump(mode="json")
        assert payload["result"]["entries"][0]["failed_at"] == "2026-10-02T12:00:00Z"
        assert engine.list_calls == [("default", 25, "0")]

    async def test_defaults_when_parameters_absent(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(_command())
        assert _result(buf)["status"] == "success"
        assert engine.list_calls == [(None, DLQ_LIST_DEFAULT_LIMIT, None)]

    async def test_queue_falls_back_to_target(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(
            _command(target={"engine": "fake", "type": "queue", "id": "emails"})
        )
        assert _result(buf)["status"] == "success"
        assert engine.list_calls == [("emails", DLQ_LIST_DEFAULT_LIMIT, None)]

    async def test_limit_is_clamped_to_the_max(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(_command(parameters={"limit": 10_000}))
        assert _result(buf)["status"] == "success"
        assert engine.list_calls == [(None, DLQ_LIST_MAX_LIMIT, None)]

    async def test_single_engine_fallback_when_target_has_no_engine(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(_command(target={}))
        assert _result(buf)["status"] == "success"
        assert len(engine.list_calls) == 1


class TestCapabilityGate:
    async def test_refused_when_capability_absent(self, buf: BufferStore) -> None:
        plain = FakeEngine()  # advertises requeue_dead_letter but not list_dead_letters
        assert "requeue_dead_letter" in plain.capabilities()
        d = CommandDispatcher(engines={"fake": plain}, schedulers={}, buffer=buf)
        await d.handle(_command(parameters={"queue": "default"}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert payload["result"] is None
        assert "does not support action 'dlq.list'" in payload["error"]
        assert "list_dead_letters" in payload["error"]

    async def test_gate_runs_before_parameter_validation(self, buf: BufferStore) -> None:
        plain = FakeEngine()
        d = CommandDispatcher(engines={"fake": plain}, schedulers={}, buffer=buf)
        await d.handle(_command(parameters={"limit": "not-an-int"}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "does not support action" in payload["error"]

    async def test_advertised_but_unimplemented_fails_closed(self, buf: BufferStore) -> None:
        d = CommandDispatcher(
            engines={"fake": AdvertisesButLacksMethod()}, schedulers={}, buffer=buf
        )
        await d.handle(_command())
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "does not implement list_dead_letters" in payload["error"]


class TestParameterValidation:
    @pytest.mark.parametrize("bad_limit", ["50", 1.5, True, None])
    async def test_non_integer_limit_refused(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore, bad_limit: object
    ) -> None:
        await dispatcher.handle(_command(parameters={"limit": bad_limit}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "limit must be an integer" in payload["error"]
        assert engine.list_calls == []

    @pytest.mark.parametrize("bad_limit", [0, -5])
    async def test_non_positive_limit_refused(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore, bad_limit: int
    ) -> None:
        await dispatcher.handle(_command(parameters={"limit": bad_limit}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "limit must be positive" in payload["error"]
        assert engine.list_calls == []

    async def test_non_string_cursor_refused(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(_command(parameters={"cursor": 7}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "cursor must be a string" in payload["error"]
        assert engine.list_calls == []

    async def test_non_string_queue_refused(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(_command(parameters={"queue": ["a"]}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "queue must be a string" in payload["error"]
        assert engine.list_calls == []

    async def test_empty_cursor_is_first_page(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        await dispatcher.handle(_command(parameters={"cursor": ""}))
        assert _result(buf)["status"] == "success"
        assert engine.list_calls == [(None, DLQ_LIST_DEFAULT_LIMIT, None)]


class TestAdapterFailures:
    async def test_z4j_error_becomes_failed_result_with_code(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        engine.raise_with = ValidationError("invalid dead-letter cursor 'x'")
        await dispatcher.handle(_command(parameters={"cursor": "x"}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert payload["error"].startswith("dlq.list: validation_error:")
        assert "cursor" in payload["error"]

    async def test_adapter_error_becomes_failed_result(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        engine.raise_with = AdapterError("redis unreachable")
        await dispatcher.handle(_command())
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert payload["error"] == "dlq.list: adapter_error: redis unreachable"

    async def test_unexpected_exception_becomes_failed_result(
        self, dispatcher: CommandDispatcher, engine: DlqEngine, buf: BufferStore
    ) -> None:
        engine.raise_with = RuntimeError("kaboom")
        await dispatcher.handle(_command())
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert payload["error"] == "dlq.list: RuntimeError: kaboom"

    async def test_non_page_return_is_refused(self, buf: BufferStore) -> None:
        class WrongShape(DlqEngine):
            async def list_dead_letters(  # type: ignore[override]
                self,
                queue: str | None = None,
                *,
                limit: int = 100,
                cursor: str | None = None,
            ) -> Any:
                return {"entries": []}

        d = CommandDispatcher(engines={"fake": WrongShape()}, schedulers={}, buffer=buf)
        await d.handle(_command())
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "expected DeadLetterPage" in payload["error"]


class TestEngineSelection:
    async def test_unknown_engine_fails_cleanly(self, buf: BufferStore) -> None:
        d = CommandDispatcher(engines={"fake": DlqEngine()}, schedulers={}, buffer=buf)
        await d.handle(_command(target={"engine": "nope"}))
        payload = _result(buf)
        assert payload["status"] == "failed"
        assert "no engine adapter registered for 'nope'" in payload["error"]

    async def test_multi_engine_binds_to_target_engine(self, buf: BufferStore) -> None:
        rq_like = DlqEngine()
        rq_like.name = "rq"
        other = DlqEngine()
        other.name = "dramatiq"
        d = CommandDispatcher(engines={"rq": rq_like, "dramatiq": other}, schedulers={}, buffer=buf)
        await d.handle(_command(target={"engine": "dramatiq"}))
        assert _result(buf)["status"] == "success"
        assert other.list_calls and not rq_like.list_calls
