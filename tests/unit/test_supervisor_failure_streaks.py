"""The supervisor must count a failure under the class that classified it.

A disconnect is logged in tiers: the first failure of a streak is loud and
carries the remedy, later ones drop to DEBUG, and every tenth re-surfaces as an
"still trying" INFO. The tier is chosen from the failure's position in its
streak, so the streak number is what decides whether an operator ever reads the
remedy.

An agent the brain rejects as incompatible has exactly one useful log line: the
one that says reconnecting will not fix it and names what has to be upgraded.
The supervisor counted that rejection under one class and read the streak back
from another, so the number it logged with was a different class's count. Zero
is not a first failure, and the actionable line was never emitted.

These tests drive the real supervisor loop. The brain is the only thing stood
in for, by a transport that refuses the handshake the way a terminal close
arrives in production -- ``connect()`` raising before any task group starts.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import SecretStr
from z4j_bare import runtime as rt
from z4j_bare.runtime import AgentRuntime
from z4j_bare.transport.websocket import _TERMINAL_CLOSE_CODES
from z4j_core.errors import AgentIncompatibleError, AuthenticationError, ProtocolError
from z4j_core.models import Config

SUPERVISOR_LOGGER = "z4j.runtime.supervisor"


class _Framework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover  never reached here
        pass


class _RefusingTransport:
    """A brain that refuses every handshake.

    ``WebSocketTransport.connect`` is where a terminal close surfaces, and it
    raises before ``_connect_and_run`` opens its task group, so this reproduces
    the exception shape the supervisor sees in production.
    """

    def __init__(
        self,
        failure: Callable[[], BaseException],
        on_attempt: Callable[[int], None],
    ) -> None:
        self._failure = failure
        self._on_attempt = on_attempt
        self.attempts = 0
        self.session_id: str | None = None

    async def connect(self) -> None:
        self.attempts += 1
        self._on_attempt(self.attempts)
        raise self._failure()


def _incompatible() -> AgentIncompatibleError:
    """The exact error the transport raises on a 4426 close."""
    return AgentIncompatibleError(
        _TERMINAL_CLOSE_CODES[4426],
        details={"close_code": 4426},
    )


#: One representative failure per error class the supervisor can classify,
#: with the tier and streak field its FIRST occurrence must produce. Keyed by
#: error class so the table below can be pinned against the runtime's own.
_FIRST_FAILURE_BY_CLASS: dict[str, tuple[Callable[[], BaseException], int, str, str]] = {
    "auth": (
        lambda: AuthenticationError("brain rejected agent token"),
        logging.WARNING,
        "auth rejected",
        "auth_error_count",
    ),
    "incompatible": (
        _incompatible,
        logging.ERROR,
        "upgrade the agent or the brain",
        "protocol_error_count",
    ),
    "protocol": (
        lambda: ProtocolError("brain speaks an unusable protocol version"),
        logging.WARNING,
        "protocol error",
        "protocol_error_count",
    ),
    "connection": (
        lambda: ConnectionError("failed to receive hello_ack"),
        logging.WARNING,
        "disconnected",
        "connection_error_count",
    ),
}


def _make_runtime(tmp_path: Path) -> AgentRuntime:
    config = Config(
        brain_url="https://brain.example.com",
        token=SecretStr("test-token-12345678901234567890"),
        project_id="test",
        buffer_path=tmp_path / "unused.sqlite",
        dev_mode=True,
        autostart=False,
        hmac_secret=SecretStr(secrets.token_hex(32)),
    )
    return AgentRuntime(
        config=config,
        framework=_Framework(),  # type: ignore[arg-type]
        engines=[],
    )


async def _supervise_until(
    runtime: AgentRuntime,
    failure: Callable[[], BaseException],
    *,
    attempts: int,
) -> _RefusingTransport:
    """Run the real supervisor for exactly ``attempts`` failed connects.

    The stop event is set from inside the last attempt, so the supervisor
    classifies, counts and logs that failure and then returns at its next stop
    check instead of sleeping out the class's backoff.
    """
    runtime._stop_event = asyncio.Event()
    runtime._reconnect_now = asyncio.Event()
    runtime._dispatcher = object()  # type: ignore[assignment]  # no command frames arrive

    def _on_attempt(n: int) -> None:
        assert runtime._stop_event is not None
        if n >= attempts:
            runtime._stop_event.set()

    transport = _RefusingTransport(failure, _on_attempt)
    runtime._transport = transport  # type: ignore[assignment]
    await asyncio.wait_for(runtime._supervise(), timeout=10.0)
    return transport


def _loud(records: list[logging.LogRecord]) -> list[logging.LogRecord]:
    return [r for r in records if r.levelno >= logging.WARNING]


def test_the_failure_table_covers_every_class_the_supervisor_schedules() -> None:
    """A new error class must arrive with a first-failure expectation.

    Without this, adding a class to the schedule table and forgetting the
    branch that reports it would leave the tests below silently not covering
    it, which is exactly how the incompatible class shipped uncounted.
    """
    assert set(_FIRST_FAILURE_BY_CLASS) == set(rt._RECONNECT_SCHEDULES)


@pytest.mark.parametrize("error_class", sorted(_FIRST_FAILURE_BY_CLASS))
async def test_a_first_failure_is_streak_one_and_logged_at_its_own_tier(
    error_class: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Every class must reach its first-failure branch, not a mid-streak one."""
    failure, level, phrase, state_key = _FIRST_FAILURE_BY_CLASS[error_class]
    runtime = _make_runtime(tmp_path)

    with caplog.at_level(logging.DEBUG, logger=SUPERVISOR_LOGGER):
        transport = await _supervise_until(runtime, failure, attempts=1)

    assert transport.attempts == 1
    records = [r for r in caplog.records if r.name == SUPERVISOR_LOGGER]

    loud = _loud(records)
    assert len(loud) == 1, f"expected one first-failure record, got {[r.msg for r in loud]}"
    assert loud[0].levelno == level
    assert phrase in loud[0].getMessage()

    # The mid-streak summary is the symptom of a miscounted streak: it can
    # only be reached with a count that is not 1.
    assert not [r for r in records if "still disconnected" in r.getMessage()]

    assert runtime.supervisor_state()[state_key] == 1


async def test_an_incompatible_agent_is_told_once_what_to_upgrade(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The reported symptom: an INFO summary numbered zero, every attempt.

    Asserted separately from the parametrised sweep because the wording is the
    deliverable. An operator whose agent is refused for its version has one
    chance to read what to do about it.
    """
    runtime = _make_runtime(tmp_path)

    with caplog.at_level(logging.DEBUG, logger=SUPERVISOR_LOGGER):
        await _supervise_until(runtime, _incompatible, attempts=1)

    records = [r for r in caplog.records if r.name == SUPERVISOR_LOGGER]
    messages = [r.getMessage() for r in records]

    assert not [m for m in messages if "#0" in m], messages
    errors = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert _TERMINAL_CLOSE_CODES[4426] in errors[0]
    assert "Reconnecting cannot fix this" in errors[0]


async def test_a_streak_escalates_by_its_own_class_count(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tier follows a real per-class count, not a constant.

    A supervisor that always logged loudly would satisfy the tests above and
    still be wrong. Ten consecutive rejections must yield one ERROR and one
    numbered summary, and the number must be ten.

    Only the retry timing is patched, so the loop does not wait out two minutes
    per incompatible attempt. Nothing under test reads it.
    """
    monkeypatch.setattr(
        rt,
        "_RECONNECT_SCHEDULES",
        {
            name: rt._ReconnectSchedule(initial=0.0, jitter=0.0, maximum=0.0)
            for name in rt._RECONNECT_SCHEDULES
        },
    )
    runtime = _make_runtime(tmp_path)

    with caplog.at_level(logging.DEBUG, logger=SUPERVISOR_LOGGER):
        transport = await _supervise_until(runtime, _incompatible, attempts=10)

    assert transport.attempts == 10
    records = [r for r in caplog.records if r.name == SUPERVISOR_LOGGER]

    assert len([r for r in records if r.levelno == logging.ERROR]) == 1
    summaries = [r.getMessage() for r in records if "still disconnected" in r.getMessage()]
    assert summaries == ["z4j agent: still disconnected (#10 incompatible)"]
    assert runtime.supervisor_state()["protocol_error_count"] == 10
