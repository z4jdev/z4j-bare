"""Tests for the process-wide ``AgentRuntime`` singleton.

The singleton prevents duplicate WebSocket sessions when two
independent install paths fire in the same Python process - the
concrete case being a Django+Celery worker where both
``Z4JDjangoConfig.ready()`` and ``celery.signals.worker_init``
try to install an agent.

Without the singleton the brain saw two WS registrations for the
same token, treated the second as a takeover, and the agent
ended up OFFLINE even while heartbeats arrived. These tests pin
the contract: first caller wins, second caller gets the existing
runtime back.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from z4j_bare import _process_singleton


@pytest.fixture(autouse=True)
def reset_singleton() -> None:
    """Every test starts with an empty registry."""
    _process_singleton.clear_runtime()
    yield
    _process_singleton.clear_runtime()


class TestBasicRegistration:
    def test_first_register_wins(self) -> None:
        runtime = MagicMock(name="first-runtime")
        active = _process_singleton.try_register(runtime, owner="alpha")
        assert active is runtime
        assert _process_singleton.current_runtime() is runtime
        assert _process_singleton.current_owner() == "alpha"

    def test_second_register_gets_the_first_back(self) -> None:
        first = MagicMock(name="first-runtime")
        second = MagicMock(name="second-runtime")
        _process_singleton.try_register(first, owner="alpha")
        active = _process_singleton.try_register(second, owner="beta")
        assert active is first, "second caller must get the first runtime"
        assert active is not second, (
            "the second runtime must be discarded by the caller"
        )
        assert _process_singleton.current_owner() == "alpha", (
            "owner label must track the winner, not the loser"
        )

    def test_skip_is_logged_with_both_owners(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        first = MagicMock(name="first-runtime")
        second = MagicMock(name="second-runtime")
        _process_singleton.try_register(first, owner="z4j_django.apps")
        with caplog.at_level("INFO", logger="z4j.agent.singleton"):
            _process_singleton.try_register(second, owner="celery.worker_init")
        msg = caplog.records[-1].message
        assert "z4j_django.apps" in msg
        assert "celery.worker_init" in msg

    def test_clear_allows_re_registration(self) -> None:
        first = MagicMock(name="first-runtime")
        second = MagicMock(name="second-runtime")
        _process_singleton.try_register(first, owner="alpha")
        _process_singleton.clear_runtime()
        assert _process_singleton.current_runtime() is None
        assert _process_singleton.current_owner() is None
        active = _process_singleton.try_register(second, owner="beta")
        assert active is second
        assert _process_singleton.current_owner() == "beta"

    def test_clear_is_idempotent(self) -> None:
        _process_singleton.clear_runtime()
        _process_singleton.clear_runtime()
        assert _process_singleton.current_runtime() is None


class TestThreadSafety:
    """Stress test the lock around concurrent registrations.

    Import-time and signal-handler code is nominally single-
    threaded, but pytest harnesses and worker-pool init paths can
    and do create threads during bootstrap. The singleton must
    survive contention without losing the first-wins invariant.
    """

    def test_concurrent_register_exactly_one_winner(self) -> None:
        num_threads = 16
        barrier = threading.Barrier(num_threads)
        winners: list[object] = []
        results: list[tuple[int, object]] = []

        def worker(i: int) -> None:
            candidate = MagicMock(name=f"runtime-{i}")
            barrier.wait()
            active = _process_singleton.try_register(
                candidate, owner=f"worker-{i}",
            )
            results.append((i, active))
            if active is candidate:
                winners.append(candidate)

        threads = [
            threading.Thread(target=worker, args=(i,))
            for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(winners) == 1, (
            f"exactly one thread must win the race, got {len(winners)}"
        )
        winner = winners[0]
        # Every losing thread must have received the winner back.
        for _, active in results:
            assert active is winner
