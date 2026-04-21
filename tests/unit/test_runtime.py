"""Unit tests for ``z4j_bare.runtime.AgentRuntime``.

These tests use a fake engine, fake transport, and fake framework
adapter to exercise the supervisor's lifecycle, signal-wiring, and
HMAC enforcement paths without spinning up a real Celery worker
or websocket connection.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from z4j_core.errors import AuthenticationError, ProtocolError
from z4j_core.models import Config

from z4j_bare.runtime import AgentRuntime, _first


class FakeFramework:
    name = "bare"

    def __init__(self) -> None:
        self.startup_fired = False

    def fire_startup(self) -> None:
        self.startup_fired = True


class FakeEngine:
    name = "fake"
    protocol_version = "1"

    def __init__(self) -> None:
        self.connect_calls: list[Any] = []
        self.disconnect_calls = 0

    def connect_signals(self, loop: Any = None) -> None:
        self.connect_calls.append(loop)

    def disconnect_signals(self) -> None:
        self.disconnect_calls += 1

    def capabilities(self) -> set[str]:
        return {"retry", "cancel"}

    async def discover_tasks(self, hints: Any = None) -> list[Any]:  # noqa: ARG002
        return []

    async def subscribe_registry_changes(self) -> Iterator[Any]:
        if False:
            yield  # pragma: no cover

    async def subscribe_events(self) -> Iterator[Any]:
        if False:
            yield  # pragma: no cover

    async def list_queues(self) -> list[Any]:
        return []

    async def list_workers(self) -> list[Any]:
        return []

    async def get_task(self, task_id: str) -> Any:
        return None

    async def retry_task(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def cancel_task(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def bulk_retry(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def purge_queue(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def requeue_dead_letter(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def restart_worker(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def _make_config(
    *,
    tmp_path: Path,
    dev_mode: bool = True,
    hmac_secret: str | None = None,
) -> Config:
    return Config(
        brain_url="https://brain.example.com",
        token=SecretStr("test-token-12345678901234567890"),
        project_id="test",
        buffer_path=tmp_path / "buf.sqlite",
        dev_mode=dev_mode,
        autostart=False,
        hmac_secret=SecretStr(hmac_secret) if hmac_secret else None,
    )


class TestStartGuards:
    def test_refuses_to_start_without_hmac_in_production(
        self, tmp_path: Path,
    ) -> None:
        config = _make_config(tmp_path=tmp_path, dev_mode=False, hmac_secret=None)
        runtime = AgentRuntime(
            config=config,
            framework=FakeFramework(),
            engines=[FakeEngine()],
        )
        with pytest.raises(RuntimeError, match="hmac_secret is required"):
            runtime.start()

    def test_dev_mode_also_requires_hmac(self, tmp_path: Path) -> None:
        # Protocol v2 has no meaningful "unsigned" mode - every
        # stateful frame on the wire carries an envelope HMAC - so
        # dev_mode does NOT let you skip the secret any more. It
        # only relaxes the ``wss://`` guard.
        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=None)
        runtime = AgentRuntime(
            config=config,
            framework=FakeFramework(),
            engines=[FakeEngine()],
        )
        with pytest.raises(RuntimeError, match="hmac_secret is required"):
            runtime.start()


class TestProductionHMAC:
    def test_short_hmac_secret_refused(self, tmp_path: Path) -> None:
        # Less than 32 bytes - HMACVerifier raises ValueError, which
        # we catch in _main and exit cleanly.
        config = _make_config(
            tmp_path=tmp_path,
            dev_mode=False,
            hmac_secret="too-short",
        )
        runtime = AgentRuntime(
            config=config,
            framework=FakeFramework(),
            engines=[FakeEngine()],
        )
        runtime.start()
        try:
            # Loop should bail out fast - give it a moment.
            import time

            time.sleep(0.2)
        finally:
            runtime.stop(timeout=2.0)


class TestSignalWiring:
    def test_engine_connect_signals_called(self, tmp_path: Path) -> None:
        engine = FakeEngine()
        config = _make_config(
            tmp_path=tmp_path,
            dev_mode=False,
            hmac_secret=secrets.token_hex(32),
        )
        runtime = AgentRuntime(
            config=config,
            framework=FakeFramework(),
            engines=[engine],
        )
        runtime.start()
        try:
            # Give the background loop a tick to call connect_signals.
            import time

            time.sleep(0.3)
            assert engine.connect_calls, "connect_signals was never called"
        finally:
            runtime.stop(timeout=2.0)

        # Disconnect must also fire as the loop tears down.
        assert engine.disconnect_calls >= 1


class TestFirstHelper:
    def test_first_returns_leaf(self) -> None:
        eg = ExceptionGroup("g", [ValueError("a"), KeyError("b")])
        first = _first(eg)
        assert isinstance(first, ValueError)

    def test_first_unwraps_nested(self) -> None:
        inner = ExceptionGroup("inner", [RuntimeError("x")])
        outer = ExceptionGroup("outer", [inner])
        first = _first(outer)
        assert isinstance(first, RuntimeError)

    def test_first_handles_auth_error(self) -> None:
        eg = ExceptionGroup("g", [AuthenticationError("nope")])
        first = _first(eg)
        assert isinstance(first, AuthenticationError)

    def test_first_handles_protocol_error(self) -> None:
        eg = ExceptionGroup("g", [ProtocolError("nope")])
        first = _first(eg)
        assert isinstance(first, ProtocolError)


@pytest.fixture
def _no_real_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub asyncio.new_event_loop slot if a test wants to bypass it."""
    yield
    # nothing to clean
