"""Regression tests for the heartbeat shutdown-ordering race.

Bug: on short-lived processes (one-shot Django ``manage.py`` commands
that boot the z4j runtime in a daemon thread), the asyncio default
ThreadPoolExecutor is torn down at interpreter exit concurrently with
an in-flight final heartbeat tick. ``asyncio.to_thread`` -> executor
``submit()`` then raises
``RuntimeError('cannot schedule new futures after shutdown')``. The
prior ``_safe_provider_call`` caught it in a broad
``except Exception: logger.exception(...)`` clause, printing a scary
2x ERROR-level traceback at the end of every CLI run.

Fix A (this test's primary target): shutdown-class RuntimeErrors are
classified and logged at DEBUG, returning a ``shutting_down``
sentinel instead of the loud ``provider raised`` blob.

Fix C: ``_safe_provider_call`` re-checks ``stop_event`` immediately
before dispatching to the executor and skips entirely if shutdown is
already underway.

Plus: the ``shutting_down`` sentinel must NOT route into
``AgentStatusPayload(**status)`` downstream (which would raise an
invalid-shape error and re-introduce the noise through a different
path).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.heartbeat import Heartbeat


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(
        path=tmp_path / "buf.sqlite",
        max_entries=100,
        max_bytes=100_000,
    )
    yield store
    store.close()


def _make_heartbeat(
    buf: BufferStore,
    *,
    health_provider: Any = None,
    status_provider: Any = None,
) -> Heartbeat:
    return Heartbeat(
        buffer=buf,
        stop_event=asyncio.Event(),
        interval=10.0,
        health_provider=health_provider,
        status_provider=status_provider,
    )


# ---------------------------------------------------------------------------
# Fix A: shutdown-class RuntimeError -> debug, not exception
# ---------------------------------------------------------------------------


class TestShutdownClassRuntimeErrorIsQuiet:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "shutdown_msg",
        [
            "cannot schedule new futures after shutdown",
            # The interpreter-teardown variant (CPython tears down the
            # default executor in its own atexit phase). The narrow
            # "after shutdown" match missed it - regression for the
            # loud traceback the Flask/FastAPI e2e surfaced.
            "cannot schedule new futures after interpreter shutdown",
            "Executor shutdown has been called",
            "Event loop is closed",
        ],
    )
    async def test_shutdown_runtimeerror_logs_at_debug(
        self,
        buf: BufferStore,
        caplog,
        shutdown_msg: str,
    ) -> None:
        def _provider() -> dict[str, str]:
            raise RuntimeError(shutdown_msg)

        hb = _make_heartbeat(buf, health_provider=_provider)

        with caplog.at_level(logging.DEBUG, logger="z4j.runtime.heartbeat"):
            result = await hb._safe_provider_call(
                _provider,
                provider_name="health",
            )

        # Sentinel, not the loud blob.
        assert result == {"error": "shutting_down"}

        # No ERROR / exception-level record for this teardown event.
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records == [], (
            "shutdown-class RuntimeError must NOT log at ERROR level - "
            f"got {[r.getMessage() for r in error_records]}"
        )

        # And it IS recorded at debug (so it's still observable).
        debug_records = [
            r for r in caplog.records if r.levelno == logging.DEBUG and "shutdown" in r.getMessage()
        ]
        assert debug_records, "shutdown-class RuntimeError should leave a debug breadcrumb"


# ---------------------------------------------------------------------------
# Genuine failures stay loud
# ---------------------------------------------------------------------------


class TestGenuineFailuresStayLoud:
    @pytest.mark.asyncio
    async def test_non_shutdown_runtimeerror_still_logs_exception(
        self,
        buf: BufferStore,
        caplog,
    ) -> None:
        def _provider() -> dict[str, str]:
            raise RuntimeError("the broker exploded for real")

        hb = _make_heartbeat(buf, health_provider=_provider)

        with caplog.at_level(logging.DEBUG, logger="z4j.runtime.heartbeat"):
            result = await hb._safe_provider_call(
                _provider,
                provider_name="health",
            )

        assert result == {"error": "provider raised"}
        # A non-shutdown RuntimeError is a real failure -> exception level.
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records, (
            "a genuine (non-shutdown) RuntimeError must still surface at "
            "exception level - the fix must not silence real failures"
        )

    @pytest.mark.asyncio
    async def test_value_error_still_logs_exception(
        self,
        buf: BufferStore,
        caplog,
    ) -> None:
        def _provider() -> dict[str, str]:
            raise ValueError("malformed provider output")

        hb = _make_heartbeat(buf, health_provider=_provider)

        with caplog.at_level(logging.DEBUG, logger="z4j.runtime.heartbeat"):
            result = await hb._safe_provider_call(
                _provider,
                provider_name="status",
            )

        assert result == {"error": "provider raised"}
        assert [r for r in caplog.records if r.levelno >= logging.ERROR], (
            "non-RuntimeError exceptions must still log loudly"
        )


# ---------------------------------------------------------------------------
# Fix C: re-check stop_event right before the to_thread dispatch
# ---------------------------------------------------------------------------


class TestFixCStopRecheck:
    @pytest.mark.asyncio
    async def test_stop_set_skips_dispatch_entirely(
        self,
        buf: BufferStore,
    ) -> None:
        calls = {"n": 0}

        def _provider() -> dict[str, str]:
            calls["n"] += 1
            return {"ok": "yes"}

        hb = _make_heartbeat(buf, health_provider=_provider)
        # Shutdown already underway before the call.
        hb.stop_event.set()

        result = await hb._safe_provider_call(
            _provider,
            provider_name="health",
        )

        assert result == {"error": "shutting_down"}
        assert calls["n"] == 0, (
            "Fix C: when stop_event is already set, the provider must "
            "not be dispatched to the executor at all"
        )


# ---------------------------------------------------------------------------
# Downstream: shutting_down sentinel must not re-introduce noise
# ---------------------------------------------------------------------------


class TestSentinelDoesNotReintroduceNoise:
    @pytest.mark.asyncio
    async def test_status_shutdown_sentinel_skips_payload_construction(
        self,
        buf: BufferStore,
        caplog,
    ) -> None:
        """A status provider that hits the shutdown race must NOT route
        ``{"error": "shutting_down"}`` into AgentStatusPayload(**status),
        which would raise an invalid-shape error logged at exception
        level - re-creating the very noise we removed."""

        def _status_provider() -> dict[str, Any]:
            raise RuntimeError("cannot schedule new futures after shutdown")

        hb = _make_heartbeat(buf, status_provider=_status_provider)

        with caplog.at_level(logging.DEBUG, logger="z4j.runtime.heartbeat"):
            await hb._enqueue_agent_status()

        # No "returned invalid shape" exception-level record.
        invalid_shape = [
            r
            for r in caplog.records
            if r.levelno >= logging.ERROR and "invalid shape" in r.getMessage()
        ]
        assert invalid_shape == [], (
            "shutting_down sentinel leaked into AgentStatusPayload "
            "construction and logged an invalid-shape exception - the "
            "downstream guard in _enqueue_agent_status is missing"
        )
        # Nothing was appended to the buffer (the dying tick was skipped).
        assert buf.size() == 0

    @pytest.mark.asyncio
    async def test_health_shutdown_sentinel_skips_heartbeat_append(
        self,
        buf: BufferStore,
        caplog,
    ) -> None:
        """A health provider that hits the shutdown race must skip the
        heartbeat append rather than ship a dying tick carrying the
        sentinel as its health blob."""

        def _health_provider() -> dict[str, str]:
            raise RuntimeError("cannot schedule new futures after shutdown")

        hb = _make_heartbeat(buf, health_provider=_health_provider)

        with caplog.at_level(logging.DEBUG, logger="z4j.runtime.heartbeat"):
            await hb._enqueue_heartbeat()

        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records == []
        # The dying heartbeat tick was skipped; nothing buffered.
        assert buf.size() == 0
