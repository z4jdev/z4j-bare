"""Tests for the 1.6.9 teardown-hardening trio.

Three structural fixes for the heartbeat shutdown-race, learned from
how CPython's own concurrent.futures + Sentry + OpenTelemetry handle
background-worker teardown:

- §4 ``register_shutdown_atexit`` -- register shutdown in the
  ``threading._register_atexit`` phase (which runs BEFORE
  concurrent.futures tears down the default executor) instead of the
  plain ``atexit`` phase (which runs after). This makes the
  executor-shutdown race structurally impossible rather than merely
  swallowed.
- §2 ``_drain_default_executor`` -- version-aware deterministic drain
  of the loop's default ThreadPoolExecutor before ``loop.close()``,
  bounded so a wedged provider thread can't hang teardown.
- §3 ``_heartbeat_enabled`` / ``Z4J_HEARTBEAT=0`` -- heartbeat-less
  mode for short-lived one-shot processes.
"""

from __future__ import annotations

import asyncio
import sys
import threading

import pytest

from z4j_bare.control import register_shutdown_atexit
from z4j_bare.runtime import _drain_default_executor, _heartbeat_enabled


# ---------------------------------------------------------------------------
# §4: register_shutdown_atexit uses the threading._register_atexit phase
# ---------------------------------------------------------------------------


class TestRegisterShutdownAtexit:
    def test_uses_threading_register_atexit_when_available(self) -> None:
        """On any supported Python (3.9+) threading._register_atexit
        exists, so the helper must report it used the 'threading'
        phase -- the one that runs BEFORE concurrent.futures'
        executor teardown."""
        recorded: list[str] = []
        mechanism = register_shutdown_atexit(lambda: recorded.append("ran"))
        assert mechanism == "threading", (
            "register_shutdown_atexit must prefer threading._register_atexit "
            "(runs before concurrent.futures executor teardown); got "
            f"{mechanism!r}. If this fails on a future Python that removed "
            "the private API, the fallback to plain atexit is acceptable "
            "but the structural ordering fix is then lost - investigate."
        )

    def test_falls_back_to_atexit_if_private_api_missing(
        self, monkeypatch,
    ) -> None:
        """If a future Python removes threading._register_atexit, the
        helper must still register the callback (via plain atexit) so
        shutdown still runs - just in the later phase."""
        monkeypatch.delattr(threading, "_register_atexit", raising=False)
        mechanism = register_shutdown_atexit(lambda: None)
        assert mechanism == "atexit"

    def test_falls_back_when_register_raises_runtimeerror(
        self, monkeypatch,
    ) -> None:
        """threading._register_atexit raises RuntimeError if called
        after interpreter shutdown has begun. The helper must catch it
        and fall back to plain atexit rather than propagating."""
        def _boom(_cb):  # noqa: ANN001
            raise RuntimeError("can't register atexit after shutdown")

        monkeypatch.setattr(threading, "_register_atexit", _boom)
        mechanism = register_shutdown_atexit(lambda: None)
        assert mechanism == "atexit"


def test_register_atexit_phase_runs_before_executor_teardown() -> None:
    """End-to-end empirical proof in a subprocess: a handler registered
    via threading._register_atexit can still submit to a
    ThreadPoolExecutor at teardown, while a plain-atexit handler hits
    'cannot schedule new futures after shutdown'. This is the exact
    z4j bug + the exact reason the §4 fix works.
    """
    import subprocess
    import textwrap

    prog = textwrap.dedent(
        """
        import threading, concurrent.futures, atexit, sys
        results = []
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        def via_register_atexit():
            try:
                ex.submit(lambda: 1).result(timeout=1)
                results.append("register_atexit:OK")
            except RuntimeError as e:
                results.append("register_atexit:FAIL")

        def via_plain_atexit():
            try:
                ex.submit(lambda: 1).result(timeout=1)
                results.append("plain_atexit:OK")
            except RuntimeError:
                results.append("plain_atexit:FAIL")

        atexit.register(lambda: sys.stdout.write("|".join(results)))
        atexit.register(via_plain_atexit)
        threading._register_atexit(via_register_atexit)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", prog],
        capture_output=True, text=True, timeout=30,
    )
    combined = out.stdout.strip()
    assert "register_atexit:OK" in combined, (
        "threading._register_atexit phase should still reach a live "
        f"executor; got: {combined!r}"
    )
    assert "plain_atexit:FAIL" in combined, (
        "plain atexit phase should hit the dead executor (this IS the "
        f"z4j bug); got: {combined!r}"
    )


# ---------------------------------------------------------------------------
# §2: _drain_default_executor drains, and never hangs
# ---------------------------------------------------------------------------


class TestDrainDefaultExecutor:
    def test_drain_on_loop_with_pending_to_thread_work_does_not_raise(
        self,
    ) -> None:
        """A loop that ran a to_thread call (populating its default
        executor) can be drained cleanly before close."""
        loop = asyncio.new_event_loop()
        try:
            # Touch the default executor via a to_thread round-trip.
            loop.run_until_complete(asyncio.sleep(0))

            async def _use_executor() -> int:
                return await asyncio.to_thread(lambda: 7)

            assert loop.run_until_complete(_use_executor()) == 7
            # The drain must complete cleanly and quickly.
            _drain_default_executor(loop, deadline_s=2.0)
        finally:
            loop.close()

    def test_drain_on_closed_loop_is_noop(self) -> None:
        loop = asyncio.new_event_loop()
        loop.close()
        # Must not raise on an already-closed loop.
        _drain_default_executor(loop, deadline_s=1.0)

    def test_drain_is_bounded_when_executor_thread_is_wedged(self) -> None:
        """The load-bearing safety property: a genuinely wedged worker
        thread (the inspector.stats BRPOP case) must NOT hang teardown.
        The drain must return within ~deadline_s and abandon the wedged
        thread to the daemon-thread net."""
        loop = asyncio.new_event_loop()
        release = threading.Event()
        try:
            # Submit a job that blocks until we release it - simulating
            # a wedged provider. We never release before the drain, so
            # the drain must hit its deadline and return.
            async def _wedge() -> None:
                # Fire-and-forget a blocking call into the default
                # executor; do not await it (it would never return).
                loop.run_in_executor(None, release.wait)
                await asyncio.sleep(0.05)

            loop.run_until_complete(_wedge())

            t0 = threading.Event()
            elapsed = {}

            import time as _time
            start = _time.monotonic()
            _drain_default_executor(loop, deadline_s=0.5)
            elapsed["s"] = _time.monotonic() - start

            # Must have returned near the deadline, NOT hung on the
            # wedged thread. Generous upper bound to avoid flakiness.
            assert elapsed["s"] < 3.0, (
                f"drain hung on a wedged executor thread "
                f"({elapsed['s']:.1f}s) - the bounded-wait guard failed"
            )
        finally:
            release.set()  # unwedge so the loop can close
            loop.close()


# ---------------------------------------------------------------------------
# §3: heartbeat-less toggle
# ---------------------------------------------------------------------------


class TestHeartbeatEnabledToggle:
    def test_default_is_enabled(self, monkeypatch) -> None:
        monkeypatch.delenv("Z4J_HEARTBEAT", raising=False)
        assert _heartbeat_enabled() is True

    @pytest.mark.parametrize("off", ["0", "false", "no", "off", "OFF", "False", ""])
    def test_falsy_values_disable(self, monkeypatch, off: str) -> None:
        monkeypatch.setenv("Z4J_HEARTBEAT", off)
        assert _heartbeat_enabled() is False, (
            f"Z4J_HEARTBEAT={off!r} should disable the heartbeat"
        )

    @pytest.mark.parametrize("on", ["1", "true", "yes", "on", "anything"])
    def test_truthy_values_enable(self, monkeypatch, on: str) -> None:
        monkeypatch.setenv("Z4J_HEARTBEAT", on)
        assert _heartbeat_enabled() is True
