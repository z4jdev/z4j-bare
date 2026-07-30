"""Unit tests for ``z4j_bare.runtime.AgentRuntime``.

These tests use a fake engine, fake transport, and fake framework
adapter to exercise the supervisor's lifecycle, signal-wiring, and
HMAC enforcement paths without spinning up a real Celery worker
or websocket connection.
"""

from __future__ import annotations

import secrets
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr
from z4j_bare.runtime import AgentRuntime, _advertised_capabilities, _first
from z4j_core.errors import AuthenticationError, ProtocolError
from z4j_core.models import Config
from z4j_core.transport import RETRY_BY_REFERENCE_CAPABILITY


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

    async def discover_tasks(self, hints: Any = None) -> list[Any]:
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


def test_retry_contract_is_derived_from_loaded_adapter() -> None:
    class CurrentEngine(FakeEngine):
        safe_retry_by_reference = True

    old = _advertised_capabilities({"fake": FakeEngine()}, {})
    current = _advertised_capabilities({"fake": CurrentEngine()}, {})

    assert RETRY_BY_REFERENCE_CAPABILITY not in old["fake"]
    assert RETRY_BY_REFERENCE_CAPABILITY in current["fake"]


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
        self,
        tmp_path: Path,
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
        # H6: a secret decoding to < 32 bytes is now refused in start() (BEFORE
        # the deployment_id is derived and the BufferStore is constructed), so
        # start() RAISES rather than the old background-thread _main exit. This is
        # what stops an invalid secret from purging/restamping pending buffer data
        # before the runtime rejects the key.
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
        with pytest.raises(RuntimeError, match="refusing to start"):
            runtime.start()


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

    def test_loop_death_during_setup_reports_stopped_not_running_r7_p1_10(
        self, tmp_path: Path
    ) -> None:
        # If the event loop dies during _main setup (here an engine's
        # connect_signals raises, before _main publishes steady-state
        # readiness), start() must RAISE and leave the runtime STOPPED -- never
        # report RUNNING on a dead loop. Previously _loop_ready was set the moment
        # the loop object was constructed, so start() returned success on a loop
        # that then crashed on the way up.
        from z4j_bare.runtime import RuntimeState

        class ExplodingEngine(FakeEngine):
            def connect_signals(self, loop: Any = None) -> None:
                raise RuntimeError("connect_signals boom")

        config = _make_config(
            tmp_path=tmp_path,
            dev_mode=False,
            hmac_secret=secrets.token_hex(32),
        )
        runtime = AgentRuntime(
            config=config,
            framework=FakeFramework(),
            engines=[ExplodingEngine()],
        )
        with pytest.raises(RuntimeError):
            runtime.start()
        # The RM7 guard reset state to STOPPED so a later start() genuinely
        # retries instead of no-opping on a half-built RUNNING runtime.
        assert runtime._state == RuntimeState.STOPPED

    def test_concurrent_stop_not_overwritten_by_start_r8_h12(self, tmp_path: Path) -> None:
        # If a stop() moves STARTING -> STOPPED while _start_bringup runs
        # (a slow connect_signals), start() must NOT publish RUNNING -- it aborts
        # and stays STOPPED rather than leaving a RUNNING runtime a stop already
        # applied to.
        from z4j_bare.runtime import RuntimeState

        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=secrets.token_hex(32))
        runtime = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])
        orig_bringup = runtime._start_bringup

        def _bringup_then_concurrent_stop(dep: str) -> None:
            orig_bringup(dep)
            # Simulate a stop() landing during bring-up.
            with runtime._state_lock:
                runtime._state = RuntimeState.STOPPED

        runtime._start_bringup = _bringup_then_concurrent_stop  # type: ignore[method-assign]
        runtime.start()
        try:
            assert runtime._state == RuntimeState.STOPPED  # NOT RUNNING
        finally:
            runtime.stop(timeout=2.0)

    def test_abort_start_signals_loop_stop_r8_h11(self, tmp_path: Path) -> None:
        # _abort_start must signal the loop to STOP (so a loop blocked in
        # setup tears itself down) BEFORE dropping the thread handle, instead of
        # leaving an untracked live loop a retry could overlap.
        from z4j_bare.runtime import RuntimeState

        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=secrets.token_hex(32))
        runtime = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])
        scheduled: list[Any] = []

        class _FakeLoop:
            def call_soon_threadsafe(self, fn: Any, *a: Any) -> None:
                scheduled.append(fn)

        class _FakeEvent:
            def set(self) -> None:  # pragma: no cover - referenced, not called
                pass

        runtime._loop = _FakeLoop()  # type: ignore[assignment]
        runtime._stop_event = _FakeEvent()  # type: ignore[assignment]
        runtime._abort_start()
        # The cooperative stop event's set was scheduled onto the loop.
        assert scheduled == [runtime._stop_event.set]
        assert runtime._state == RuntimeState.STOPPED

    def test_superseded_start_does_not_teardown_new_owner_r9_h8(self, tmp_path: Path) -> None:
        # A start() superseded by a NEWER start (epoch bumped while it was
        # in bring-up) must NOT publish RUNNING over the new owner NOR tear down
        # the new owner's handles -- it returns cleanly.
        from z4j_bare.runtime import RuntimeState

        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=secrets.token_hex(32))
        runtime = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])
        orig_bringup = runtime._start_bringup

        def _bringup_then_superseded(dep: str) -> None:
            orig_bringup(dep)
            # Simulate a concurrent stop()+restart winning a newer epoch and
            # publishing its own RUNNING while we were blocked in bring-up.
            with runtime._state_lock:
                runtime._start_epoch += 1
                runtime._state = RuntimeState.RUNNING

        runtime._start_bringup = _bringup_then_superseded  # type: ignore[method-assign]
        runtime.start()
        try:
            assert runtime._state == RuntimeState.RUNNING  # new owner intact
            # The superseded start did NOT run _abort_start (which would have
            # closed + nulled the buffer the new owner is using).
            assert runtime._buffer is not None
        finally:
            runtime.stop(timeout=2.0)

    def test_stop_reaches_stopped_when_loop_wakeup_raises_r9_m7(self, tmp_path: Path) -> None:
        # If call_soon_threadsafe raises (the loop closed mid-teardown
        # before self._loop was cleared), stop() must still reach STOPPED, not
        # strand in STOPPING (which would block every later stop() and start()).
        import asyncio

        from z4j_bare.runtime import RuntimeState

        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=secrets.token_hex(32))
        runtime = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])

        class _ClosedLoop:
            def call_soon_threadsafe(self, fn: Any, *a: Any) -> None:
                raise RuntimeError("Event loop is closed")

        runtime._state = RuntimeState.RUNNING
        runtime._loop = _ClosedLoop()  # type: ignore[assignment]
        runtime._stop_event = asyncio.Event()
        runtime._thread = None
        runtime._buffer = None
        runtime.stop(timeout=1.0)
        assert runtime._state == RuntimeState.STOPPED

    def test_stop_bounds_buffer_close_by_remaining_budget_r7_p2_8(self, tmp_path: Path) -> None:
        # Stop(timeout=T) is a TOTAL budget. Time spent joining the
        # loop thread must be SUBTRACTED from the lock_timeout handed to
        # buffer.close(), so a wedged daemon orphan-scan cannot make stop()
        # overrun T (join up to T, THEN close up to T = 2T previously).
        import time

        from z4j_bare.runtime import RuntimeState

        class _SlowJoinThread:
            def join(self, timeout: float | None = None) -> None:
                time.sleep(0.2)

            def is_alive(self) -> bool:
                return False

        captured: dict[str, float] = {}

        class _FakeBuffer:
            closed = False

            def close(self, lock_timeout: float | None = None) -> None:
                captured["lock_timeout"] = lock_timeout

        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=secrets.token_hex(32))
        runtime = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])
        runtime._state = RuntimeState.RUNNING
        runtime._thread = _SlowJoinThread()  # type: ignore[assignment]
        runtime._loop = None
        runtime._stop_event = None
        runtime._buffer = _FakeBuffer()  # type: ignore[assignment]

        runtime.stop(timeout=1.0)

        # The 0.2s join was charged against the 1.0s budget, so close() got
        # STRICTLY less than the full budget (and never a negative value).
        assert "lock_timeout" in captured
        assert 0.0 <= captured["lock_timeout"] <= 1.0 - 0.15


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


class TestReinitAfterForkB6:
    """B6: under gunicorn/uWSGI --preload the app (and agent) is installed
    once in the arbiter, then forked into workers. Threads don't survive a
    fork, so the workers inherit a dead agent. ``reinit_after_fork`` (wired
    via ``z4j_bare.post_fork()``) forces a clean restart in the child with a
    per-PID buffer path.
    """

    def test_reinit_resets_state_reresolves_buffer_and_restarts(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from z4j_bare.runtime import RuntimeState

        config = _make_config(tmp_path=tmp_path, dev_mode=True, hmac_secret=secrets.token_hex(32))
        runtime = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])

        # Simulate the inherited-from-parent state: looks RUNNING, threads dead.
        runtime._state = RuntimeState.RUNNING
        original_buffer = runtime.config.buffer_path

        started: list[str] = []

        def _fake_start() -> None:
            started.append("start")
            # start() would normally require STOPPED; assert reinit reset it.
            assert runtime._state == RuntimeState.STOPPED

        monkeypatch.setattr(runtime, "start", _fake_start)
        runtime.reinit_after_fork()

        assert started == ["start"]
        # Buffer path re-resolved to a per-PID file (not the parent's).
        assert runtime.config.buffer_path != original_buffer
        assert f"buffer-{__import__('os').getpid()}.sqlite" in str(runtime.config.buffer_path)

    def test_post_fork_without_installed_agent_returns_none(self) -> None:
        from z4j_bare import post_fork
        from z4j_bare._process_singleton import clear_runtime

        clear_runtime()
        assert post_fork() is None


class TestDeploymentFingerprintRH5:
    """RH5 regression guard: the H8 deployment fingerprint MUST derive from the
    UNMASKED secret. str(SecretStr) is the literal '**********' for every secret,
    which made the fingerprint a single constant across all deployments and
    defeated H8 cross-deployment orphan isolation. This exact regression shipped
    once and had no coverage; reverting the fix must now fail here."""

    def test_distinct_secrets_give_distinct_fingerprints(self) -> None:
        from z4j_bare.runtime import _derive_deployment_id

        a = _derive_deployment_id(SecretStr("secret-A"))
        b = _derive_deployment_id(SecretStr("secret-B"))
        # If _derive_deployment_id used str(SecretStr) (the regression), both
        # would equal sha256('**********')[:16] and this would fail.
        assert a != b

    def test_stable_per_secret(self) -> None:
        from z4j_bare.runtime import _derive_deployment_id

        # Same secret -> same fingerprint (so a same-deployment restart's orphan
        # buffer still matches and is adopted).
        assert _derive_deployment_id(SecretStr("secret-A")) == _derive_deployment_id(
            SecretStr("secret-A")
        )

    def test_not_the_masked_constant(self) -> None:
        import hashlib

        from z4j_bare.runtime import _derive_deployment_id

        masked = hashlib.sha256(b"**********").hexdigest()[:16]
        assert _derive_deployment_id(SecretStr("secret-A")) != masked
        assert _derive_deployment_id(SecretStr("secret-B")) != masked

    def test_invariant_to_secret_encoding_form(self) -> None:
        # runtime:152: padded / unpadded / whitespace-wrapped forms of the SAME
        # urlsafe-base64 secret decode to the same key bytes (the HMAC identity),
        # so they must map to the SAME deployment fingerprint -- otherwise one
        # deployment's own workers refuse to adopt each other's buffers.
        import base64

        from z4j_bare.runtime import _derive_deployment_id

        key = base64.urlsafe_b64encode(b"0123456789abcdef").decode()  # padded
        canonical = _derive_deployment_id(SecretStr(key))
        assert _derive_deployment_id(SecretStr(key.rstrip("="))) == canonical
        assert _derive_deployment_id(SecretStr(f"  {key}\n")) == canonical


class TestPeriodicOrphanAdoptionRH8:
    """RH8 follow-up: the _periodic_orphan_adoption runtime task must re-run the
    orphan scan on a cadence and shut down cleanly on stop. Exercises the ACTUAL
    repaired code (the runtime task), not the pre-existing lease gate."""

    def test_start_is_ready_before_slow_orphan_classification(self, tmp_path, monkeypatch) -> None:
        """C fresh-first: old-buffer classification cannot gate startup."""
        import threading

        from z4j_bare import buffer as buffer_mod
        from z4j_bare import runtime as runtime_mod
        from z4j_bare.runtime import AgentRuntime, RuntimeState

        scan_started = threading.Event()
        release_scan = threading.Event()
        start_done = threading.Event()
        start_errors: list[BaseException] = []

        def _slow_adopt(buffer, *, home_dir):
            scan_started.set()
            release_scan.wait(timeout=5.0)
            return 0

        monkeypatch.setattr(buffer_mod, "adopt_orphaned_buffers", _slow_adopt)
        # Keep the test mutation-valid against the old synchronous import.
        monkeypatch.setattr(
            runtime_mod,
            "adopt_orphaned_buffers",
            _slow_adopt,
            raising=False,
        )

        config = _make_config(
            tmp_path=tmp_path,
            dev_mode=True,
            hmac_secret=secrets.token_hex(32),
        )
        runtime = AgentRuntime(
            config=config,
            framework=FakeFramework(),
            engines=[FakeEngine()],
        )

        def _start() -> None:
            try:
                runtime.start()
            except BaseException as exc:
                start_errors.append(exc)
            finally:
                start_done.set()

        starter = threading.Thread(target=_start, daemon=True)
        starter.start()
        try:
            assert scan_started.wait(timeout=2.0), "recovery scan never started"
            assert start_done.wait(timeout=1.0), (
                "startup waited for old-buffer classification instead of publishing the fresh sink"
            )
            assert not start_errors
            assert runtime.state == RuntimeState.RUNNING
            assert runtime._loop_ready.is_set()
        finally:
            release_scan.set()
            starter.join(timeout=2.0)
            runtime.stop(timeout=2.0)

    async def test_first_scan_is_immediate_then_waits_for_cadence(
        self, tmp_path, monkeypatch
    ) -> None:
        """The fresh-first move must not delay initial recovery by one interval."""
        import asyncio
        from types import SimpleNamespace

        from z4j_bare import buffer as buffer_mod
        from z4j_bare.runtime import AgentRuntime

        monkeypatch.setattr(buffer_mod, "_ORPHAN_RESCAN_SECONDS", 3600.0)
        called = asyncio.Event()

        def _fake_adopt(buffer, *, home_dir):
            called_loop.call_soon_threadsafe(called.set)
            return 0

        monkeypatch.setattr(buffer_mod, "adopt_orphaned_buffers", _fake_adopt)
        called_loop = asyncio.get_running_loop()
        rt = AgentRuntime.__new__(AgentRuntime)
        rt._stop_event = asyncio.Event()
        buf = SimpleNamespace(path=tmp_path / "buffer-1.sqlite")

        task = asyncio.create_task(rt._periodic_orphan_adoption(buf))
        await asyncio.wait_for(called.wait(), timeout=1.0)
        assert not task.done()
        rt._stop_event.set()
        await asyncio.wait_for(task, timeout=1.0)

    async def test_shutdown_during_inflight_initial_scan_preserves_source(
        self, tmp_path, monkeypatch
    ) -> None:
        """Cancelling the first scan cannot acknowledge rows into a closed sink."""
        import asyncio
        import os
        import sqlite3
        import threading
        import time

        from z4j_bare import buffer as buffer_mod
        from z4j_bare.buffer import BufferStore
        from z4j_bare.runtime import AgentRuntime

        if buffer_mod.fcntl is None:
            pytest.skip("requires recovery possession")

        source = tmp_path / "buffer-410050.sqlite"
        old = BufferStore(
            source,
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )
        old.append("task.event", b"recover-me")
        old.close()
        conn = sqlite3.connect(str(source))
        try:
            conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                (buffer_mod._LEASE_HEARTBEAT_KEY, "0"),
            )
            conn.commit()
        finally:
            conn.close()
        old_mtime = time.time() - 120
        os.utime(source, (old_mtime, old_mtime))

        current = BufferStore(
            tmp_path / "buffer-410051.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )
        active = current.path
        append_entered = threading.Event()
        release_append = threading.Event()
        scan_finished = threading.Event()
        real_append = current._append_recovered
        real_adopt = buffer_mod.adopt_orphaned_buffers

        def blocked_append(kind: str, payload: bytes) -> bool:
            append_entered.set()
            release_append.wait(timeout=5.0)
            return real_append(kind, payload)

        def tracked_adopt(buffer: BufferStore, *, home_dir: Path) -> int:
            try:
                return real_adopt(buffer, home_dir=home_dir)
            finally:
                scan_finished.set()

        monkeypatch.setattr(current, "_append_recovered", blocked_append)
        monkeypatch.setattr(buffer_mod, "adopt_orphaned_buffers", tracked_adopt)
        rt = AgentRuntime.__new__(AgentRuntime)
        rt._stop_event = asyncio.Event()
        task = asyncio.create_task(rt._periodic_orphan_adoption(current))
        try:
            assert await asyncio.to_thread(append_entered.wait, 2.0)
            rt._stop_event.set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            current.close(lock_timeout=0.2)
            assert not active.exists()
            release_append.set()
            assert await asyncio.to_thread(scan_finished.wait, 2.0)

            assert source.exists()
            check = sqlite3.connect(str(source))
            try:
                assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
            finally:
                check.close()
        finally:
            release_append.set()
            if not current.closed:
                current.close()

    async def test_rescans_then_stops_cleanly(self, tmp_path, monkeypatch) -> None:
        import asyncio
        from types import SimpleNamespace

        from z4j_bare import buffer as buffer_mod
        from z4j_bare.runtime import AgentRuntime

        # Tiny cadence so a few ticks run fast; count re-scans.
        monkeypatch.setattr(buffer_mod, "_ORPHAN_RESCAN_SECONDS", 0.01)
        calls = {"n": 0}

        def _fake_adopt(buffer, *, home_dir):
            calls["n"] += 1
            return 0

        monkeypatch.setattr(buffer_mod, "adopt_orphaned_buffers", _fake_adopt)

        rt = AgentRuntime.__new__(AgentRuntime)  # bypass __init__
        rt._stop_event = asyncio.Event()
        buf = SimpleNamespace(path=tmp_path / "buffer-1.sqlite")

        task = asyncio.create_task(rt._periodic_orphan_adoption(buf))
        for _ in range(40):
            await asyncio.sleep(0.005)
            if calls["n"] >= 2:
                break
        # The periodic re-scan actually ran (the repair's behavior). Reverting
        # the runtime task hunk removes the method -> this test errors.
        assert calls["n"] >= 1
        # Clean shutdown on stop (no leak, no cancel).
        rt._stop_event.set()
        await asyncio.wait_for(task, timeout=1.0)
        assert task.done() and not task.cancelled()

    async def test_rescan_swallows_adopt_errors(self, tmp_path, monkeypatch) -> None:
        # A scan failure must NOT crash the runtime (agent-never-crashes rule).
        import asyncio
        from types import SimpleNamespace

        from z4j_bare import buffer as buffer_mod
        from z4j_bare.runtime import AgentRuntime

        monkeypatch.setattr(buffer_mod, "_ORPHAN_RESCAN_SECONDS", 0.01)

        def _boom(buffer, *, home_dir):
            raise RuntimeError("scan blew up")

        monkeypatch.setattr(buffer_mod, "adopt_orphaned_buffers", _boom)
        rt = AgentRuntime.__new__(AgentRuntime)
        rt._stop_event = asyncio.Event()
        buf = SimpleNamespace(path=tmp_path / "buffer-1.sqlite")
        task = asyncio.create_task(rt._periodic_orphan_adoption(buf))
        await asyncio.sleep(0.05)  # let a failing scan happen
        assert not task.done()  # survived the error, still looping
        rt._stop_event.set()
        await asyncio.wait_for(task, timeout=1.0)
        assert task.done() and not task.cancelled()


class TestLegacyScheduleSnapshotSuppressionBoundaryD:
    """No schedule inventory is observed before the Brain assigns an epoch.

    The mutation-grade serialization guard now lives in
    ``test_external_schedule_runtime_boundary_d.py`` on the sequenced path.
    These older call shapes remain useful as compatibility oracles: neither a
    malformed nor a valid inventory may escape through the removed legacy
    unsequenced emitter.
    """

    async def test_unserializable_schedule_aborts_snapshot(self) -> None:
        from z4j_bare.runtime import AgentRuntime

        class _GoodSched:
            def model_dump(self, mode: str = "python") -> dict[str, object]:
                return {"id": "good", "name": "good"}

        class _BadSched:
            def model_dump(self, mode: str = "python") -> dict[str, object]:
                raise TypeError("bytes is not JSON serializable")

        class _SchedulerStub:
            name = "apscheduler"

            async def list_schedules(self) -> list[object]:
                return [_GoodSched(), _BadSched()]

        recorded: list[Any] = []
        rt = AgentRuntime.__new__(AgentRuntime)  # bypass __init__
        rt.record_event = recorded.append  # type: ignore[method-assign]

        await rt._emit_schedule_snapshot(_SchedulerStub(), reason="test")

        # Nothing emitted: one un-dumpable job aborts the whole cycle rather
        # than shipping an inventory that would false-delete it.
        assert recorded == []

    async def test_all_serializable_emits_full_snapshot(self) -> None:
        from z4j_bare.runtime import AgentRuntime

        class _GoodSched:
            def __init__(self, sid: str) -> None:
                self._sid = sid

            def model_dump(self, mode: str = "python") -> dict[str, object]:
                return {"id": self._sid, "name": self._sid}

        class _SchedulerStub:
            name = "apscheduler"

            async def list_schedules(self) -> list[object]:
                return [_GoodSched("a"), _GoodSched("b")]

        recorded: list[Any] = []
        rt = AgentRuntime.__new__(AgentRuntime)
        rt.record_event = recorded.append  # type: ignore[method-assign]

        await rt._emit_schedule_snapshot(_SchedulerStub(), reason="test")

        assert recorded == []


class TestAwaitInDaemonThreadM5:
    """M5: blocking off-loop work (the orphan scan) runs on a DAEMON thread so a
    wedged call can never be atexit-joined and hang process exit."""

    async def test_runs_on_daemon_thread_and_returns_result(self) -> None:
        import threading

        from z4j_bare.runtime import _await_in_daemon_thread

        seen = {}

        def _work() -> int:
            seen["daemon"] = threading.current_thread().daemon
            seen["is_main"] = threading.current_thread() is threading.main_thread()
            return 42

        result = await _await_in_daemon_thread(_work)
        assert result == 42
        assert seen["daemon"] is True  # never atexit-joined
        assert seen["is_main"] is False  # ran off the event loop

    async def test_propagates_exception(self) -> None:
        from z4j_bare.runtime import _await_in_daemon_thread

        def _boom() -> None:
            raise ValueError("scan blew up")

        with pytest.raises(ValueError, match="scan blew up"):
            await _await_in_daemon_thread(_boom)


class TestBufferInitFailureResetsStateRM7:
    """RM7: a BufferStore construction failure must reset state to STOPPED so
    a later start() (e.g. after a transient disk-full recovers) genuinely
    retries instead of no-oping at the STOPPED guard with _buffer=None."""

    def test_construction_failure_leaves_stopped_and_retryable(self, tmp_path, monkeypatch) -> None:
        import secrets

        from z4j_bare import runtime as runtime_mod
        from z4j_bare.runtime import AgentRuntime, RuntimeState

        config = _make_config(
            tmp_path=tmp_path, dev_mode=False, hmac_secret=secrets.token_urlsafe(48)
        )
        rt = AgentRuntime(config=config, framework=FakeFramework(), engines=[FakeEngine()])
        calls = {"n": 0}

        def _boom(*_a, **_k):
            calls["n"] += 1
            raise OSError("disk full")

        monkeypatch.setattr(runtime_mod, "BufferStore", _boom)

        with pytest.raises(OSError, match="disk full"):
            rt.start()
        assert rt._state == RuntimeState.STOPPED  # RM7: not stranded in STARTING

        # A later start() RETRIES construction (proves it did not short-circuit
        # at the STOPPED guard with a leaked STARTING state / _buffer=None).
        with pytest.raises(OSError, match="disk full"):
            rt.start()
        assert calls["n"] == 2
        assert rt._buffer is None


class TestDaemonThreadCancelSettleRL3:
    """RL3: cancelling _await_in_daemon_thread must not raise InvalidStateError
    when the worker later completes and tries to settle the cancelled future."""

    async def test_settle_after_cancel_does_not_error(self) -> None:
        import asyncio
        import threading

        from z4j_bare.runtime import _await_in_daemon_thread

        errors: list[dict] = []
        asyncio.get_running_loop().set_exception_handler(lambda _loop, ctx: errors.append(ctx))
        started = threading.Event()
        proceed = threading.Event()

        def _work() -> int:
            started.set()
            proceed.wait(2.0)  # hold until we have cancelled the awaiter
            return 7

        task = asyncio.ensure_future(_await_in_daemon_thread(_work))
        await asyncio.to_thread(started.wait, 1.0)  # worker is running
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        proceed.set()  # worker now finishes and settles the CANCELLED future
        await asyncio.sleep(0.1)
        # RL3: the settle is skipped (fut already done), so no InvalidStateError
        # reaches the loop exception handler.
        assert not any(isinstance(c.get("exception"), asyncio.InvalidStateError) for c in errors), (
            errors
        )
