"""Unit tests for ``z4j_bare.buffer.BufferStore``."""

from __future__ import annotations

from pathlib import Path

import pytest
from z4j_bare.buffer import BufferStore


@pytest.fixture
def buffer_path(tmp_path: Path) -> Path:
    return tmp_path / "buf.sqlite"


@pytest.fixture
def buf(buffer_path: Path) -> BufferStore:
    store = BufferStore(path=buffer_path, max_entries=100, max_bytes=100_000)
    yield store
    store.close()


class TestBasics:
    def test_new_buffer_is_empty(self, buf: BufferStore) -> None:
        assert buf.size() == 0
        assert buf.byte_size() == 0
        assert buf.drain(10) == []

    def test_append_returns_incrementing_id(self, buf: BufferStore) -> None:
        id1 = buf.append("event_batch", b'{"events": []}')
        id2 = buf.append("event_batch", b'{"events": []}')
        assert id2 > id1

    def test_size_reflects_appends(self, buf: BufferStore) -> None:
        for i in range(5):
            buf.append("event_batch", f'{{"i":{i}}}'.encode())
        assert buf.size() == 5

    def test_byte_size_reflects_content(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"x" * 100)
        buf.append("event_batch", b"y" * 200)
        assert buf.byte_size() == 300


class TestDrainConfirm:
    def test_drain_returns_oldest_first(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        buf.append("event_batch", b"b")
        buf.append("event_batch", b"c")

        entries = buf.drain(10)
        assert [e.payload for e in entries] == [b"a", b"b", b"c"]

    def test_drain_respects_limit(self, buf: BufferStore) -> None:
        for i in range(5):
            buf.append("event_batch", str(i).encode())
        entries = buf.drain(3)
        assert len(entries) == 3

    def test_drain_without_confirm_is_idempotent(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        first = buf.drain(10)
        second = buf.drain(10)
        assert [e.id for e in first] == [e.id for e in second]

    def test_confirm_removes_entries(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        buf.append("event_batch", b"b")
        entries = buf.drain(10)
        buf.confirm([entries[0].id])
        remaining = buf.drain(10)
        assert len(remaining) == 1
        assert remaining[0].payload == b"b"

    def test_confirm_empty_list_is_noop(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        buf.confirm([])
        assert buf.size() == 1

    def test_drain_with_zero_raises(self, buf: BufferStore) -> None:
        with pytest.raises(ValueError):
            buf.drain(0)

    def test_drain_with_negative_raises(self, buf: BufferStore) -> None:
        with pytest.raises(ValueError):
            buf.drain(-1)


class TestAttempts:
    def test_new_entry_has_zero_attempts(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        entries = buf.drain(10)
        assert entries[0].attempts == 0

    def test_increment_attempts(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        entries = buf.drain(10)
        buf.increment_attempts([entries[0].id])
        buf.increment_attempts([entries[0].id])
        entries = buf.drain(10)
        assert entries[0].attempts == 2

    def test_increment_empty_is_noop(self, buf: BufferStore) -> None:
        buf.increment_attempts([])

    def test_evict_if_exhausted_drops_at_cap(self, buf: BufferStore) -> None:
        """R5-M2 / R7-MED: only the TARGETED id at the cap is dropped."""
        keep = buf.append("event_batch", b"keep")
        poison = buf.append("event_batch", b"poison")
        for _ in range(5):
            buf.increment_content_rejects([poison])
        dropped = buf.evict_if_exhausted([poison], 5)
        assert dropped == 1
        remaining = buf.drain(10)
        assert [e.id for e in remaining] == [keep]
        assert buf.size() == 1

    def test_attempts_metric_never_triggers_eviction(self, buf: BufferStore) -> None:
        """R8-H1: eviction consults the DEDICATED ``content_rejects`` budget,
        never the ``attempts`` metric. A WS ack-watchdog (or a pre-1.7
        cross-version entry) that accumulated many ``attempts`` must NOT be
        dropped on its first content reject -- it starts the content budget
        fresh at 0."""
        e = buf.append("event_batch", b"stuck")
        for _ in range(50):
            buf.increment_attempts([e])  # heavy WS-timeout / legacy history
        # attempts=50 but content_rejects=0, so nothing drops:
        assert buf.evict_if_exhausted([e], 10) == 0
        assert buf.size() == 1
        # One content reject leaves it at 1 -- still below the 10 cap:
        buf.increment_content_rejects([e])
        assert buf.evict_if_exhausted([e], 10) == 0
        assert buf.size() == 1

    def test_evict_if_exhausted_below_cap_keeps(self, buf: BufferStore) -> None:
        e = buf.append("event_batch", b"a")
        buf.increment_content_rejects([e])
        assert buf.evict_if_exhausted([e], 5) == 0
        assert buf.size() == 1

    def test_evict_if_exhausted_zero_cap_noop(self, buf: BufferStore) -> None:
        e = buf.append("event_batch", b"a")
        assert buf.evict_if_exhausted([e], 0) == 0
        assert buf.size() == 1

    def test_evict_if_exhausted_empty_ids_noop(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        assert buf.evict_if_exhausted([], 5) == 0
        assert buf.size() == 1

    def test_evict_if_exhausted_only_targets_given_ids(
        self,
        buf: BufferStore,
    ) -> None:
        """R7-MED: eviction is ID-targeted, never kind- or cap-scoped.

        The attempt counter is shared, so a flaky connection can push a
        valid sibling (a command_result, or another event_batch) to the
        cap too. Passing ONLY the isolated offending id must drop that id
        alone -- every other at-cap entry is left untouched, so a
        request-level content rejection can never mass-delete valid
        siblings that merely shared the counter.
        """
        offending = buf.append("event_batch", b"eb")
        sibling_eb = buf.append("event_batch", b"eb2")
        cmd = buf.append("command_result", b"cmd")
        for _ in range(5):
            buf.increment_content_rejects([offending, sibling_eb, cmd])
        # Only the isolated offending frame is passed.
        dropped = buf.evict_if_exhausted([offending], 5)
        assert dropped == 1
        remaining = {e.id for e in buf.drain(10)}
        assert offending not in remaining  # the isolated frame is dropped
        assert sibling_eb in remaining  # at-cap sibling event_batch kept
        assert cmd in remaining  # at-cap control frame kept


class TestDrainExcludeInFlight:
    def test_drain_large_exclude_no_param_error(self, buffer_path: Path) -> None:
        """R6-F5: a large in-flight exclude set must not raise SQLite's
        bound-parameter OperationalError.

        A SQL ``NOT IN (?, ?, ...)`` with 33k ids exceeds SQLite's 32766
        variable ceiling and crashed the send loop. drain() now filters
        in Python, so any size is safe.
        """
        buf = BufferStore(path=buffer_path, max_entries=100_000, max_bytes=10**9)
        try:
            wanted = [buf.append("event_batch", str(i).encode()) for i in range(5)]
            # An exclude set far larger than SQLite's 32766 param limit.
            huge_exclude = set(range(1_000_000, 1_000_000 + 40_000))
            entries = buf.drain(5, exclude_ids=huge_exclude)
            assert [e.id for e in entries] == wanted
        finally:
            buf.close()

    def test_drain_excludes_in_flight_ids(self, buf: BufferStore) -> None:
        """R5-M2: in-flight (sent, awaiting ack) entries are skipped so
        the send loop advances to fresh entries instead of re-sending."""
        a = buf.append("event_batch", b"a")
        b = buf.append("event_batch", b"b")
        c = buf.append("event_batch", b"c")
        # a + b are 'in flight'; the next drain must return only c.
        entries = buf.drain(10, exclude_ids={a, b})
        assert [e.id for e in entries] == [c]

    def test_drain_exclude_none_returns_all(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a")
        buf.append("event_batch", b"b")
        assert len(buf.drain(10, exclude_ids=None)) == 2
        assert len(buf.drain(10, exclude_ids=set())) == 2

    def test_drain_exclude_does_not_starve_beyond_window(
        self,
        buffer_path: Path,
    ) -> None:
        """With a full in-flight window, drain still reaches later entries
        (the pre-fix loop spun on the in-flight prefix forever)."""
        buf = BufferStore(path=buffer_path, max_entries=100, max_bytes=100_000_000)
        try:
            ids = [buf.append("event_batch", str(i).encode()) for i in range(10)]
            in_flight = set(ids[:5])
            entries = buf.drain(3, exclude_ids=in_flight)
            assert [e.id for e in entries] == ids[5:8]
        finally:
            buf.close()


class TestEviction:
    def test_entry_count_limit(self, buffer_path: Path) -> None:
        buf = BufferStore(path=buffer_path, max_entries=5, max_bytes=100_000_000)
        try:
            for i in range(10):
                buf.append("event_batch", str(i).encode())
            assert buf.size() <= 5
            # The LAST 5 should have survived.
            remaining = buf.drain(10)
            assert [e.payload for e in remaining] == [str(i).encode() for i in range(5, 10)]
        finally:
            buf.close()

    def test_byte_size_limit(self, buffer_path: Path) -> None:
        buf = BufferStore(path=buffer_path, max_entries=1000, max_bytes=300)
        try:
            for _ in range(10):
                buf.append("event_batch", b"x" * 100)
            # At most 3 entries of 100 bytes each fit.
            assert buf.byte_size() <= 300
            assert buf.size() <= 3
        finally:
            buf.close()


class TestPersistence:
    def test_survives_close_and_reopen(self, buffer_path: Path) -> None:
        buf = BufferStore(path=buffer_path)
        buf.append("event_batch", b"survived")
        buf.close()

        buf2 = BufferStore(path=buffer_path)
        try:
            entries = buf2.drain(10)
            assert len(entries) == 1
            assert entries[0].payload == b"survived"
        finally:
            buf2.close()

    def test_migrates_pre_1_7_db_without_content_rejects(self, buffer_path: Path) -> None:
        """R8-H1: a pre-1.7 buffer file has no ``content_rejects`` column and
        may carry a high ``attempts`` (old transport-failure counting). On
        open, the migration adds ``content_rejects`` DEFAULT 0 so the entry
        starts its content-drop budget fresh and is NOT deleted on its first
        content reject.
        """
        import sqlite3

        # Hand-build the OLD schema (no content_rejects) with a high-attempts
        # entry, exactly as a 1.6.9 buffer would leave on disk.
        conn = sqlite3.connect(str(buffer_path), isolation_level=None)
        conn.executescript(
            """
            CREATE TABLE entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO entries (kind, payload, created_at, attempts)
            VALUES ('event_batch', X'6f6c64', 1.0, 50);
            """
        )
        conn.close()

        buf = BufferStore(path=buffer_path)
        try:
            (eid,) = (e.id for e in buf.drain(10))
            # attempts carried over (50) but the fresh content_rejects=0 means
            # a content-reject-budget check does not drop it:
            assert buf.evict_if_exhausted([eid], 10) == 0
            assert buf.size() == 1
        finally:
            buf.close()


class TestClosedStore:
    def test_append_after_close_raises(self, buf: BufferStore) -> None:
        buf.close()
        with pytest.raises(RuntimeError, match="closed"):
            buf.append("event_batch", b"a")

    def test_drain_after_close_returns_empty(self, buf: BufferStore) -> None:
        buf.close()
        assert buf.drain(10) == []

    def test_size_after_close_is_zero(self, buf: BufferStore) -> None:
        buf.close()
        assert buf.size() == 0


class TestSecurityP1FilePermissions:
    """z4j-bare 1.6.5 (security advisory P1): the buffer SQLite DB
    and its WAL/SHM sidecar files MUST be created with owner-only
    permissions (0600). Pre-1.6.5 they inherited the process umask,
    which on multi-tenant POSIX hosts (default umask 022) produced
    a world-readable buffer holding task/event payload BLOBs.
    """

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only test (no chmod on Windows)",
    )
    def test_buffer_db_is_0600_after_create(
        self,
        buffer_path: Path,
    ) -> None:
        import stat

        buf = BufferStore(path=buffer_path)
        try:
            # Write something to force WAL + SHM materialisation
            # (SQLite WAL mode lazily creates sidecar files on first
            # write; if we don't write, the chmod targets may not
            # exist yet -- which is expected and tolerated, but we
            # want to assert the chmod actually fires on real files).
            buf.append("event_batch", b"x" * 32)
        finally:
            buf.close()

        # Primary DB file: must be owner-only.
        db_mode = stat.S_IMODE(buffer_path.stat().st_mode)
        assert db_mode == 0o600, (
            f"1.6.5 P1 regression: buffer DB {buffer_path} has "
            f"mode 0{db_mode:o}, expected 0600. "
            "On a multi-tenant POSIX host this leaks task payload "
            "BLOBs to other local users."
        )

        # WAL sidecar (created on first write since WAL is enabled).
        wal_path = buffer_path.with_suffix(buffer_path.suffix + "-wal")
        if wal_path.exists():
            wal_mode = stat.S_IMODE(wal_path.stat().st_mode)
            assert wal_mode == 0o600, (
                f"1.6.5 P1 regression: WAL sidecar {wal_path} has mode 0{wal_mode:o}, expected 0600"
            )

        # SHM sidecar (memory-mapped index for the WAL).
        shm_path = buffer_path.with_suffix(buffer_path.suffix + "-shm")
        if shm_path.exists():
            shm_mode = stat.S_IMODE(shm_path.stat().st_mode)
            assert shm_mode == 0o600, (
                f"1.6.5 P1 regression: SHM sidecar {shm_path} has mode 0{shm_mode:o}, expected 0600"
            )

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only test",
    )
    def test_loose_z4j_home_emits_warning(
        self,
        buffer_path: Path,
        caplog,
    ) -> None:
        """When the buffer's parent directory is group/world
        accessible, BufferStore startup MUST log a WARN naming the
        path + the remediation command. Operators on multi-tenant
        hosts need this signal."""
        import logging

        # Reset the one-shot guard so this test always exercises the
        # check, even when run after another test that triggered it.
        from z4j_bare import buffer as buffer_module

        buffer_module._z4j_home_perms_warned = False

        # Make the parent dir group-readable (0o755 inclusive of
        # group/world read bits) -- this is the bit pattern the
        # warning is supposed to flag.
        buffer_path.parent.chmod(0o755)

        with caplog.at_level(logging.WARNING, logger="z4j.runtime.buffer"):
            buf = BufferStore(path=buffer_path)
            try:
                pass
            finally:
                buf.close()

        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("group/world accessible" in r.getMessage() for r in warns), (
            "1.6.5 P1: BufferStore must WARN when Z4J_HOME (or the "
            "buffer's parent dir) is group/world accessible. Found "
            f"WARN records: {[r.getMessage() for r in warns]}"
        )

    def test_close_is_idempotent(self, buf: BufferStore) -> None:
        buf.close()
        buf.close()  # must not raise

    def test_append_during_concurrent_close(self, buffer_path: Path) -> None:
        """Race the lock: a concurrent close() must not cause append()
        to silently swallow data. The contract is: append either
        succeeds (entry stored) or raises RuntimeError (closed). It
        must never both fail to store AND fail to raise.
        """
        import threading

        store = BufferStore(path=buffer_path)
        observed: list[str] = []

        def writer() -> None:
            try:
                store.append("event_batch", b"x")
                observed.append("ok")
            except RuntimeError:
                observed.append("closed")

        def closer() -> None:
            store.close()

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=closer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert observed and observed[0] in ("ok", "closed")


class TestCachedTotals:
    def test_size_cache_stays_in_sync(self, buf: BufferStore) -> None:
        for i in range(10):
            buf.append("event_batch", f'{{"i":{i}}}'.encode())
        # Confirm half of them; size must reflect the deletion.
        ids_to_drop = [e.id for e in buf.drain(10)[:5]]
        buf.confirm(ids_to_drop)
        assert buf.size() == 5

    def test_byte_size_cache_stays_in_sync(self, buf: BufferStore) -> None:
        buf.append("event_batch", b"a" * 100)
        buf.append("event_batch", b"b" * 50)
        assert buf.byte_size() == 150
        confirm_id = buf.drain(10)[0].id
        buf.confirm([confirm_id])
        assert buf.byte_size() == 50


class TestDriftRecovery:
    """Tests for the drift-detection / reconcile code path.

    The live Docker stack on 2026-04-21 observed
    ``HeartbeatPayload`` validation failures because
    ``_cached_count`` drifted to ``-12`` - breaking the
    heartbeat's ``buffer_size >= 0`` invariant and forcing
    reconnect churn. These tests pin the fix so the heartbeat
    can never again be crashed by a bad cache value.
    """

    def test_size_clamps_to_zero_when_cache_goes_negative(
        self,
        buf: BufferStore,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Simulate the drift observed in production.
        buf._cached_count = -12
        with caplog.at_level("WARNING", logger="z4j.runtime.buffer"):
            assert buf.size() == 0
        assert any("drifted negative" in rec.message for rec in caplog.records), (
            "expected WARNING about drifted counters"
        )

    def test_byte_size_clamps_to_zero_when_cache_goes_negative(
        self,
        buf: BufferStore,
    ) -> None:
        buf._cached_bytes = -4096
        assert buf.byte_size() == 0

    def test_reconcile_uses_disk_truth(
        self,
        buf: BufferStore,
    ) -> None:
        # Seed real entries, then corrupt the cache; the
        # reconcile path must return the disk-truth value, not
        # the clamped zero.
        for i in range(7):
            buf.append("event_batch", f"row-{i}".encode())
        buf._cached_count = -99
        buf._cached_bytes = -99
        assert buf.size() == 7
        # Reconcile also fixes byte_size - next call returns the
        # real disk-sourced bytes total, no longer clamped.
        real_bytes = sum(len(f"row-{i}".encode()) for i in range(7))
        assert buf.byte_size() == real_bytes

    def test_drift_warning_fires_only_once_per_instance(
        self,
        buf: BufferStore,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # A persistent bug would otherwise spam the log every
        # heartbeat (10s cadence). One warning per BufferStore
        # lifetime is enough signal for operators.
        with caplog.at_level("WARNING", logger="z4j.runtime.buffer"):
            buf._cached_count = -1
            buf.size()
            buf._cached_count = -5
            buf.size()
            buf._cached_count = -42
            buf.size()
        drift_logs = [rec for rec in caplog.records if "drifted negative" in rec.message]
        assert len(drift_logs) == 1


class TestDrainCloseRace:
    """Regression for the teardown race surfaced by the 1.6.9 Docker
    e2e: a short-lived process exiting while the send loop is mid-drain
    let ``stop()`` close the buffer on another thread between
    ``drain()``'s pre-lock ``_closed`` check and its ``execute``,
    raising ``sqlite3.ProgrammingError: Cannot operate on a closed
    database``. The fix moves the ``_closed`` check INSIDE the lock
    (matching ``confirm`` / ``size`` / ``byte_size``)."""

    def test_drain_after_close_returns_empty(self, buffer_path: Path) -> None:
        store = BufferStore(path=buffer_path, max_entries=100, max_bytes=100_000)
        store.append("event_batch", b'{"events": []}')
        store.close()
        # A fully-closed buffer drains to nothing; must never raise.
        assert store.drain(10) == []

    def test_drain_checks_closed_inside_lock(self, buffer_path: Path) -> None:
        # Simulate the EXACT interleaving: a concurrent stop() closes the
        # buffer the moment drain() enters its critical section. With the
        # old pre-lock check this raised ProgrammingError on the closed
        # connection; with the in-lock re-check drain() returns [].
        store = BufferStore(path=buffer_path, max_entries=100, max_bytes=100_000)
        store.append("event_batch", b'{"events": []}')

        real_lock = store._lock
        state = {"closed_once": False}

        class _ClosingLock:
            def __enter__(self) -> object:
                real_lock.acquire()
                if not state["closed_once"]:
                    state["closed_once"] = True
                    # Mimic stop() closing the buffer from another thread
                    # right as drain() takes the lock. Done directly (not
                    # via close(), which would re-acquire and deadlock).
                    store._closed = True
                    store._conn.close()
                return real_lock

            def __exit__(self, *_exc: object) -> bool:
                real_lock.release()
                return False

        store._lock = _ClosingLock()  # type: ignore[assignment]
        assert store.drain(10) == []
