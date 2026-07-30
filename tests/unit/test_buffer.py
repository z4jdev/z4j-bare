"""Unit tests for ``z4j_bare.buffer.BufferStore``."""

from __future__ import annotations

from pathlib import Path

import pytest
from z4j_bare import buffer as buffer_mod
from z4j_bare.buffer import BufferStore


def _age_lease(path: Path, *, seconds_ago: float = 1000.0) -> None:
    """Make a buffer file's liveness lease STALE so a self-claim on a platform
    without an effective flock (Windows / no-op mount) treats the previous owner
    as dead and proceeds with the reused-pid discard (H4). A fresh lease would
    correctly make it leave the file untouched."""
    import sqlite3
    import time as _t

    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "UPDATE _meta SET value = ? WHERE key = 'lease_heartbeat'",
            (str(_t.time() - seconds_ago),),
        )
        conn.commit()
    finally:
        conn.close()
    old = _t.time() - seconds_ago
    path.touch()
    import os

    os.utime(path, (old, old))


def _write_exact_pre_18_buffer(path: Path, payloads: list[bytes]) -> None:
    """Create the released 1.7 on-disk shape, which predates ``_meta``."""
    import os
    import sqlite3
    import time

    conn = sqlite3.connect(str(path), isolation_level=None)
    try:
        conn.executescript(
            """
            CREATE TABLE entries (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                kind             TEXT    NOT NULL,
                payload          BLOB    NOT NULL,
                created_at       REAL    NOT NULL,
                attempts         INTEGER NOT NULL DEFAULT 0,
                content_rejects  INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX idx_entries_created_at ON entries (created_at);
            """,
        )
        conn.executemany(
            "INSERT INTO entries(kind, payload, created_at, attempts) "
            "VALUES ('task.event', ?, 0, 0)",
            [(payload,) for payload in payloads],
        )
    finally:
        conn.close()
    old = time.time() - 120
    os.utime(path, (old, old))


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
        """Only the TARGETED id at the cap is dropped."""
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
        """Eviction consults the DEDICATED ``content_rejects`` budget,
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
        """Eviction is ID-targeted, never kind- or cap-scoped.

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
        """A large in-flight exclude set must not raise SQLite's
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
        """In-flight (sent, awaiting ack) entries are skipped so
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

    def test_durable_dedup_survives_reopen_r9_h10(self, buffer_path: Path) -> None:
        # A command_id recorded as seen must STILL be a duplicate after the
        # buffer is closed and reopened (an agent restart on a persistent path), so
        # a re-delivered fire is not re-executed. A kept entry stops the empty-file
        # unlink on close (an agent whose result was LOST has un-drained rows, so
        # the file survives -- exactly the case that matters).
        b1 = BufferStore(path=buffer_path)
        b1.append("event_batch", b"keep")
        assert b1.mark_command_seen("cmd-1") is False  # first sighting
        assert b1.mark_command_seen("cmd-1") is True  # duplicate in-process
        b1.close()

        b2 = BufferStore(path=buffer_path)  # reopen the SAME persistent file
        try:
            assert b2.mark_command_seen("cmd-1") is True  # STILL a duplicate
            assert b2.mark_command_seen("cmd-2") is False  # a genuinely new command
        finally:
            b2.close()

    def test_migrates_pre_1_7_db_without_content_rejects(self, buffer_path: Path) -> None:
        """A pre-1.7 buffer file has no ``content_rejects`` column and
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


class TestOrphanAdoptionB8:
    """B8: on restart the new pid opens a fresh ``buffer-<pid>.sqlite`` and
    never reopens the dead pid's file, so buffered-but-undelivered events
    were lost forever and the stale file leaked on disk. Startup adoption
    drains a dead peer's buffer into the live one and removes it -- while
    leaving live siblings untouched.
    """

    def _write_orphan(
        self,
        home: Path,
        pid: int,
        n: int,
        *,
        deployment_id: str | None = None,
        lease_age_seconds: float = 1000.0,
    ) -> Path:
        p = home / f"buffer-{pid}.sqlite"
        ob = BufferStore(
            path=p,
            max_entries=1000,
            max_bytes=10_000_000,
            deployment_id=deployment_id,
        )
        for i in range(n):
            ob.append("task.event", f"payload-{i}".encode())
        ob.close()  # non-empty -> file persists
        import os as _os
        import sqlite3 as _sqlite
        import time as _time

        # RH8: stamp the liveness lease. Default is STALE (models a DEAD owner)
        # so the orphan is adoptable; a small lease_age_seconds models a LIVE
        # owner whose fresh lease must (correctly) block adoption.
        conn = _sqlite.connect(str(p))
        try:
            conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES ('lease_heartbeat', ?)",
                (str(_time.time() - lease_age_seconds),),
            )
            conn.commit()
        finally:
            conn.close()
        # Age the file LAST -- a real dead peer's buffer is never written again
        # after it dies, so its mtime is old. The lease write above (the final
        # disk write) would otherwise reset mtime to now and (correctly) trip the
        # _ADOPT_MIN_AGE_S guard, which skips freshly-written buffers.
        old = _time.time() - 120  # past the mtime guard
        _os.utime(p, (old, old))
        return p

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: real-fd adoption (M1) + unlink-open-file semantics",
    )
    def test_dead_peer_buffer_is_adopted_and_removed(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424242, n=3)
        # Simulate a fully DEAD owner by reporting the owner pid as gone.
        # Adoption must obtain its real production flock + SQLite exclusion
        # capability; a synthetic read-only fd cannot represent that claim.
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)

        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        adopted = buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path)

        assert adopted == 3
        assert live.size() == 3
        assert not orphan.exists()
        live.close()

    def test_deployment_fingerprint_is_stamped_and_read_h8(
        self,
        tmp_path: Path,
    ) -> None:
        # H8: a buffer stamps its deployment fingerprint into _meta, and
        # _read_deployment_id recovers it (platform-independent). A buffer
        # created without a fingerprint reads back None (legacy path).
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        stamped = BufferStore(
            path=tmp_path / "buffer-1.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="dep-abc123",
        )
        stamped.append("task.event", b"x")  # non-empty so close() keeps the file
        assert stamped.deployment_id == "dep-abc123"
        stamped.close()
        conn = sqlite3.connect(str(tmp_path / "buffer-1.sqlite"))
        try:
            assert buffer_mod._read_deployment_id(conn) == "dep-abc123"
        finally:
            conn.close()

        legacy = BufferStore(
            path=tmp_path / "buffer-2.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
        )
        legacy.append("task.event", b"x")
        assert legacy.deployment_id is None
        legacy.close()
        conn2 = sqlite3.connect(str(tmp_path / "buffer-2.sqlite"))
        try:
            assert buffer_mod._read_deployment_id(conn2) is None
        finally:
            conn2.close()

    def test_live_peer_buffer_is_left_untouched(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424243, n=5)
        # A LIVE sibling still holds its lifetime lock, so the claim fails
        # (None) and its buffer must never be stolen.
        monkeypatch.setattr(buffer_mod, "_claim_dead_orphan", lambda path: None)

        live = BufferStore(
            path=tmp_path / f"buffer-{__import__('os').getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        adopted = buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path)

        assert adopted == 0
        assert live.size() == 0
        assert orphan.exists()  # untouched
        live.close()

    def test_recent_orphan_within_grace_is_skipped(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from z4j_bare import buffer as buffer_mod

        # Dead pid but freshly written -> mtime guard defers adoption so we
        # never race a just-started peer our liveness check hasn't seen.
        p = tmp_path / "buffer-424244.sqlite"
        ob = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000)
        ob.append("task.event", b"x")
        ob.close()
        # Even though the owner is claimable (dead), the mtime grace guard
        # (checked BEFORE the claim) must defer adoption of a fresh file.
        monkeypatch.setattr(buffer_mod, "_claim_dead_orphan", lambda path: -1)

        live = BufferStore(
            path=tmp_path / f"buffer-{__import__('os').getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 0
        assert p.exists()
        live.close()

    # ------------------------------------------------------------------
    # RH6: fail-closed deployment attribution on the OWN per-pid file
    # (cross-platform: SQLite-only, no fd/flock)
    # ------------------------------------------------------------------

    def test_reused_pid_foreign_deployment_rows_preserved_rh6(self, tmp_path: Path) -> None:
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-777.sqlite"
        a = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        a.append("task.event", b"x")
        a.append("task.event", b"y")
        a.close()
        _age_lease(p)  # the reused pid means the old owner is dead (stale lease)
        # A DIFFERENT deployment reuses the pid/path -> A's rows must NOT be
        # adopted and re-signed under B.
        b = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-B")
        try:
            assert b.path != p
            assert b.size() == 0
            assert b.deployment_id == "dep-B"
            check = sqlite3.connect(str(p))
            try:
                assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 2
                assert buffer_mod._read_deployment_id(check) == "dep-A"
            finally:
                check.close()
        finally:
            b.close()

    def test_contended_lock_relocates_to_per_pid_rh9(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If the ownership flock is CONTENDED (another LIVE process holds
        # this exact path), __init__ must NOT stay operational on the SHARED file
        # (suppressing only the discard still let it append/drain/unlink the
        # holder's rows -- the H4 corruption). It relocates to a per-pid sibling
        # in the same directory, leaving the holder's file fully intact.
        import os

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-780.sqlite"
        seed = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        seed.append("task.event", b"x")
        seed.append("task.event", b"y")
        seed.close()
        # Force ONLY the shared path to look contended; the per-pid sibling we
        # relocate to is nobody's, so it acquires cleanly. (a per-pid
        # re-acquire that is STILL contended fails closed with
        # BufferOwnershipError -- covered by its own test; here relocation must
        # succeed and operate on the fresh sibling.)
        monkeypatch.setattr(
            buffer_mod,
            "_acquire_own_lock",
            lambda path: (None, True) if Path(path) == p else (None, False),
        )
        mine = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-B")
        try:
            # Relocated to a GLOBALLY-UNIQUE sibling (pid + random tail) in
            # the SAME directory -- not p.
            assert mine.path != p
            assert mine.path.parent == p.parent
            assert mine.path.name.startswith(f"buffer-{os.getpid()}-")
            assert mine.path.suffix == ".sqlite"
            assert mine.size() == 0  # its own fresh file, holder's rows untouched
        finally:
            mine.close()
        # The holder's original file is intact: rows + fingerprint preserved
        # (read directly so reopening does not itself mutate it).
        import sqlite3

        conn = sqlite3.connect(str(p))
        try:
            (count,) = conn.execute("SELECT COUNT(*) FROM entries").fetchone()
            assert count == 2
            assert buffer_mod._read_deployment_id(conn) == "dep-A"
        finally:
            conn.close()

    def test_still_contended_after_relocation_raises_r7_p1_9(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # (a): if the per-pid sibling we relocate to is ALSO held by a
        # live process (a pid-reuse / pid-namespace clash on a shared dir), the
        # store must fail CLOSED with BufferOwnershipError rather than proceed
        # operational on an inode it could not take ownership of. Here every
        # acquire looks contended, so the post-relocation re-acquire is contended
        # too and construction refuses.
        from z4j_bare import buffer as buffer_mod
        from z4j_bare.buffer import BufferOwnershipError

        p = tmp_path / "buffer-781.sqlite"
        seed = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        seed.append("task.event", b"x")
        seed.close()
        monkeypatch.setattr(buffer_mod, "_acquire_own_lock", lambda path: (None, True))
        with pytest.raises(BufferOwnershipError):
            BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-B")
        # The holder's original file is untouched (relocation never wrote to it,
        # and the refusing store never operated on the sibling either).
        import sqlite3

        conn = sqlite3.connect(str(p))
        try:
            (count,) = conn.execute("SELECT COUNT(*) FROM entries").fetchone()
            assert count == 1
            assert buffer_mod._read_deployment_id(conn) == "dep-A"
        finally:
            conn.close()

    @pytest.mark.skipif(buffer_mod.fcntl is None, reason="requires recovery possession")
    def test_reused_path_recovers_same_deployment_into_fresh_sink(self, tmp_path: Path) -> None:
        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-778.sqlite"
        a = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        a.append("task.event", b"x")
        a.close()
        _age_lease(p)

        # Startup publishes a fresh, sealed sink before it classifies any old
        # path. The SAME + READY source is then recovered into that sole sink.
        again = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        try:
            assert again.path != p
            assert again.size() == 0
            assert buffer_mod.adopt_orphaned_buffers(again, home_dir=tmp_path) == 1
            assert again.size() == 1
            assert not p.exists()
        finally:
            again.close()

    @pytest.mark.skipif(buffer_mod.fcntl is None, reason="requires a held ownership flock")
    def test_sqlite_flock_incompatibility_uses_exclusive_generation_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        real_initialize = buffer_mod.BufferStore._initialize_created_file
        calls = {"n": 0}

        def _drvfs_like(self, path, deployment_id):
            calls["n"] += 1
            if calls["n"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return real_initialize(self, path, deployment_id)

        monkeypatch.setattr(
            buffer_mod.BufferStore,
            "_initialize_created_file",
            _drvfs_like,
        )
        p = tmp_path / "buffer-8801.sqlite"
        b = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        try:
            assert calls["n"] == 2
            assert b.path == p
            assert b._lock_fd is None
            b.append("task.event", b"x")
            assert b.size() == 1
        finally:
            b.close()

    def test_real_contention_is_not_mistaken_for_a_self_conflict_r13(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The other half: the probe must not weaken genuine contention. When the
        # lock is still refused with our connection closed, a live peer really
        # does hold it and the relocation must still happen.
        from z4j_bare import buffer as buffer_mod

        monkeypatch.setattr(buffer_mod, "_acquire_own_lock", lambda path: (None, True))
        p = tmp_path / "buffer-8802.sqlite"
        with pytest.raises(buffer_mod.BufferOwnershipError):
            BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")

    @pytest.mark.skipif(buffer_mod.fcntl is None, reason="requires recovery possession")
    def test_untagged_pre_fingerprint_buffer_is_recovered_on_upgrade(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-987654320.sqlite"
        _write_exact_pre_18_buffer(p, [b"x"])
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        b = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-B")
        try:
            assert b.path != p
            assert b.size() == 0
            assert buffer_mod.adopt_orphaned_buffers(b, home_dir=tmp_path) == 1
            assert not p.exists()
            assert b.size() == 1
        finally:
            b.close()

    def test_tagged_foreign_rows_are_preserved_not_discarded_r14(self, tmp_path: Path) -> None:
        # The other half: migrating UNTAGGED buffers must not weaken the
        # cross-deployment protection for buffers that ARE tagged. A stamp that
        # is present and different is still another deployment's data.
        p = tmp_path / "buffer-780.sqlite"
        other = BufferStore(
            path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-OTHER"
        )
        other.append("task.event", b"theirs")
        other.close()
        _age_lease(p)
        mine = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-MINE")
        try:
            # Not emitted -- we relocated to our own file rather than
            # adopting theirs.
            assert mine.size() == 0, "another deployment's rows must not be emitted"
            assert mine.path != p, "we must step aside, not take over their file"
            # ...and NOT deleted. The stamp is derived from the agent's
            # hmac_secret, so a rotated secret makes a deployment's OWN events
            # read as foreign; deleting on that evidence destroyed recoverable
            # data. Inequality justifies refusing to emit, never destroying.
            import sqlite3 as _s

            chk = _s.connect(str(p))
            try:
                assert chk.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
            finally:
                chk.close()
        finally:
            mine.close()

    @pytest.mark.skipif(buffer_mod.fcntl is None, reason="requires recovery possession")
    def test_unreadable_fingerprint_preserves_recovery_source(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-781.sqlite"
        seed = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        seed.append("task.event", b"mine")
        seed.close()
        _age_lease(p)

        real = buffer_mod._read_deployment_id
        calls = {"n": 0}

        def _flaky(conn):
            calls["n"] += 1
            if calls["n"] == 1:
                raise buffer_mod.DeploymentIdUnreadableError("database is locked")
            return real(conn)

        monkeypatch.setattr(buffer_mod, "_read_deployment_id", _flaky)
        b = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000, deployment_id="dep-A")
        try:
            assert b.path != p
            assert b.size() == 0
            assert buffer_mod.adopt_orphaned_buffers(b, home_dir=tmp_path) == 0
            assert calls["n"] >= 1, "the fingerprint read was never exercised"
            assert b.size() == 0
            assert p.exists()
            assert b.recovery_required()[0]["attribution"] == "unknown"
        finally:
            b.close()

    def test_read_deployment_id_distinguishes_failure_from_absence_r13(self) -> None:
        # The primitive the two paths above depend on.
        import sqlite3 as _sqlite3

        from z4j_bare import buffer as buffer_mod

        conn = _sqlite3.connect(":memory:")  # no _meta table -> the SELECT raises
        try:
            with pytest.raises(buffer_mod.DeploymentIdUnreadableError):
                buffer_mod._read_deployment_id(conn)
            # ...while a real table with no row is genuine ABSENCE.
            conn.execute("CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT)")
            assert buffer_mod._read_deployment_id(conn) is None
        finally:
            conn.close()

    def test_no_flock_uses_fresh_sink_and_keeps_foreign_source(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-782.sqlite"
        seed = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        seed.append("task.event", b"x")
        seed.close()
        # Fresh lease (NOT aged) + no flock available.
        monkeypatch.setattr(buffer_mod, "_acquire_own_lock", lambda path: (None, False))
        mine = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-B")
        try:
            assert mine.path != p
            assert mine.size() == 0
            assert buffer_mod._read_deployment_id(mine._conn) == "dep-B"
            check = sqlite3.connect(str(p))
            try:
                assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
                assert buffer_mod._read_deployment_id(check) == "dep-A"
            finally:
                check.close()
        finally:
            mine.close()

    def test_failed_fresh_initialization_rolls_back_unsealed_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-991.sqlite"
        seed = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        seed.append("task.event", b"x")
        seed.close()

        def _fail_initialization(self, path, deployment_id):
            del self, path, deployment_id
            raise RuntimeError("schema migration failed")

        monkeypatch.setattr(
            buffer_mod.BufferStore,
            "_initialize_created_file",
            _fail_initialization,
        )
        with pytest.raises(RuntimeError, match="schema migration failed"):
            BufferStore(
                path=p,
                max_entries=100,
                max_bytes=1_000_000,
                deployment_id="dep-B",
            )

        # The old source remains intact, while the new unsealed sibling and its
        # sidecars are removed before construction reports failure.
        assert p.exists()
        assert list(tmp_path.glob("buffer-*-*.sqlite")) == []

    def test_foreign_buffer_is_never_touched_r14(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # (Supersedes H5). H5 said: if DISCARDING another deployment's rows
        # fails, abort before restamping, so they are never relabelled ours and
        # shipped. We no longer discard them at all, which makes that guarantee
        # unconditional instead of dependent on a failure path: a foreign buffer
        # is neither emitted nor destroyed, and its stamp is left alone.
        #
        # This matters because the stamp is derived from the agent's hmac_secret,
        # so a rotated secret makes a deployment's OWN events read as foreign.
        # Deleting them on that evidence destroyed recoverable data.
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-783.sqlite"
        seed = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        seed.append("task.event", b"x")
        seed.close()
        _age_lease(p)

        def _boom(self, *, reason):  # pragma: no cover - must never be called
            raise AssertionError(
                "a foreign buffer was sent down the discard path; inequality is "
                "not authority to delete another deployment's events"
            )

        monkeypatch.setattr(buffer_mod.BufferStore, "_discard_unowned_rows", _boom)
        mine = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-B")
        try:
            assert mine.path != p, "we must step aside onto our own buffer"
            assert mine.size() == 0
        finally:
            mine.close()

        check = sqlite3.connect(str(p))
        try:
            assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
            row = check.execute("SELECT value FROM _meta WHERE key='deployment'").fetchone()
        finally:
            check.close()
        assert row[0] == "dep-A"  # untouched, not relabelled dep-B

    def test_untagged_own_buffer_kept_when_not_fingerprinting_rh6(self, tmp_path: Path) -> None:
        # A pre-fingerprint agent (no deployment_id) keeps the legacy behaviour:
        # it does not fingerprint, so it cannot and does not discard.
        p = tmp_path / "buffer-780.sqlite"
        legacy = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000)
        legacy.append("task.event", b"x")
        legacy.close()
        again = BufferStore(path=p, max_entries=1000, max_bytes=10_000_000)
        assert again.size() == 1
        again.close()

    # ------------------------------------------------------------------
    # RH6 (orphan path) + RH7 (inode TOCTOU) -- POSIX-only (fd/flock/uid)
    # ------------------------------------------------------------------

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_untagged_pre_fingerprint_orphan_is_adopted_from_private_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = tmp_path / "buffer-424250.sqlite"
        _write_exact_pre_18_buffer(
            orphan,
            [f"payload-{index}".encode() for index in range(4)],
        )
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
            deployment_id="dep-B",
        )
        try:
            assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 4
            assert not orphan.exists()
            assert live.size() == 4
        finally:
            live.close()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_foreign_deployment_orphan_not_adopted_rh6(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424251, n=4, deployment_id="dep-A")
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
            deployment_id="dep-B",
        )
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 0
        assert orphan.exists()
        live.close()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: no-flock adoption pid-liveness gate",
    )
    def test_no_flock_foreign_orphan_not_deleted_b3(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # B3: on a NO-FLOCK platform a FOREIGN deployment's orphan must be
        # left UNTOUCHED, neither drained nor DELETED. The pre-fix order renamed
        # the orphan to a private .adopting name, found the fingerprint foreign,
        # then unlinked it unconditionally, destroying another deployment's
        # undelivered events. The fix reads the fingerprint from a read-only probe
        # BEFORE the rename and skips a foreign orphan without touching it.
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424253, n=4, deployment_id="dep-A")
        before = orphan.read_bytes()
        # Force the no-flock adoption path: no fcntl, and the flock claim returns
        # None so adopt_orphaned_buffers falls through to _adopt_no_flock_orphan.
        monkeypatch.setattr(buffer_mod, "fcntl", None)
        monkeypatch.setattr(buffer_mod, "_claim_dead_orphan", lambda path: None)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
            deployment_id="dep-B",  # DIFFERENT deployment
        )
        adopted = buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path)
        assert adopted == 0
        assert orphan.exists()  # NOT deleted -- the B3 data-loss fix
        assert orphan.read_bytes() == before  # untouched, all 4 events intact
        assert not list(tmp_path.glob("*.adopting-*"))  # no stray claim file
        live.close()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: no-flock adoption pid-liveness gate",
    )
    def test_no_flock_matching_orphan_adopted_and_removed_b3(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Even a SAME-deployment orphan is left alone when file locking is
        # unavailable. A fallback used to drain and unlink it here on a
        # heartbeat-plus-pid inference; matching the deployment proves the file is
        # OURS but says nothing about whether its owner is still ALIVE, and the
        # heartbeat that was supposed to prove death also reads stale for a merely
        # paused process. So the rows stay where they are, intact and recoverable.
        import os as _os
        import sqlite3 as _sqlite3

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424254, n=4, deployment_id="dep-A")
        monkeypatch.setattr(buffer_mod, "fcntl", None)
        monkeypatch.setattr(buffer_mod, "_claim_dead_orphan", lambda path: None)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
            deployment_id="dep-A",  # SAME deployment, but still no proof of death
        )
        adopted = buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path)
        assert adopted == 0
        assert live.size() == 0
        assert orphan.exists(), "the orphan must be left intact on disk"
        check = _sqlite3.connect(str(orphan))
        try:
            assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 4
        finally:
            check.close()
        assert list(tmp_path.glob("*.adopting-*")) == []
        live.close()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_matching_deployment_orphan_is_adopted_rh6(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424252, n=4, deployment_id="dep-A")
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
            deployment_id="dep-A",  # SAME fingerprint -> adopt
        )
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 4
        assert not orphan.exists()
        live.close()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: inode identity check",
    )
    def test_path_still_matches_locked_inode_detects_swap_rh7(self, tmp_path: Path) -> None:
        import os as _os

        from z4j_bare import buffer as buffer_mod

        real = tmp_path / "probe.sqlite"
        real.write_bytes(b"real")
        fd = _os.open(str(real), _os.O_RDONLY)
        try:
            assert buffer_mod._path_still_matches_locked_inode(real, fd) is True
            # An attacker swaps a DIFFERENT inode in at the pathname.
            real.unlink()
            real.write_bytes(b"forged")
            assert buffer_mod._path_still_matches_locked_inode(real, fd) is False
        finally:
            _os.close(fd)

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_drain_refuses_when_inode_diverged_rh7(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424253, n=3)
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        # Simulate the post-open inode check failing (a forged-DB swap between
        # our lstat and sqlite's open): no rows drained, orphan left in place.
        monkeypatch.setattr(
            buffer_mod,
            "_path_still_matches_locked_inode",
            lambda p, fd, **_kwargs: False,
        )
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 0
        assert live.size() == 0
        assert orphan.exists()
        live.close()

    # ------------------------------------------------------------------
    # RH8: liveness lease
    # ------------------------------------------------------------------

    def test_touch_lease_stamps_meta_rh8(self, tmp_path: Path) -> None:
        import sqlite3 as _sqlite
        import time as _time

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-1.sqlite"
        store = BufferStore(path=p, max_entries=100, max_bytes=1_000_000)
        store.append("task.event", b"x")  # keep the file on close
        # Force the on-disk lease STALE first, so the assertion below isolates
        # touch_lease()'s effect rather than the __init__/append stamp (which
        # would satisfy a naive "is it fresh" check even if touch_lease were a
        # no-op). Also reset the in-memory throttle so touch_lease actually writes.
        stale = _time.time() - 5000.0
        store._conn.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES ('lease_heartbeat', ?)",
            (str(stale),),
        )
        store._last_lease_ts = 0.0
        store.touch_lease()
        store.close()
        conn = _sqlite.connect(str(p))
        try:
            lease = buffer_mod._read_lease_heartbeat(conn)
        finally:
            conn.close()
        assert lease is not None
        # touch_lease must have OVERWRITTEN the stale value with ~now.
        assert lease > stale + 1000.0
        assert abs(_time.time() - lease) < 60

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_fresh_lease_orphan_not_adopted_rh8(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # RH8: flock succeeded (a no-op on some NFS mounts) AND the pid check
        # reports dead (meaningless across PID namespaces), but the owner's
        # liveness lease is FRESH -- the buffer must NOT be drained/unlinked.
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424260, n=4, lease_age_seconds=5.0)
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 0
        assert live.size() == 0
        assert orphan.exists()  # left in place; owner is alive
        live.close()

    def test_unreadable_lease_fails_closed_p1_8(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # P1-8: if the lease SELECT raises (a live buffer under WAL contention),
        # the gate must fail CLOSED -- refuse to adopt -- not mistake the
        # unreadable buffer for a lease-less adoptable orphan. Drive the pure
        # gate directly (patch the inode check so no flock/fd is needed), so
        # this runs on every platform, not just POSIX.
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        conn = sqlite3.connect(":memory:")

        def _boom(_conn: sqlite3.Connection) -> float | None:
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(
            buffer_mod,
            "_path_still_matches_locked_inode",
            lambda p, fd, **_kwargs: True,
        )
        monkeypatch.setattr(buffer_mod, "_read_lease_heartbeat", _boom)
        current = BufferStore(path=Path(":memory:"), max_entries=10, max_bytes=1_000_000)
        try:
            assert (
                buffer_mod._orphan_is_adoptable(conn, Path("buffer-999.sqlite"), -1, current)
                is False
            )
        finally:
            conn.close()
            current.close()

    def test_read_lease_heartbeat_propagates_select_error_p1_8(self) -> None:
        # P1-8 (the other half): the REAL _read_lease_heartbeat must PROPAGATE a
        # failed SELECT (its internal try/except-return-None was removed) so the
        # gate can fail closed, instead of masquerading a read failure as "no
        # lease". Drives the real function (not a monkeypatch), so reverting the
        # removed try/except is caught.
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        conn = sqlite3.connect(":memory:")
        try:
            # No _meta table at all -> the SELECT raises; it must propagate.
            with pytest.raises(sqlite3.Error):
                buffer_mod._read_lease_heartbeat(conn)
            # With a _meta table but NO lease row -> None (a legacy buffer).
            conn.execute("CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT)")
            assert buffer_mod._read_lease_heartbeat(conn) is None
            # With a valid lease value -> that float.
            conn.execute(
                "INSERT INTO _meta(key, value) VALUES (?, ?)",
                (buffer_mod._LEASE_HEARTBEAT_KEY, "1234.5"),
            )
            assert buffer_mod._read_lease_heartbeat(conn) == 1234.5
            # M5: a PRESENT-but-corrupt value must RAISE LeaseUnreadableError (fail
            # closed), NOT return None (which would look like legacy-absence and
            # skip the liveness gate).
            for corrupt in ("not-a-float", "NaN", "inf"):
                conn.execute(
                    "UPDATE _meta SET value = ? WHERE key = ?",
                    (corrupt, buffer_mod._LEASE_HEARTBEAT_KEY),
                )
                with pytest.raises(buffer_mod.LeaseUnreadableError):
                    buffer_mod._read_lease_heartbeat(conn)
        finally:
            conn.close()

    def test_copy_and_clear_removes_orphan_rows_p1_1212(self, tmp_path: Path) -> None:
        # buffer:1212: rows copied into `current` are deleted from the orphan, so
        # if a later unlink fails and the file survives, a rescan cannot re-copy
        # (duplicate-deliver) them. Cross-platform: exercises the pure copy+clear
        # helper without the POSIX flock/inode machinery.
        from z4j_bare import buffer as buffer_mod

        orphan = BufferStore(path=tmp_path / "orphan.sqlite", max_entries=100, max_bytes=1_000_000)
        orphan.append("task.event", b"a")
        orphan.append("task.event", b"b")
        current = BufferStore(
            path=tmp_path / "current.sqlite", max_entries=100, max_bytes=1_000_000
        )
        try:
            assert buffer_mod._copy_and_clear_orphan_rows(orphan._conn, current) == 2
            assert current.size() == 2
            # The orphan is now empty -> a second pass (a rescan on a surviving
            # file) copies nothing, so no rows are delivered twice.
            assert buffer_mod._copy_and_clear_orphan_rows(orphan._conn, current) == 0
            assert current.size() == 2
        finally:
            orphan.close()
            current.close()

    def test_copy_and_clear_durable_on_transactional_conn_p1_1212(self, tmp_path: Path) -> None:
        # buffer:1212 (load-bearing autocommit): the per-row DELETE must survive
        # the caller's conn.close() even when the drain connection is NOT
        # autocommit. The helper commits per row, so it does not secretly depend
        # on _drain_orphan_into opening the orphan isolation_level=None.
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        opath = tmp_path / "orphan.sqlite"
        seed = BufferStore(path=opath, max_entries=100, max_bytes=1_000_000)
        seed.append("task.event", b"a")
        seed.append("task.event", b"b")
        seed.close()
        current = BufferStore(
            path=tmp_path / "current.sqlite", max_entries=100, max_bytes=1_000_000
        )
        # DEFAULT-isolation (transactional) connection, NOT BufferStore autocommit.
        drain = sqlite3.connect(str(opath))
        try:
            assert buffer_mod._copy_and_clear_orphan_rows(drain, current) == 2
            drain.close()  # without the per-row commit this rolls the DELETEs back
            assert current.size() == 2
            check = sqlite3.connect(str(opath))
            try:
                (n,) = check.execute("SELECT COUNT(*) FROM entries").fetchone()
            finally:
                check.close()
            assert n == 0  # rows durably removed -> no re-copy on a rescan
        finally:
            current.close()

    def test_copy_and_clear_reports_partial_progress_p1_1288(self, tmp_path: Path) -> None:
        # buffer:1288: on a mid-loop failure the helper's local count is lost, so
        # it writes progress[0] per durably-moved row; _drain_orphan_into reads it
        # to report the rows actually adopted (not 0).
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        opath = tmp_path / "orphan.sqlite"
        seed = BufferStore(path=opath, max_entries=100, max_bytes=1_000_000)
        for payload in (b"a", b"b", b"c"):
            seed.append("task.event", payload)
        seed.close()

        class _FailingCurrent:
            def __init__(self) -> None:
                self.n = 0

            def append(self, kind: str, payload: bytes) -> None:
                self.n += 1
                if self.n == 2:  # fail on the SECOND row
                    raise sqlite3.OperationalError("disk full")

        drain = sqlite3.connect(str(opath), isolation_level=None)
        moved = [0]
        try:
            with pytest.raises(sqlite3.OperationalError):
                buffer_mod._copy_and_clear_orphan_rows(drain, _FailingCurrent(), progress=moved)
        finally:
            drain.close()
        assert moved[0] == 1  # exactly one row durably moved before the failure

    @pytest.mark.skipif(
        buffer_mod.fcntl is None,
        reason="requires a held ownership flock",
    )
    def test_drain_reports_partial_count_on_mid_drain_error_p1_1288(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # buffer:1288 CALLER half: _drain_orphan_into must recover the rows the
        # helper durably moved (via progress) and return that, not 0, on a
        # mid-drain error. The body claims a real ownership flock via
        # _claim_dead_orphan, which returns None without fcntl by design (flock
        # adoption is POSIX-only), so the first assertion cannot hold on
        # Windows. Same guard the sibling adoption tests already carry; the
        # comment below predates it and overstated the portability.
        import os
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        opath = tmp_path / "orphan.sqlite"
        seed = BufferStore(path=opath, max_entries=100, max_bytes=1_000_000)
        seed.append("task.event", b"a")
        seed.append("task.event", b"b")
        seed.close()
        current = BufferStore(
            path=tmp_path / "current.sqlite", max_entries=100, max_bytes=1_000_000
        )
        lock_fd = buffer_mod._claim_dead_orphan(opath)
        assert lock_fd is not None

        monkeypatch.setattr(buffer_mod, "_orphan_is_adoptable", lambda *a, **k: True)

        def _partial_then_raise(conn, cur, *, progress=None):
            if progress is not None:
                progress[0] = 1  # one row durably moved before...
            raise sqlite3.OperationalError("mid-drain failure")

        monkeypatch.setattr(buffer_mod, "_copy_and_clear_orphan_rows", _partial_then_raise)
        try:
            adopted = buffer_mod._drain_orphan_into(current, opath, lock_fd)
        finally:
            os.close(lock_fd)
            current.close()
        assert adopted == 1  # the rows actually moved, NOT 0
        assert opath.exists()  # not unlinked -- the rest is left for a rescan

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_stale_lease_orphan_still_adopted_rh8(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The complement: a STALE lease (dead owner) does not block adoption.
        import os as _os

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424261, n=3, lease_age_seconds=1000.0)
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 3
        assert not orphan.exists()
        live.close()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only: fd/flock-gated orphan adoption",
    )
    def test_refused_fresh_lease_orphan_adopted_after_lease_ages_rh8(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # RH8 follow-up: a fresh-lease orphan is REFUSED on the first scan (the
        # owner might still be alive), then ADOPTED on a later re-scan once its
        # lease ages past the stale window -- so the periodic re-scan recovers
        # what a one-shot startup scan would have stranded.
        import os as _os
        import sqlite3 as _sqlite
        import time as _time

        from z4j_bare import buffer as buffer_mod

        orphan = self._write_orphan(tmp_path, pid=424262, n=3, lease_age_seconds=5.0)
        monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)
        live = BufferStore(
            path=tmp_path / f"buffer-{_os.getpid()}.sqlite",
            max_entries=1000,
            max_bytes=10_000_000,
        )
        # First scan: fresh lease -> refused, orphan left in place.
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 0
        assert orphan.exists()
        # The dead owner's lease ages past the stale window.
        conn = _sqlite.connect(str(orphan))
        try:
            conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES ('lease_heartbeat', ?)",
                (str(_time.time() - 1000.0),),
            )
            conn.commit()
        finally:
            conn.close()
        # Re-age the file: the lease write above reset mtime to now, which would
        # trip the _ADOPT_MIN_AGE_S guard. A real dead peer is not written again.
        _old = _time.time() - 120
        _os.utime(orphan, (_old, _old))
        # Re-scan (what _periodic_orphan_adoption does): now adopted.
        assert buffer_mod.adopt_orphaned_buffers(live, home_dir=tmp_path) == 3
        assert not orphan.exists()
        live.close()


class TestBoundedCloseRM6:
    """RM6: close(lock_timeout=...) must NOT block indefinitely when the store
    lock is held (a wedged daemon orphan-scan append); it abandons the handle."""

    def test_close_abandons_on_held_lock(self, tmp_path: Path) -> None:
        import threading
        import time

        p = tmp_path / "buffer-901.sqlite"
        buf = BufferStore(path=p, max_entries=100, max_bytes=1_000_000)
        buf.append("task.event", b"x")

        held = threading.Event()
        release = threading.Event()

        def _holder() -> None:
            with buf._lock:
                held.set()
                release.wait(3.0)

        t = threading.Thread(target=_holder)
        t.start()
        try:
            assert held.wait(1.0)
            start = time.monotonic()
            buf.close(lock_timeout=0.05)  # must not wait for the holder
            elapsed = time.monotonic() - start
            assert elapsed < 1.0, "bounded close waited for the held lock"
            assert buf.closed is True  # abandoned but marked closed
        finally:
            release.set()
            t.join(3.0)
            # RM6 abandons the sqlite handle to the OS; close it now that the
            # holder released, so the test does not leak a connection.
            import contextlib

            with contextlib.suppress(Exception):
                buf._conn.close()

    def test_close_unbounded_default_still_cleans_up(self, tmp_path: Path) -> None:
        # The default (lock_timeout=None) path is unchanged: a clean close of an
        # empty buffer unlinks the file.
        p = tmp_path / "buffer-902.sqlite"
        buf = BufferStore(
            path=p,
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="dep-A",
        )
        buf.close()
        assert buf.closed is True
        assert not p.exists()  # empty -> removed

    def test_close_abandon_keeps_flock_r8_h10(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # On the abandon path (store lock held by a wedged daemon append)
        # close() must NOT release the ownership flock. The wedged append may be
        # past its closed-check and blocked in SQLite; dropping the flock would
        # let another deployment claim/restamp this exact inode and the resuming
        # append would write into the NEW owner's buffer (cross-owner corruption).
        # Keeping the flock held (OS reclaims at process exit) is strictly safer
        # than the bounded fd "leak" -- this reverts the release.
        import threading

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "buffer-903.sqlite"
        buf = BufferStore(path=p, max_entries=100, max_bytes=1_000_000)

        released: list[int | None] = []
        monkeypatch.setattr(buffer_mod, "_release_lock", released.append)
        # Simulate a held ownership flock (on Windows _lock_fd is None natively).
        buf._lock_fd = 4242

        held = threading.Event()
        release = threading.Event()

        def _holder() -> None:
            with buf._lock:
                held.set()
                release.wait(3.0)

        t = threading.Thread(target=_holder)
        t.start()
        try:
            assert held.wait(1.0)
            buf.close(lock_timeout=0.05)
            assert buf.closed is True
            assert released == []  # H10: flock NOT released on abandon
            assert buf._lock_fd == 4242  # still held until process exit
        finally:
            release.set()
            t.join(3.0)
            import contextlib

            with contextlib.suppress(Exception):
                buf._conn.close()


class TestNoFlockOwnershipR8:
    """M10,M11: no-flock inode ownership via an ATOMICALLY
    claimed per-open owner token, globally-unique relocation, and no-fcntl orphan
    adoption.: every test FORCES the no-flock branch (fcntl=None) so it
    genuinely exercises that code path on Linux too, not just on native Windows."""

    def test_two_live_owners_relocate_r8_h9(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # On a no-flock platform a SECOND LIVE store on one explicit
        # shared buffer_path must relocate (the first atomically claimed the owner
        # token) rather than operate on the shared inode and drain the peer's rows.
        import os

        from z4j_bare import buffer as buffer_mod

        monkeypatch.setattr(buffer_mod, "fcntl", None)  # Force no-flock
        p = tmp_path / "shared.sqlite"
        a = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        a.append("task.event", b"x")
        # A is STILL LIVE (not closed) -> its owner-token is fresh in _meta.
        b = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        try:
            assert b.path != p  # relocated off the shared inode
            assert b.path.name.startswith(f"buffer-{os.getpid()}-")  # Unique
            assert b.path.suffix == ".sqlite"
            assert b.size() == 0  # its own fresh file
            assert a.size() == 1  # A's row untouched by B
        finally:
            b.close()
            a.close()

    def test_no_flock_sequential_reopen_preserves_old_source(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sqlite3

        from z4j_bare import buffer as buffer_mod

        # Without a possession capability the new process still gets a fresh
        # private sink, but automatic recovery cannot prove the old owner dead.
        monkeypatch.setattr(buffer_mod, "fcntl", None)  # Force no-flock
        p = tmp_path / "seq.sqlite"
        a = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        a.append("task.event", b"x")
        a.close()  # clears the owner token
        b = BufferStore(path=p, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        try:
            assert b.path != p
            assert b.size() == 0
            assert buffer_mod.adopt_orphaned_buffers(b, home_dir=tmp_path) == 0
            check = sqlite3.connect(str(p))
            try:
                assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
            finally:
                check.close()
        finally:
            b.close()

    def test_no_flock_orphan_is_left_intact_r12(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # On a platform with no working file locking an orphan is NOT
        # adopted, and above all is NOT destroyed. A no-flock fallback used to
        # adopt here on a heartbeat-plus-pid inference, and every part of that
        # inference proved reachable in the wrong direction: a stalled or
        # clock-skewed LIVE owner reads as dead, and the file being vetted could
        # not be tied to the file being claimed without an identity taken from the
        # open handle and a rename that refuses to overwrite. It deleted running
        # processes' events. Without a lock there is no authoritative liveness
        # proof, so we leave the file alone and log it; an operator can recover
        # it. This is the previous release's behaviour.
        import os

        from z4j_bare import buffer as buffer_mod
        from z4j_bare.buffer import adopt_orphaned_buffers

        monkeypatch.setattr(buffer_mod, "fcntl", None)  # Force no-flock
        # A dead peer's orphan buffer, same deployment, with a stale lease.
        dead_pid = 999_999  # not a live pid
        orphan = tmp_path / f"buffer-{dead_pid}.sqlite"
        peer = BufferStore(path=orphan, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        peer.append("task.event", b"undelivered")
        # Age its lease past the stale window so it reads as a dead owner, then
        # abandon the handle WITHOUT a clean close (simulating a crash).
        peer._write_lease(0.0)  # epoch -> definitely stale
        peer._conn.close()
        # Age the file past _ADOPT_MIN_AGE_S so the young-file guard does not skip
        # it (the crash we simulate happened a while ago).
        import time as _time

        old = _time.time() - 120
        os.utime(orphan, (old, old))

        # Our current buffer (different pid path), same deployment.
        current = BufferStore(
            path=tmp_path / f"buffer-{os.getpid()}.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="dep-A",
        )
        try:
            adopted = adopt_orphaned_buffers(current, home_dir=tmp_path)
            assert adopted == 0, "no lock means no proof of death; do not adopt"
            assert current.size() == 0
            # The property that makes the bound acceptable: the peer's row is
            # still recoverable. Never renamed, never drained, never unlinked.
            assert orphan.exists(), "the orphan must be left intact on disk"
            import sqlite3 as _sqlite3

            check = _sqlite3.connect(str(orphan))
            try:
                assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
            finally:
                check.close()
            # No private .adopting-* leftover was created either.
            assert list(tmp_path.glob("*.adopting-*")) == []
        finally:
            current.close()

    def test_unsupported_flock_errno_not_contended_r8_m6(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An unsupported-lock filesystem (EOPNOTSUPP) must be classified as
        # no-flock, NOT as contention (which would needlessly relocate + fail
        # closed on a mount that merely cannot lock). A genuinely-held lock
        # (EWOULDBLOCK) IS contention.
        import errno as _errno

        from z4j_bare import buffer as buffer_mod

        p = tmp_path / "nolock.sqlite"
        p.write_bytes(b"")  # must exist for os.open

        class _Fcntl:
            LOCK_EX = 2
            LOCK_NB = 4
            LOCK_UN = 8
            _err = _errno.EOPNOTSUPP

            def flock(self, fd: int, op: int) -> None:
                raise OSError(self._err, "boom")

        fake = _Fcntl()
        monkeypatch.setattr(buffer_mod, "fcntl", fake)
        fd, contended = buffer_mod._acquire_own_lock(p)
        assert fd is None
        assert contended is False  # unsupported -> no-flock, not contended

        fake._err = _errno.EWOULDBLOCK
        fd2, contended2 = buffer_mod._acquire_own_lock(p)
        assert fd2 is None
        assert contended2 is True  # genuinely held -> contended


class TestNoFlockSimultaneousOpenR11:
    """Two openers arriving at the SAME no-flock inode must never share it.

    The claim used to COMMIT the owner token and stamp the liveness lease
    separately. A second opener arriving in that window read the token but found
    no fresh lease, concluded the inode was not live-owned, overwrote the token
    and carried on using the SAME file: two live writers on one SQLite database
    with no lock. Reproduced deterministically by pausing the first opener inside
    the window. The claim now writes token+lease under one transaction.
    """

    def test_second_opener_relocates_instead_of_sharing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import contextlib
        import threading

        from z4j_bare import buffer as buffer_mod

        monkeypatch.setattr(buffer_mod, "fcntl", None)  # force no-flock
        shared = tmp_path / "shared.sqlite"

        in_window = threading.Event()
        release = threading.Event()
        orig_write_lease = buffer_mod.BufferStore._write_lease

        def slow_write_lease(self, now):
            # Park the FIRST opener immediately after it claimed the inode.
            if not in_window.is_set():
                in_window.set()
                release.wait(10)
            return orig_write_lease(self, now)

        monkeypatch.setattr(buffer_mod.BufferStore, "_write_lease", slow_write_lease)

        made: dict[str, BufferStore] = {}

        def open_first() -> None:
            made["a"] = BufferStore(path=shared, max_entries=100, max_bytes=1_000_000)

        t = threading.Thread(target=open_first, daemon=True)
        t.start()
        try:
            assert in_window.wait(10), "first opener never reached the claim window"
            # Second opener arrives while the first is inside the window.
            b = BufferStore(path=shared, max_entries=100, max_bytes=1_000_000)
        finally:
            release.set()
            t.join(10)

        a = made["a"]
        try:
            ia = a.path.stat()
            ib = b.path.stat()
            assert (ia.st_dev, ia.st_ino) != (ib.st_dev, ib.st_ino), (
                "two live no-flock openers ended up on the SAME inode"
            )
        finally:
            for s in (a, b):
                with contextlib.suppress(Exception):
                    s.close()


class TestSharedPathIsNotSweptR12:
    """An explicit shared buffer_path is deliberately NOT auto-adopted.

    Swept the originally-requested path so that rows a peer crashed on there
    were not stranded. That candidate carries no pid in its name, so it lost the
    ``_pid_is_alive`` backstop every other candidate keeps, leaving a stale lease
    as its only liveness proof. A >90s clock skew (shared network path) or a >90s
    stall of the owner (SIGSTOP, a hung write, a blocked event loop) makes a LIVE
    owner's lease read stale, and the sweep then renamed, drained and UNLINKED
    the file underneath it; the partial-drain requeue additionally renamed back
    onto a pathname a restarted peer may already have re-created, replacing its
    inode. Both destroy a live process's data.

    Stranding a dead peer's rows is recoverable by an operator -- the file is
    still on disk and the relocation is logged. Destroying a live peer's rows is
    not. So the sweep is gone, the limitation is documented, and this test pins
    BOTH halves: not adopted, and not destroyed.
    """

    def test_shared_path_is_not_adopted_but_is_left_intact(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os
        import sqlite3 as _sqlite
        import time as _time

        from z4j_bare import buffer as buffer_mod
        from z4j_bare.buffer import adopt_orphaned_buffers

        monkeypatch.setattr(buffer_mod, "fcntl", None)  # force no-flock
        shared = tmp_path / "shared.sqlite"

        # A peer holds the shared path, buffers a row, then CRASHES with its
        # lease still fresh (abandon the handle without a clean close).
        peer = BufferStore(path=shared, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A")
        peer.append("task.event", b"undelivered")
        peer._conn.close()

        # The next opener sees a FRESH foreign lease and relocates off the path.
        current = BufferStore(
            path=shared, max_entries=100, max_bytes=1_000_000, deployment_id="dep-A"
        )
        assert current.path != shared, "expected relocation off the live shared path"

        # Time passes: the crashed peer's lease goes stale and the file ages.
        conn = _sqlite.connect(str(shared))
        try:
            conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES ('lease_heartbeat', ?)",
                (str(_time.time() - 10_000),),
            )
            conn.commit()
        finally:
            conn.close()
        old = _time.time() - 120
        os.utime(shared, (old, old))

        try:
            adopted = adopt_orphaned_buffers(current, home_dir=tmp_path)
            # The bound: not swept, because the pid-less candidate cannot prove
            # its owner is dead without risking a live one.
            assert adopted == 0, "an explicit shared path must not be auto-adopted"
            assert current.size() == 0
            # The safety property that makes the bound acceptable: the rows are
            # STILL THERE for an operator to recover -- never renamed, drained
            # or unlinked out from under whoever may still hold the file.
            assert shared.exists(), "the shared path must be left intact on disk"
            check = _sqlite.connect(str(shared))
            try:
                assert check.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
            finally:
                check.close()
        finally:
            current.close()
