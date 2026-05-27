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


class TestEviction:
    def test_entry_count_limit(self, buffer_path: Path) -> None:
        buf = BufferStore(path=buffer_path, max_entries=5, max_bytes=100_000_000)
        try:
            for i in range(10):
                buf.append("event_batch", str(i).encode())
            assert buf.size() <= 5
            # The LAST 5 should have survived.
            remaining = buf.drain(10)
            assert [e.payload for e in remaining] == [
                str(i).encode() for i in range(5, 10)
            ]
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
        self, buffer_path: Path,
    ) -> None:
        import os
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
        db_mode = stat.S_IMODE(os.stat(buffer_path).st_mode)
        assert db_mode == 0o600, (
            f"1.6.5 P1 regression: buffer DB {buffer_path} has "
            f"mode 0{db_mode:o}, expected 0600. "
            "On a multi-tenant POSIX host this leaks task payload "
            "BLOBs to other local users."
        )

        # WAL sidecar (created on first write since WAL is enabled).
        wal_path = buffer_path.with_suffix(buffer_path.suffix + "-wal")
        if wal_path.exists():
            wal_mode = stat.S_IMODE(os.stat(wal_path).st_mode)
            assert wal_mode == 0o600, (
                f"1.6.5 P1 regression: WAL sidecar {wal_path} has "
                f"mode 0{wal_mode:o}, expected 0600"
            )

        # SHM sidecar (memory-mapped index for the WAL).
        shm_path = buffer_path.with_suffix(buffer_path.suffix + "-shm")
        if shm_path.exists():
            shm_mode = stat.S_IMODE(os.stat(shm_path).st_mode)
            assert shm_mode == 0o600, (
                f"1.6.5 P1 regression: SHM sidecar {shm_path} has "
                f"mode 0{shm_mode:o}, expected 0600"
            )

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "getuid"),
        reason="POSIX-only test",
    )
    def test_loose_z4j_home_emits_warning(
        self, buffer_path: Path, caplog,
    ) -> None:
        """When the buffer's parent directory is group/world
        accessible, BufferStore startup MUST log a WARN naming the
        path + the remediation command. Operators on multi-tenant
        hosts need this signal."""
        import logging
        import os

        # Reset the one-shot guard so this test always exercises the
        # check, even when run after another test that triggered it.
        from z4j_bare import buffer as buffer_module

        buffer_module._z4j_home_perms_warned = False

        # Make the parent dir group-readable (0o755 inclusive of
        # group/world read bits) -- this is the bit pattern the
        # warning is supposed to flag.
        os.chmod(buffer_path.parent, 0o755)

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
        self, buf: BufferStore, caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Simulate the drift observed in production.
        buf._cached_count = -12  # noqa: SLF001 - deliberate drift
        with caplog.at_level("WARNING", logger="z4j.runtime.buffer"):
            assert buf.size() == 0
        assert any(
            "drifted negative" in rec.message for rec in caplog.records
        ), "expected WARNING about drifted counters"

    def test_byte_size_clamps_to_zero_when_cache_goes_negative(
        self, buf: BufferStore,
    ) -> None:
        buf._cached_bytes = -4096  # noqa: SLF001
        assert buf.byte_size() == 0

    def test_reconcile_uses_disk_truth(
        self, buf: BufferStore,
    ) -> None:
        # Seed real entries, then corrupt the cache; the
        # reconcile path must return the disk-truth value, not
        # the clamped zero.
        for i in range(7):
            buf.append("event_batch", f"row-{i}".encode())
        buf._cached_count = -99  # noqa: SLF001
        buf._cached_bytes = -99  # noqa: SLF001
        assert buf.size() == 7
        # Reconcile also fixes byte_size - next call returns the
        # real disk-sourced bytes total, no longer clamped.
        real_bytes = sum(len(f"row-{i}".encode()) for i in range(7))
        assert buf.byte_size() == real_bytes

    def test_drift_warning_fires_only_once_per_instance(
        self, buf: BufferStore, caplog: pytest.LogCaptureFixture,
    ) -> None:
        # A persistent bug would otherwise spam the log every
        # heartbeat (10s cadence). One warning per BufferStore
        # lifetime is enough signal for operators.
        with caplog.at_level("WARNING", logger="z4j.runtime.buffer"):
            buf._cached_count = -1  # noqa: SLF001
            buf.size()
            buf._cached_count = -5  # noqa: SLF001
            buf.size()
            buf._cached_count = -42  # noqa: SLF001
            buf.size()
        drift_logs = [
            rec for rec in caplog.records if "drifted negative" in rec.message
        ]
        assert len(drift_logs) == 1
