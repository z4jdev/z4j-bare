"""Boundary C acceptance reproductions.

These tests exercise the production ``BufferStore`` and real SQLite files.
They pin the four fail-open orderings carried in the 1.8.0 handoff.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from z4j_bare import buffer as buffer_mod
from z4j_bare.buffer import BufferStore


def _age_owner_metadata(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
            (buffer_mod._LEASE_HEARTBEAT_KEY, "0"),
        )
        conn.commit()
    finally:
        conn.close()
    old = time.time() - 120
    os.utime(path, (old, old))


def _row_count(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return int(conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0])
    finally:
        conn.close()


def _make_real_17_buffer(path: Path, *, payload: bytes) -> None:
    """Create the exact production 1.7 buffer shape (which has no _meta)."""
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
            """
        )
        conn.execute(
            "INSERT INTO entries(kind, payload, created_at, attempts) "
            "VALUES ('task.event', ?, 0, 0)",
            (payload,),
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM sqlite_schema WHERE name = '_meta'").fetchone()[0]
            == 0
        )
    finally:
        conn.close()
    old = time.time() - 120
    os.utime(path, (old, old))


def _append_from_independent_process(path: Path, payload: bytes) -> int:
    """Return zero only when a separate SQLite process can commit a write."""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sqlite3
import sys

try:
    connection = sqlite3.connect(sys.argv[1], timeout=0)
    connection.execute(
        "INSERT INTO entries(kind, payload, created_at, attempts) "
        "VALUES ('task.event', ?, 0, 0)",
        (sys.argv[2].encode(),),
    )
    connection.commit()
    connection.close()
except sqlite3.OperationalError:
    raise SystemExit(73)
""",
            str(path),
            payload.decode(),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    return completed.returncode


def _make_crash_wal_source(
    path: Path,
    *,
    deployment_id: str,
    payload: bytes,
) -> None:
    """Copy a live WAL set, modeling the bytes left by process death."""
    live = path.parent / f".{path.name}.live"
    conn = sqlite3.connect(str(live), isolation_level=None)
    try:
        assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.executescript(buffer_mod._SCHEMA)
        for key, value in (
            (buffer_mod._BUFFER_UUID_KEY, "1" * 32),
            (buffer_mod._LIFECYCLE_KEY, buffer_mod._LIFECYCLE_SEALED_READY),
            ("deployment", deployment_id),
            (buffer_mod._LEASE_HEARTBEAT_KEY, "0"),
        ):
            conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                (key, value),
            )
        conn.execute(
            "INSERT INTO entries(kind, payload, created_at, attempts) "
            "VALUES ('task.event', ?, 0, 0)",
            (payload,),
        )
        assert Path(str(live) + "-wal").exists()
        for suffix in ("", "-wal", "-shm"):
            source_component = Path(str(live) + suffix)
            if source_component.exists():
                shutil.copyfile(source_component, Path(str(path) + suffix))
    finally:
        conn.close()
        for suffix in ("", "-wal", "-shm"):
            Path(str(live) + suffix).unlink(missing_ok=True)
    old = time.time() - 120
    os.utime(path, (old, old))


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX record-lock semantics")
def test_inode_probe_preserves_pre_18_sqlite_exclusion(tmp_path: Path) -> None:
    """An inode identity check must not silently discard the held POSIX lock."""
    source = tmp_path / "buffer-999991.sqlite"
    _make_real_17_buffer(source, payload=b"original")

    lock_fd = buffer_mod._claim_dead_orphan(source)
    assert lock_fd is not None
    try:
        assert _append_from_independent_process(source, b"before-probe") == 73
        assert buffer_mod._path_still_matches_locked_inode(
            source,
            lock_fd,
            reassert_sqlite_exclusion=True,
        )
        assert _append_from_independent_process(source, b"after-probe") == 73
    finally:
        buffer_mod._release_lock(lock_fd)

    assert _append_from_independent_process(source, b"after-release") == 0
    assert _row_count(source) == 2


def _component_digests(path: Path) -> dict[str, tuple[int, str] | None]:
    result: dict[str, tuple[int, str] | None] = {}
    for suffix in ("", "-wal", "-shm", "-journal"):
        component = Path(str(path) + suffix)
        if component.exists():
            raw = component.read_bytes()
            result[suffix] = (len(raw), hashlib.sha256(raw).hexdigest())
        else:
            result[suffix] = None
    return result


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_explicit_shared_legacy_path_is_never_swept_from_live_owner(
    tmp_path: Path,
) -> None:
    """A path without the per-process name is not an automatic recovery source."""
    # It deliberately matches the broad historical ``buffer-*.sqlite`` glob
    # while carrying no numeric process identity.
    shared = tmp_path / "buffer-shared.sqlite"
    owner = sqlite3.connect(str(shared), isolation_level=None)
    owner.executescript(buffer_mod._SCHEMA)
    owner.execute(
        "INSERT OR REPLACE INTO _meta(key, value) VALUES ('deployment', ?)",
        ("deployment-A",),
    )
    owner.execute(
        "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
        (buffer_mod._BUFFER_UUID_KEY, "1" * 32),
    )
    owner.execute(
        "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
        (
            buffer_mod._LIFECYCLE_KEY,
            buffer_mod._LIFECYCLE_SEALED_READY,
        ),
    )
    owner.execute(
        "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, '0')",
        (buffer_mod._LEASE_HEARTBEAT_KEY,),
    )
    owner.execute(
        "INSERT INTO entries(kind, payload, created_at, attempts) VALUES ('task.event', ?, 0, 0)",
        (b"live-legacy-event",),
    )
    old = time.time() - 120
    os.utime(shared, (old, old))

    current = BufferStore(
        shared,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert current.path != shared
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert shared.exists()
        assert _row_count(shared) == 1
    finally:
        current.close()
        owner.close()


def test_rollback_runbook_inventories_explicit_sources_and_requires_manifest() -> None:
    """The old scanner may see files that automatic 1.8 discovery excludes."""
    repository = Path(__file__).resolve().parents[4]
    runbook = (repository / "docs/operations/buffer-recovery.md").read_text(
        encoding="utf-8",
    )
    rollback = runbook.split("## Preparing a rollback from 1.8", maxsplit=1)[1]

    assert "explicitly configured shared path" in rollback
    assert "active, discovered, and advisory" in rollback
    assert "zero undelivered rows" in rollback
    assert "FOREIGN or UNKNOWN" in rollback
    assert "outside the old scanner root" in rollback
    assert "rollback manifest" in rollback
    assert "device/inode" in rollback
    assert "row count" in rollback
    assert "`-wal`, `-shm`, and `-journal`" in rollback


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_foreign_crash_wal_classification_is_byte_preserving(tmp_path: Path) -> None:
    """Classifying FOREIGN must not checkpoint or remove its WAL/SHM."""
    source = tmp_path / "buffer-410021.sqlite"
    _make_crash_wal_source(
        source,
        deployment_id="deployment-B",
        payload=b"foreign-wal-event",
    )
    before = _component_digests(source)
    assert before["-wal"] is not None

    current = BufferStore(
        tmp_path / "buffer-410022.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert _component_digests(source) == before
        assert current.recovery_required()[0]["attribution"] == "foreign"
        assert list(tmp_path.glob("z4j-buffer-classify-*")) == []
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_classification_snapshot_is_bounded_before_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An oversized source is preserved before scratch disk or memory is spent."""
    payload = b"x" * (buffer_mod._RECOVERY_MAX_BYTES_PER_FILE + 1)
    source = BufferStore(
        tmp_path / "buffer-555551.sqlite",
        max_entries=2,
        max_bytes=len(payload) + 1024,
        deployment_id="deployment-A",
    )
    source_path = source.path
    assert source.append("task.event", payload)
    source.close()
    _age_owner_metadata(source_path)

    copied_component_bytes: list[int] = []
    real_copy = buffer_mod._copy_source_component

    def record_copy(
        source_component: Path,
        destination: Path,
        expected: tuple[int, int, int, int],
    ) -> None:
        copied_component_bytes.append(expected[2])
        real_copy(source_component, destination, expected)

    monkeypatch.setattr(buffer_mod, "_copy_source_component", record_copy)
    current = BufferStore(
        tmp_path / "buffer-555552.sqlite",
        max_entries=2,
        max_bytes=len(payload) + 1024,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert copied_component_bytes == []
        assert source_path.exists()
        assert _row_count(source_path) == 1
        records = current.recovery_required()
        assert len(records) == 1
        assert "classification snapshot exceeds" in str(records[0]["reason"])
    finally:
        current.close()


def test_orphan_payload_budget_is_checked_before_payload_read(tmp_path: Path) -> None:
    """The drain probes payload size before SQLite materializes the BLOB."""
    source = BufferStore(
        tmp_path / "orphan.sqlite",
        max_entries=2,
        max_bytes=1_000_000,
    )
    current = BufferStore(
        tmp_path / "current.sqlite",
        max_entries=2,
        max_bytes=1_000_000,
    )
    statements: list[str] = []
    source._conn.set_trace_callback(statements.append)
    try:
        assert source.append("task.event", b"x" * 32)
        statements.clear()
        with pytest.raises(
            buffer_mod.RecoveryBackpressureError,
            match="per-scan recovery budget exhausted",
        ):
            buffer_mod._copy_and_clear_orphan_rows(
                source._conn,
                current,
                max_bytes=16,
            )

        assert not any(
            "FROM entries" in statement
            and "payload" in statement
            and "length(payload)" not in statement
            for statement in statements
        )
        assert source.size() == 1
        assert current.size() == 0
    finally:
        source.close()
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_same_crash_wal_source_recovers_through_disposable_copy(tmp_path: Path) -> None:
    """The non-mutating classification step must not strand valid WAL rows."""
    source = tmp_path / "buffer-410023.sqlite"
    _make_crash_wal_source(
        source,
        deployment_id="deployment-A",
        payload=b"same-wal-event",
    )
    assert Path(str(source) + "-wal").exists()

    current = BufferStore(
        tmp_path / "buffer-410024.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert [entry.payload for entry in current.drain(limit=10)] == [
            b"same-wal-event",
        ]
        for suffix in ("", "-wal", "-shm", "-journal"):
            assert not Path(str(source) + suffix).exists()
        assert list(tmp_path.glob("z4j-buffer-classify-*")) == []
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_classification_scratch_is_removed_after_copy_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "buffer-410025.sqlite"
    old = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    old.append("task.event", b"preserve-after-copy-failure")
    old.close()
    _age_owner_metadata(source)
    before = _component_digests(source)

    current = BufferStore(
        tmp_path / "buffer-410026.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    real_copy = buffer_mod._copy_source_component

    def fail_after_copy(source_path, destination, expected) -> None:
        real_copy(source_path, destination, expected)
        raise OSError("classification copy failed")

    monkeypatch.setattr(buffer_mod, "_copy_source_component", fail_after_copy)
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert _component_digests(source) == before
        assert list(tmp_path.glob("z4j-buffer-classify-*")) == []
        records = current.recovery_required()
        assert len(records) == 1
        assert records[0]["attribution"] == "unknown"
        assert records[0]["lifecycle"] == "unknown"
        assert "classification snapshot failed" in records[0]["reason"]
    finally:
        current.close()


@pytest.mark.skipif(
    buffer_mod.fcntl is None or not Path("/proc/self/fd").is_dir(),
    reason="Linux descriptor oracle with POSIX recovery possession",
)
def test_classification_copy_failure_closes_source_and_scratch_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "buffer-410027.sqlite"
    old = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    old.append("task.event", b"preserve-after-write-failure")
    old.close()
    _age_owner_metadata(source)
    before = _component_digests(source)

    current = BufferStore(
        tmp_path / "buffer-410028.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )

    def fail_write(fd: int, data) -> int:
        del fd, data
        raise OSError("classification destination write failed")

    monkeypatch.setattr(buffer_mod.os, "write", fail_write)
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert _component_digests(source) == before
        open_targets: list[str] = []
        for fd_path in Path("/proc/self/fd").iterdir():
            with contextlib.suppress(OSError):
                open_targets.append(str(fd_path.readlink()))
        leaked_targets = [
            target
            for target in open_targets
            if target.startswith(str(source)) or "z4j-buffer-classify-" in target
        ]
        assert leaked_targets == []
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_pre_18_per_process_buffer_recovers_on_upgrade(tmp_path: Path) -> None:
    source = tmp_path / "buffer-410018.sqlite"
    _make_real_17_buffer(source, payload=b"pre-1.8-event")

    current = BufferStore(
        tmp_path / "buffer-410019.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert not source.exists()
        entries = current.drain(limit=10)
        assert [entry.payload for entry in entries] == [b"pre-1.8-event"]
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_pre_18_schema_lookalike_with_extra_column_is_preserved(
    tmp_path: Path,
) -> None:
    """Only the exact historical schema receives the legacy-local exception."""
    source = tmp_path / "buffer-410031.sqlite"
    _make_real_17_buffer(source, payload=b"lookalike-event")
    conn = sqlite3.connect(str(source), isolation_level=None)
    try:
        conn.execute("ALTER TABLE entries ADD COLUMN unrecognized TEXT")
    finally:
        conn.close()
    old = time.time() - 120
    os.utime(source, (old, old))

    current = BufferStore(
        tmp_path / "buffer-410032.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert source.exists()
        assert _row_count(source) == 1
        records = current.recovery_required()
        assert len(records) == 1
        assert records[0]["attribution"] == "unknown"
    finally:
        current.close()


@pytest.mark.parametrize(
    "missing_keys",
    [
        (buffer_mod._BUFFER_UUID_KEY,),
        (buffer_mod._LIFECYCLE_KEY, buffer_mod._BUFFER_UUID_KEY),
    ],
)
@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_current_buffer_missing_required_identity_is_preserved(
    tmp_path: Path,
    missing_keys: tuple[str, ...],
) -> None:
    """A damaged current source cannot inherit the legacy-local exception."""
    source = tmp_path / "buffer-410033.sqlite"
    damaged = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    damaged.append("task.event", b"damaged-identity-event")
    damaged.close()
    conn = sqlite3.connect(str(source), isolation_level=None)
    try:
        conn.executemany(
            "DELETE FROM _meta WHERE key = ?",
            [(key,) for key in missing_keys],
        )
        conn.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, '0')",
            (buffer_mod._LEASE_HEARTBEAT_KEY,),
        )
    finally:
        conn.close()
    old = time.time() - 120
    os.utime(source, (old, old))

    current = BufferStore(
        tmp_path / "buffer-410034.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert source.exists()
        assert _row_count(source) == 1
        records = current.recovery_required()
        assert len(records) == 1
        assert records[0]["attribution"] == "same"
        assert records[0]["lifecycle"] in {
            "unknown",
            buffer_mod._LIFECYCLE_SEALED_READY,
        }
    finally:
        current.close()


@pytest.mark.skipif(
    buffer_mod.fcntl is None or not hasattr(os, "fork"),
    reason="POSIX legacy-writer liveness",
)
def test_live_pre_18_numeric_writer_is_never_recovered(
    tmp_path: Path,
) -> None:
    """A live 1.7 writer has no 1.8 lifetime flock to contend with."""
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    child = os.fork()
    if child == 0:  # pragma: no cover - parent asserts the child result
        os.close(ready_read)
        os.close(release_write)
        source = tmp_path / f"buffer-{os.getpid()}.sqlite"
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(source), isolation_level=None)
            assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            conn.execute("PRAGMA wal_autocheckpoint=0")
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
                """
            )
            conn.execute(
                "INSERT INTO entries(kind, payload, created_at, attempts) "
                "VALUES ('task.event', ?, 0, 0)",
                (b"live-pre-1.8-wal-event",),
            )
            os.write(ready_write, b"1")
            os.read(release_read, 1)
        except BaseException:
            os._exit(2)
        finally:
            if conn is not None:
                conn.close()
            os.close(ready_write)
            os.close(release_read)
        os._exit(0)

    os.close(ready_write)
    os.close(release_read)
    source = tmp_path / f"buffer-{child}.sqlite"
    current: BufferStore | None = None
    try:
        assert os.read(ready_read, 1) == b"1"
        assert Path(str(source) + "-wal").exists()
        old = time.time() - 120
        os.utime(source, (old, old))
        current = BufferStore(
            tmp_path / "buffer-410029.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )

        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert source.exists()
        assert _row_count(source) == 1
    finally:
        if current is not None:
            current.close()
        with contextlib.suppress(OSError):
            os.write(release_write, b"1")
        os.close(ready_read)
        os.close(release_write)
        _, status = os.waitpid(child, 0)
        assert os.waitstatus_to_exitcode(status) == 0


@pytest.mark.skipif(
    buffer_mod.fcntl is None or not hasattr(os, "fork"),
    reason="POSIX legacy-writer possession",
)
def test_live_pre_18_numeric_looking_explicit_writer_is_never_recovered(
    tmp_path: Path,
) -> None:
    """The inode owner, not an operator-chosen numeric filename, is authority."""

    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    source = tmp_path / "buffer-987654321.sqlite"
    child = os.fork()
    if child == 0:  # pragma: no cover - parent asserts the child result
        os.close(ready_read)
        os.close(release_write)
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(source), isolation_level=None)
            assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            conn.execute("PRAGMA wal_autocheckpoint=0")
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
                """
            )
            conn.execute(
                "INSERT INTO entries(kind, payload, created_at, attempts) "
                "VALUES ('task.event', ?, 0, 0)",
                (b"live-explicit-pre-1.8-event",),
            )
            os.write(ready_write, b"1")
            os.read(release_read, 1)
        except BaseException:
            os._exit(2)
        finally:
            if conn is not None:
                conn.close()
            os.close(ready_write)
            os.close(release_read)
        os._exit(0)

    os.close(ready_write)
    os.close(release_read)
    current: BufferStore | None = None
    child_reaped = False
    try:
        assert os.read(ready_read, 1) == b"1"
        assert child != 987654321
        old = time.time() - 120
        os.utime(source, (old, old))
        current = BufferStore(
            tmp_path / "buffer-410030.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )

        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.drain(limit=10) == []
        assert source.exists()

        os.write(release_write, b"1")
        _, status = os.waitpid(child, 0)
        child_reaped = True
        assert os.waitstatus_to_exitcode(status) == 0

        # Closing the last WAL connection can checkpoint and refresh the main
        # file's mtime. Age it again so this assertion tests possession release,
        # not the independent fresh-file grace period.
        os.utime(source, (old, old))
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert not source.exists()
        assert [entry.payload for entry in current.drain(limit=10)] == [
            b"live-explicit-pre-1.8-event",
        ]
    finally:
        if current is not None:
            current.close()
        if not child_reaped:
            with contextlib.suppress(OSError):
                os.write(release_write, b"1")
            with contextlib.suppress(OSError):
                os.waitpid(child, 0)
        os.close(ready_read)
        os.close(release_write)


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_current_buffer_missing_meta_is_not_misclassified_as_pre_18(
    tmp_path: Path,
) -> None:
    """Other 1.8 tables prove metadata loss, so the source stays UNKNOWN."""
    source = tmp_path / "buffer-410017.sqlite"
    damaged = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    damaged.append("task.event", b"damaged-current-event")
    damaged.close()
    conn = sqlite3.connect(str(source), isolation_level=None)
    try:
        conn.execute("DROP TABLE _meta")
    finally:
        conn.close()
    old = time.time() - 120
    os.utime(source, (old, old))

    current = BufferStore(
        tmp_path / "buffer-410016.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert _row_count(source) == 1
        records = current.recovery_required()
        assert len(records) == 1
        assert records[0]["attribution"] == "unknown"
        assert "lease unreadable" in records[0]["reason"]
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="requires recovery possession")
def test_unreadable_attribution_never_leaves_source_operational(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deployment B must never drain A after A's stamp read fails."""
    source = tmp_path / "buffer-410001.sqlite"
    owner = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    owner.append("task.event", b"A-event")
    owner.close()
    _age_owner_metadata(source)

    newcomer = BufferStore(
        tmp_path / "buffer-410099.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-B",
    )
    calls = 0

    def unreadable_at_every_attribution_check(
        conn: sqlite3.Connection,
    ) -> str | None:
        nonlocal calls
        calls += 1
        raise buffer_mod.DeploymentIdUnreadableError("database is locked")

    monkeypatch.setattr(
        buffer_mod,
        "_read_deployment_id",
        unreadable_at_every_attribution_check,
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(newcomer, home_dir=tmp_path) == 0
        assert newcomer.drain(limit=10) == []
        assert _row_count(source) == 1
        assert calls >= 1, "the unreadable-attribution fault was not exercised"
    finally:
        newcomer.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX flock ordering")
def test_owner_exit_between_recovery_claims_cannot_cross_attribution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Owner exit makes a later claim possible, never cross-deployment recovery."""
    source = tmp_path / "buffer-410002.sqlite"
    owner = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    owner.append("task.event", b"A-event")

    newcomer = BufferStore(
        tmp_path / "buffer-410098.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-B",
    )
    real_claim = buffer_mod._claim_dead_orphan
    claims = 0

    def owner_exits_after_refused_claim(path: Path) -> int | None:
        nonlocal claims
        claims += 1
        result = real_claim(path)
        if claims == 1:
            assert result is None
            owner.close()
            _age_owner_metadata(source)
        return result

    monkeypatch.setattr(
        buffer_mod,
        "_claim_dead_orphan",
        owner_exits_after_refused_claim,
    )
    old = time.time() - 120
    os.utime(source, (old, old))
    try:
        assert buffer_mod.adopt_orphaned_buffers(newcomer, home_dir=tmp_path) == 0
        assert claims == 1
        assert buffer_mod.adopt_orphaned_buffers(newcomer, home_dir=tmp_path) == 0
        assert claims == 2, "the post-owner-exit possession claim was not exercised"
        assert newcomer.drain(limit=10) == []
        assert _row_count(source) == 1
    finally:
        newcomer.close()
        owner.close()


@pytest.mark.skipif(not hasattr(os, "getuid"), reason="POSIX inode semantics")
def test_silently_noop_flock_never_adopts_live_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lock implementation that accepts every claim supplies no possession."""

    class NoOpFlock:
        LOCK_EX = 2
        LOCK_NB = 4
        LOCK_UN = 8

        @staticmethod
        def flock(fd: int, operation: int) -> None:
            del fd, operation

    monkeypatch.setattr(buffer_mod, "fcntl", NoOpFlock())
    monkeypatch.setattr(buffer_mod, "_pid_is_alive", lambda pid: False)

    source = tmp_path / "buffer-999991.sqlite"
    owner = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    owner.append("task.event", b"live-event")
    owner._conn.execute(
        "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
        (buffer_mod._LEASE_HEARTBEAT_KEY, "0"),
    )
    old = time.time() - 120
    os.utime(source, (old, old))

    current = BufferStore(
        tmp_path / "buffer-999992.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert current.size() == 0
        assert source.exists()
    finally:
        current.close()
        owner.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX flock ordering")
def test_empty_close_unlinks_before_releasing_possession(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successor writing after lock release must not be unlinked by close."""
    path = tmp_path / "buffer-410004.sqlite"
    closing = BufferStore(
        path,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )

    real_release = buffer_mod._release_lock
    real_inode_match = buffer_mod._path_still_matches_locked_inode
    released = threading.Event()
    successor_created = threading.Event()
    retired = path.with_suffix(".retired")

    def create_successor() -> None:
        if successor_created.is_set():
            return
        if closing.path.exists():
            closing.path.rename(retired)
        successor = sqlite3.connect(str(closing.path))
        try:
            successor.execute(
                "CREATE TABLE entries ("
                "id INTEGER PRIMARY KEY, kind TEXT, payload BLOB, "
                "created_at REAL, attempts INTEGER"
                ")"
            )
            successor.execute(
                "INSERT INTO entries(kind, payload, created_at, attempts) "
                "VALUES ('task.event', ?, 0, 0)",
                (b"successor-event",),
            )
            successor.commit()
        finally:
            successor.close()
        successor_created.set()

    def release_then_maybe_create_successor(fd: int | None) -> None:
        real_release(fd)
        if fd is None:
            return
        released.set()
        # Correct ordering unlinked while possession was held, so the successor
        # can now create the vacated pathname. A broken early release leaves the
        # old path present; the inode-check hook below stages the successor only
        # after that check has observed the old inode.
        if not closing.path.exists():
            create_successor()

    def match_then_stage_released_successor(candidate: Path, fd: int) -> bool:
        matched = real_inode_match(candidate, fd)
        if matched and released.is_set():
            create_successor()
        return matched

    monkeypatch.setattr(
        buffer_mod,
        "_release_lock",
        release_then_maybe_create_successor,
    )
    monkeypatch.setattr(
        buffer_mod,
        "_path_still_matches_locked_inode",
        match_then_stage_released_successor,
    )
    closing.close()
    try:
        assert successor_created.is_set()
        assert path.exists(), "close unlinked a third party's successor inode"
        assert _row_count(path) == 1
    finally:
        monkeypatch.setattr(buffer_mod, "_release_lock", real_release)
        for base in (path, retired):
            for suffix in ("", "-wal", "-shm"):
                Path(str(base) + suffix).unlink(missing_ok=True)


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX flock ordering")
def test_recovery_fsyncs_unlink_before_releasing_possession(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A drained source's unlink is durable before possession is released."""
    source = tmp_path / "buffer-510001.sqlite"
    owner = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    owner.append("task.event", b"recovered")
    owner.close()
    _age_owner_metadata(source)

    current = BufferStore(
        tmp_path / "buffer-510002.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    events: list[str] = []
    real_fsync = buffer_mod._fsync_directory
    real_release = buffer_mod._release_lock

    def record_fsync(directory: Path) -> None:
        assert directory == source.parent
        events.append("fsync")
        real_fsync(directory)

    def record_release(fd: int | None) -> None:
        events.append("release")
        real_release(fd)

    monkeypatch.setattr(buffer_mod, "_fsync_directory", record_fsync)
    monkeypatch.setattr(buffer_mod, "_release_lock", record_release)
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert not source.exists()
        assert events[:2] == ["fsync", "release"]
    finally:
        current.close()


def test_new_buffer_is_sealed_ready_before_it_becomes_operational(
    tmp_path: Path,
) -> None:
    path = tmp_path / "buffer-410005.sqlite"
    store = BufferStore(
        path,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert store.append("task.event", b"ready") > 0
        assert buffer_mod._read_lifecycle(store._conn) == buffer_mod._LIFECYCLE_SEALED_READY
        assert buffer_mod._read_buffer_uuid(store._conn) == store.buffer_uuid
    finally:
        store.close()


def test_non_lock_initialization_error_never_retries_without_possession(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fail_with_io_error(self, path, deployment_id):
        nonlocal calls
        del self, path, deployment_id
        calls += 1
        raise sqlite3.OperationalError("disk I/O error")

    real_release = buffer_mod._release_lock
    released: list[int] = []
    real_open_capability = buffer_mod._open_exclusive_path_capability
    capability_fds: list[int] = []

    def record_release(fd: int | None) -> None:
        if fd is not None:
            released.append(fd)
        real_release(fd)

    def record_open_capability(path: Path) -> int:
        fd = real_open_capability(path)
        capability_fds.append(fd)
        return fd

    monkeypatch.setattr(
        buffer_mod.BufferStore,
        "_initialize_created_file",
        fail_with_io_error,
    )
    monkeypatch.setattr(buffer_mod, "_release_lock", record_release)
    monkeypatch.setattr(
        buffer_mod,
        "_open_exclusive_path_capability",
        record_open_capability,
    )
    path = tmp_path / "buffer-410015.sqlite"
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        BufferStore(
            path,
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )
    assert calls == 1
    assert len(capability_fds) == 1
    with pytest.raises(OSError):
        os.fstat(capability_fds[0])
    expected_lock_releases = 1 if buffer_mod.fcntl is not None else 0
    assert len(released) == expected_lock_releases
    assert not path.exists()


def test_contended_new_inode_is_not_unlinked_without_possession(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        buffer_mod,
        "_acquire_own_lock",
        lambda path: (None, True),
    )
    path = tmp_path / "buffer-410016.sqlite"
    with pytest.raises(buffer_mod.BufferOwnershipError):
        BufferStore(
            path,
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )
    assert path.exists()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_initializing_source_is_preserved_and_recorded_for_recovery(
    tmp_path: Path,
) -> None:
    source = tmp_path / "buffer-410006.sqlite"
    old = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    old.append("task.event", b"not-ready")
    old.close()
    conn = sqlite3.connect(str(source))
    try:
        conn.execute(
            "UPDATE _meta SET value = ? WHERE key = ?",
            (
                buffer_mod._LIFECYCLE_INITIALIZING,
                buffer_mod._LIFECYCLE_KEY,
            ),
        )
        conn.execute(
            "UPDATE _meta SET value = '0' WHERE key = ?",
            (buffer_mod._LEASE_HEARTBEAT_KEY,),
        )
        conn.commit()
    finally:
        conn.close()
    aged = time.time() - 120
    os.utime(source, (aged, aged))

    current = BufferStore(
        tmp_path / "buffer-410007.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert source.exists()
        assert _row_count(source) == 1
        records = current.recovery_required()
        assert len(records) == 1
        assert records[0]["lifecycle"] == buffer_mod._LIFECYCLE_INITIALIZING
        assert records[0]["row_count"] is None
    finally:
        current.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX process generations")
def test_forked_child_cannot_use_or_unlock_parent_buffer(tmp_path: Path) -> None:
    path = tmp_path / "buffer-410008.sqlite"
    store = BufferStore(
        path,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    child = os.fork()
    if child == 0:  # pragma: no cover - asserted through exit status
        try:
            try:
                store.append("task.event", b"child")
            except RuntimeError:
                store.close()
                os._exit(0)
            os._exit(2)
        except BaseException:
            os._exit(3)

    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    # The child's close dropped only its inherited descriptor reference; it did
    # not unlock the parent's open-file description.
    fd, contended = buffer_mod._acquire_own_lock(store.path)
    try:
        assert fd is None
        assert contended is True
        store.append("task.event", b"parent")
        assert store.size() == 1
    finally:
        buffer_mod._release_lock(fd)
        store.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory ownership")
def test_active_buffer_degrades_without_refusing_unsafe_directory(
    tmp_path: Path,
) -> None:
    tmp_path.chmod(0o777)
    try:
        store = BufferStore(
            tmp_path / "buffer-410009.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )
        active = store.path
        assert active.parent != tmp_path
        store.append("task.event", b"supported-filesystem-degrade")
        assert store.size() == 1
        assert store._automatic_recovery_enabled is False
        store.confirm([store.drain(limit=1)[0].id])
        store.close()
        assert not active.exists()
    finally:
        tmp_path.chmod(0o700)


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory ownership")
def test_unsafe_directory_retry_stays_inside_verified_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An O_EXCL collision must not move a degraded sink back to the unsafe root."""
    real_open = buffer_mod._open_exclusive_path_capability
    create_attempts = 0

    def collide_first_buffer_create(path: Path) -> int:
        nonlocal create_attempts
        create_attempts += 1
        if create_attempts == 1:
            raise FileExistsError(path)
        return real_open(path)

    tmp_path.chmod(0o777)
    monkeypatch.setattr(
        buffer_mod,
        "_open_exclusive_path_capability",
        collide_first_buffer_create,
    )
    try:
        store = BufferStore(
            tmp_path / "buffer-410020.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-A",
        )
        try:
            assert create_attempts >= 2
            assert store.path.parent != tmp_path
            assert buffer_mod._buffer_directory_trust(store.path.parent)[0] is True
        finally:
            store.close()
    finally:
        tmp_path.chmod(0o700)


def test_empty_close_unlinks_even_when_recovery_advisories_exist(
    tmp_path: Path,
) -> None:
    store = BufferStore(
        tmp_path / "buffer-410017.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    active = store.path
    store._conn.execute(
        "INSERT INTO _recovery_required("
        "buffer_uuid, path, observed_dev, observed_ino, attribution, "
        "lifecycle, row_count, reason, observed_at"
        ") VALUES (?, ?, 1, 2, 'foreign', 'unknown', NULL, 'test', 0)",
        ("00000000000000000000000000000001", str(tmp_path / "foreign.sqlite")),
    )
    store.close()
    assert not active.exists()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_recovery_backpressure_never_evicts_live_sink_rows(tmp_path: Path) -> None:
    source = tmp_path / "buffer-410010.sqlite"
    old = BufferStore(
        source,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    old.append("task.event", b"one")
    old.append("task.event", b"two")
    old.close()
    _age_owner_metadata(source)

    current = BufferStore(
        tmp_path / "buffer-410011.sqlite",
        max_entries=1,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert current.size() == 1
        assert source.exists()
        assert _row_count(source) == 1
        assert len(current.recovery_required()) == 1
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_recovery_advisory_survives_active_sink_recovery(tmp_path: Path) -> None:
    foreign = tmp_path / "buffer-410012.sqlite"
    foreign_store = BufferStore(
        foreign,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-B",
    )
    foreign_store.append("task.event", b"foreign")
    foreign_store.close()
    _age_owner_metadata(foreign)

    old_active = BufferStore(
        tmp_path / "buffer-410013.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    assert buffer_mod.adopt_orphaned_buffers(old_active, home_dir=tmp_path) == 0
    assert len(old_active.recovery_required()) == 1
    old_active.append("task.event", b"carry-advisory")
    old_path = old_active.path
    old_active.close()
    _age_owner_metadata(old_path)

    current = BufferStore(
        tmp_path / "buffer-410014.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert not old_path.exists()
        assert current.size() == 1
        records = current.recovery_required()
        assert len(records) == 1
        assert records[0]["path"] == str(foreign)
        assert records[0]["attribution"] == "foreign"
    finally:
        current.close()


@pytest.mark.skipif(buffer_mod.fcntl is None, reason="POSIX recovery possession")
def test_bounded_scans_round_robin_past_manual_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(buffer_mod, "_RECOVERY_MAX_FILES_PER_SCAN", 2)
    for suffix in ("100", "101"):
        foreign = BufferStore(
            tmp_path / f"buffer-{suffix}.sqlite",
            max_entries=100,
            max_bytes=1_000_000,
            deployment_id="deployment-B",
        )
        foreign.append("task.event", b"foreign")
        foreign.close()
        _age_owner_metadata(tmp_path / f"buffer-{suffix}.sqlite")

    same_path = tmp_path / "buffer-102.sqlite"
    same = BufferStore(
        same_path,
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    same.append("task.event", b"same")
    same.close()
    _age_owner_metadata(same_path)

    current = BufferStore(
        tmp_path / "buffer-999.sqlite",
        max_entries=100,
        max_bytes=1_000_000,
        deployment_id="deployment-A",
    )
    try:
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 0
        assert same_path.exists()
        assert buffer_mod.adopt_orphaned_buffers(current, home_dir=tmp_path) == 1
        assert not same_path.exists()
        assert current.size() == 1
    finally:
        current.close()
