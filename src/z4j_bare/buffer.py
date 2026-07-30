"""Local SQLite buffer for agent events and outgoing frames.

The buffer is the agent's crash-safety net. Events captured by engine
adapters are appended here first, then drained by the transport layer
and forwarded to the brain. If the brain is unreachable, the buffer
fills; if the agent crashes, the buffer persists to disk and is
flushed on the next startup.

Design constraints:

- **Bounded**: oldest entries are dropped when size/byte limits are hit.
  The buffer never grows unbounded.
- **Non-blocking** to the host app: all operations are fast enough to
  call from an engine signal/middleware/hook without measurable impact
  (Celery signal, RQ Job callback, Dramatiq middleware, etc.).
- **Crash-safe**: SQLite in WAL mode with ``synchronous=NORMAL`` gives
  us durability across process crashes.
- **Thread-safe**: a single ``threading.Lock`` guards the connection.
  All buffer ops happen on the agent's background thread, but we still
  protect against surprise callers.

We deliberately use raw ``sqlite3`` (stdlib) rather than ``aiosqlite``
to keep z4j-bare's dependency footprint minimal.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import logging
import math
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

try:
    # POSIX advisory whole-file lock. Absent on Windows -- there the
    # ownership-lock liveness signal is unavailable and orphan adoption is
    # conservatively skipped (see _claim_dead_orphan).
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None  # type: ignore[assignment]
from pathlib import Path

from z4j_core.errors import BufferStorageError

from z4j_bare.storage import (
    ensure_buffer_root_writable,
    is_writable_dir,
)

logger = logging.getLogger("z4j.runtime.buffer")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS entries (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    kind             TEXT    NOT NULL,
    payload          BLOB    NOT NULL,
    created_at       REAL    NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    content_rejects  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_entries_created_at
    ON entries (created_at);
CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS _recovery_required (
    buffer_uuid   TEXT PRIMARY KEY,
    path          TEXT NOT NULL,
    observed_dev  INTEGER NOT NULL,
    observed_ino  INTEGER NOT NULL,
    attribution   TEXT NOT NULL,
    lifecycle     TEXT NOT NULL,
    row_count     INTEGER,
    reason        TEXT NOT NULL,
    observed_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS _external_schedule_authority (
    owner                       TEXT    NOT NULL,
    source_scope                TEXT    NOT NULL,
    stream_id                   TEXT    NOT NULL,
    epoch_uuid                  TEXT    NOT NULL,
    epoch_number                INTEGER NOT NULL,
    adapter_instance_id         TEXT    NOT NULL,
    process_generation          TEXT    NOT NULL,
    last_sequence               INTEGER NOT NULL DEFAULT 0,
    updated_at                  REAL    NOT NULL,
    PRIMARY KEY (owner, source_scope),
    UNIQUE (stream_id, epoch_uuid),
    CHECK (epoch_number > 0),
    CHECK (last_sequence >= 0)
);
CREATE TABLE IF NOT EXISTS _external_schedule_control_reservations (
    operation_id               TEXT    PRIMARY KEY,
    owner                      TEXT    NOT NULL,
    source_scope               TEXT    NOT NULL,
    stream_id                  TEXT    NOT NULL,
    epoch_uuid                 TEXT    NOT NULL,
    epoch_number               INTEGER NOT NULL,
    adapter_instance_id        TEXT    NOT NULL,
    process_generation         TEXT    NOT NULL,
    expected_sequence          INTEGER NOT NULL,
    reserved_sequence          INTEGER NOT NULL,
    desired_projection_digest  TEXT    NOT NULL,
    status                     TEXT    NOT NULL,
    entry_id                   INTEGER,
    created_at                 REAL    NOT NULL,
    updated_at                 REAL    NOT NULL,
    CHECK (epoch_number > 0),
    CHECK (expected_sequence >= 0),
    CHECK (reserved_sequence = expected_sequence + 1),
    CHECK (status IN ('RESERVED', 'PUBLISHED')),
    CHECK (
        (status = 'RESERVED' AND entry_id IS NULL)
        OR (status = 'PUBLISHED' AND entry_id IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS
    uq_external_schedule_control_reserved_scope
    ON _external_schedule_control_reservations (owner, source_scope)
    WHERE status = 'RESERVED';
"""

#: ``attempts`` is a general stuck-entry METRIC (incremented by the WS
#: ack-watchdog on a re-send, and historically by pre-1.7 transport
#: failures); it must NOT gate any drop. ``content_rejects`` is the
#: DEDICATED bounded drop budget: only a long-poll per-frame content
#: rejection (413/415/422) increments it, and only it is consulted by
#: ``evict_if_exhausted``. Keeping them physically separate stops WS-timeout
#: (or cross-version pre-1.7) ``attempts`` history from destructively priming
#: the content-drop budget so a deliverable event is dropped on its FIRST
#: content reject.

# Pragmas applied to every connection for durability and speed.
# WAL = write-ahead log (crash-safe + concurrent readers)
# synchronous=NORMAL = sync on WAL checkpoint, not on every commit
# temp_store=MEMORY = temp tables in memory
# mmap_size = larger memory-mapped region for read performance
_PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA mmap_size=67108864",  # 64 MiB
    "PRAGMA busy_timeout=5000",
)

#: Durable causal entries are not ordinary telemetry.  They may be removed
#: only after the Brain acknowledges the exact event-batch frame; generic
#: bounded eviction and poison-frame cleanup must preserve them.
EXTERNAL_SCHEDULE_ENTRY_KIND = "external_schedule_projection"


#: Module-level flag so the Z4J_HOME perms warning fires at most
#: once per process (not once per BufferStore construction).
_z4j_home_perms_warned: bool = False


class ExternalScheduleAuthorityError(RuntimeError):
    """The active process cannot publish for the requested stream epoch."""


@dataclass(frozen=True, slots=True)
class ExternalScheduleControlReservation:
    """One durable, gap-blocking external control sequence reservation."""

    sequence: int
    already_published: bool


def _warn_if_z4j_home_loose(db_path: Path) -> None:
    """Warn if the buffer's parent directory is group/world accessible.

    Added in z4j-bare 1.6.5 (security advisory P1). When the
    operator sets ``Z4J_HOME`` (or accepts the default ``~/.z4j``)
    on a multi-tenant host, the directory may have permissive
    bits inherited from a parent or set deliberately for some
    other reason. We don't presume to chmod the directory itself
    (the operator may have reasons), but we DO want to surface
    the risk so an operator who didn't intend a shared-host setup
    sees the warning in the worker logs.

    No-op on Windows.

    Fires at most once per process via a module-level guard.
    """
    global _z4j_home_perms_warned  # noqa: PLW0603  module-level one-shot warn guard
    if _z4j_home_perms_warned or os.name != "posix":
        return
    parent = db_path.parent
    try:
        mode = parent.stat().st_mode & 0o777
    except OSError:
        return
    if mode & 0o077:  # any group or other permission bit set
        logger.warning(
            "z4j-bare buffer: directory %s has mode 0%o "
            "(group/world accessible). The buffer database "
            "contains task/event payload bytes (potentially PII). "
            "Run `chmod 700 %s` (or set Z4J_HOME to a private "
            "directory) before the next agent start to harden.",
            parent,
            mode,
            parent,
        )
    _z4j_home_perms_warned = True


def _restrict_buffer_files(db_path: Path) -> None:
    """Force owner-only permissions (0600) on the buffer DB and its
    SQLite sidecar files.

    Added in z4j-bare 1.6.5 (security advisory P1). The buffer
    stores task/event payload BLOBs that may contain PII. On a
    multi-tenant host where ``Z4J_HOME`` was created with a
    permissive umask (or points to a pre-existing world-readable
    directory) the SQLite ``connect`` call inherits the umask and
    creates the DB world-readable. This helper re-tightens after
    the file exists.

    Sidecar files (``-wal``, ``-shm``) are created lazily by SQLite
    on first write; we attempt to chmod them too, tolerating
    ``FileNotFoundError`` when they haven't materialised yet
    (subsequent writes recreate them, and operators who care can
    re-run a maintenance task -- but typical agent workloads
    produce both files within the first second).

    Best-effort: errors are logged at WARN level but do NOT raise.
    Some filesystems (tmpfs without perm semantics, FAT-on-USB,
    SMB mounts) intentionally ignore chmod; the agent must still
    start in those environments.

    No-op on Windows (POSIX permissions don't apply).
    """
    if os.name != "posix":
        return

    # Derive sidecar paths by string-append, NOT Path.with_suffix: SQLite names
    # them ``<db>-wal`` / ``<db>-shm`` (append, not suffix-replace), and on
    # Python 3.14 with_suffix rejects a suffix without a leading dot (breaking a
    # suffix-less path such as an in-memory DB). str-append matches the WAL/SHM
    # naming exactly and never raises on an odd path.
    targets = [
        db_path,
        Path(str(db_path) + "-wal"),
        Path(str(db_path) + "-shm"),
    ]
    for target in targets:
        try:
            target.chmod(0o600)
        except FileNotFoundError:
            # Sidecar files may not exist until the first write.
            continue
        except OSError as exc:
            logger.warning(
                "z4j-bare buffer: chmod 0600 failed on %s: %s "
                "(buffer may be world-readable; ensure Z4J_HOME "
                "permissions are tight)",
                target,
                exc,
            )


@dataclass(frozen=True, slots=True)
class BufferEntry:
    """A single buffered frame awaiting transmission to the brain.

    Attributes:
        id: Auto-incrementing primary key. Used by :meth:`BufferStore.confirm`
            to delete the entry once the brain has acknowledged it.
        kind: Frame type tag - matches the ``type`` field of the wire
              frame (``"event_batch"``, ``"command_result"``, ...).
        payload: UTF-8 encoded JSON bytes of the frame.
        created_at: Unix timestamp when the entry was appended.
        attempts: How many times we have tried to flush this entry.
                  Incremented on every retry; used to surface a
                  "stuck" entry in metrics.
    """

    id: int
    kind: str
    payload: bytes
    created_at: float
    attempts: int


@dataclass(frozen=True, slots=True)
class _Possession:
    """A held capability for one buffer inode and one process generation."""

    fd: int | None
    dev: int
    ino: int
    pid: int
    generation: str
    locking_supported: bool

    def belongs_to_current_process(self) -> bool:
        return self.pid == os.getpid()


class BufferStore:
    """A crash-safe local queue of outgoing frames.

    Lifecycle:

    1. Constructed with a ``path`` (a ``~/.z4j/buffer.sqlite`` by default).
       A production store exclusively creates and seals a fresh active inode;
       any pre-existing path remains a separate recovery source.
    2. Events are appended via :meth:`append`. The id is assigned
       automatically and returned.
    3. The transport loop calls :meth:`drain` to pull the oldest N
       entries. It ships them to the brain, then calls
       :meth:`confirm` with the ids to delete them.
    4. On shutdown, :meth:`close` removes an empty file while still holding
       possession. A non-empty file remains for bounded recovery by a later
       process.

    The store uses a single ``threading.Lock`` to serialize access.
    Agent runtime code runs on a single background thread, so
    contention is expected to be near zero - the lock is defense in
    depth against accidental callers from other threads.
    """

    def __init__(  # noqa: PLR0915 - legacy setup plus isolated production path
        self,
        path: Path,
        *,
        max_entries: int = 100_000,
        max_bytes: int = 256 * 1024 * 1024,
        deployment_id: str | None = None,
    ) -> None:
        # Resolve the actual on-disk path. Most callers pass the
        # default from Config.buffer_path; if that path's parent is
        # not writable (service user with unwritable HOME, e.g.
        # gunicorn under www-data) we silently relocate to the per-uid
        # tmp fallback. The original filename is preserved so that
        # ``buffer-{pid}.sqlite`` continues to namespace per-process.
        path = self._resolve_writable_path(path)

        self._path = path
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self.deployment_id: str | None = deployment_id
        self._creator_pid = os.getpid()
        self._process_generation = f"{self._creator_pid}-{time.time_ns()}-{os.urandom(8).hex()}"
        self._possession: _Possession | None = None
        self._path_capability_fd: int | None = None
        self._directory_trusted = True
        self._automatic_recovery_enabled = True
        self._sealed_ready = deployment_id is None
        self._recovery_scan_cursor: str | None = None
        # A UNIQUE per-OPEN owner token. Stamped into _meta while we own
        # the inode and cleared on a clean close, it lets the no-flock open path
        # tell a concurrent LIVE peer (a different fresh token) from this owner's
        # own sequential reopen (no token, because close cleared it).
        self._owner_token = f"{os.getpid()}-{os.urandom(6).hex()}"
        # Was a FRESH liveness lease already present when we claimed this
        # inode? Our own claim now stamps a lease atomically with the token, so
        # re-reading the lease later cannot distinguish "a live owner is using
        # this file" from "we just wrote our own". Captured at claim time.
        self._lease_was_fresh_before_claim = False

        # Boundary C: a production buffer (one with deployment attribution)
        # never activates an existing path. It exclusively creates a fresh sink,
        # seals it READY, and treats every older path as a recovery source. This
        # removes classification from startup: unreadable/foreign/incomplete
        # metadata can delay recovery, but can never leave that source operational.
        if deployment_id is not None:
            self._init_fresh_active(path, deployment_id)
            return

        self._open(path)
        self._closed = False

        # P1-7: take the lifetime ownership flock BEFORE any destructive
        # deployment-attribution work below (discarding foreign rows / re-
        # stamping). Orphan adoption gates on THIS same flock (_claim_dead_
        # orphan), so acquiring it first makes self-claim and adoption mutually
        # exclusive through one lock -- we never DELETE rows an adopter is
        # concurrently draining, and an adopter never drains rows we are
        # re-stamping. A CONTENDED lock means a live peer / in-progress adopter
        # owns the file right now, so the discard below is suppressed (fail
        # closed: never wipe rows another process may still deliver).
        self._lock_fd, self._lock_contended = _acquire_own_lock(path)
        if self._lock_contended:
            # On some filesystems our OWN open SQLite connection blocks our
            # OWN flock. WSL's DrvFS (a Windows drive mounted at /mnt/...) is the
            # observed case: with WAL plus mmap active, flock on the database file
            # returns EAGAIN, which is byte-identical to a live peer holding it.
            # The agent therefore relocated, hit the same refusal on the fresh
            # sibling, and REFUSED TO START at all on such a mount.
            #
            # Locking before opening SQLite does not help: while we hold the
            # flock, SQLite itself cannot write ("database is locked"). On this
            # filesystem the two are simply mutually exclusive on one file, so
            # the only honest outcome is to recognise that and fall back to the
            # already-supported degraded mode rather than fail closed.
            #
            # The probe distinguishes the two causes without weakening genuine
            # contention: drop our connection, retry the lock, and reopen. If the
            # retry SUCCEEDS the blocker was our own SQLite, so this filesystem
            # cannot do both and locking is unavailable here. If it still fails,
            # a real peer holds the file and the relocation below is correct.
            self._lock_fd, self._lock_contended = self._classify_lock_conflict(path)

        # A CONTENDED path means another LIVE process owns this exact
        # file right now. Suppressing only the discard is not enough -- this store
        # would still be fully operational on the SHARED file (append / drain /
        # confirm / unlink the peer's rows: the H4 corruption). The default path
        # is per-pid (buffer-<pid>.sqlite), but an operator may point an explicit
        # buffer_path at one shared file across worker processes (docs show
        # /var/lib/z4j/buffer.sqlite). Relocate to a per-pid sibling in the SAME
        # operator-provisioned directory -- the model reinit_after_fork already
        # uses -- so each live process owns its own file and the peer's data is
        # never touched. The vacated shared path keeps belonging to its owner.
        # Relocate to a per-pid sibling when a LIVE peer owns
        # this exact inode. Two ownership signals:
        #   - a CONTENDED flock (definitive: another process holds it now); or
        #   on a no-flock platform, a FRESH FOREIGN OWNER-TOKEN. showed
        #     that relying on SQLite serialization + discard-suppression alone
        #     lets two live no-fcntl processes on one explicit shared buffer_path
        #     drain/confirm each other's rows (counter drift, duplicate delivery).
        #     The owner-token resolves the ambiguity could not: a clean close
        #     CLEARS the token, so a sequential reopen / same-deployment restart
        #     sees no owner and does NOT relocate (its rows survive), while a
        #     genuinely-concurrent live peer's token is present + fresh and we
        #     relocate off the shared inode. The default per-pid path never shares,
        #     so this only ever fires under the documented shared-path misconfig.
        if self._lock_contended or self._no_flock_foreign_owner():
            _release_lock(self._lock_fd)
            self._lock_fd = None
            with contextlib.suppress(sqlite3.Error):
                self._conn.close()
            # Relocate to a GLOBALLY-UNIQUE per-open sibling (pid + a random
            # tail), not just ``buffer-<pid>.sqlite``. A pid-only target could
            # itself be a live owner (a pid-namespace clash on a shared dir, or two
            # stores in one process), and the newcomer would then SHARE and delete
            # that owner's rows. A freshly-random name is guaranteed to have empty
            # _meta and no live owner, so relocation can never land on a shared
            # inode -- which also makes the post-relocation ownership check below
            # effectively unreachable (kept as a belt-and-suspenders).
            relocated = self._resolve_writable_path(
                path.parent / f"buffer-{os.getpid()}-{os.urandom(6).hex()}.sqlite",
            )
            logger.warning(
                "z4j buffer: %s is held by another live process; relocating "
                "this process to its own private buffer %s (a shared buffer_path "
                "across worker processes is unsafe -- prefer the per-pid default)",
                path,
                relocated,
            )
            self._path = relocated
            self._open(relocated)
            # (Belt and braces): a freshly-random sibling is unowned BY
            # CONSTRUCTION, so nothing about the inode we just walked away from
            # may leak into how we attribute this one. The claim now publishes
            # the flag only on its success path, so this should already be False.
            self._lease_was_fresh_before_claim = False
            self._lock_fd, self._lock_contended = _acquire_own_lock(relocated)
            # Belt-and-suspenders: a freshly-random sibling has no owner, so this
            # never fires in practice; retained only for a pathological same-name
            # collision. Gate on flock CONTENTION only (definitive). The M6 guard
            # below releases the lock + closes the conn; start()'s RM7 guard resets
            # state to STOPPED (agent disabled, host app unharmed).
            if self._lock_contended:
                _release_lock(self._lock_fd)
                self._lock_fd = None
                with contextlib.suppress(sqlite3.Error):
                    self._conn.close()
                self._closed = True
                raise BufferOwnershipError(
                    f"buffer {self._path} is still owned by a live peer after "
                    "relocation; refusing to run on a shared inode",
                )

        try:
            self._init_after_lock(deployment_id)
        except BufferForeignDataError:
            # This file holds another deployment's events. Do NOT delete
            # them and do NOT emit them -- step aside. Release the lock, close
            # the connection, and open a fresh private buffer of our own, leaving
            # the original byte-for-byte intact for an operator.
            #
            # The old behaviour deleted them, which is unrecoverable, and the
            # evidence it acted on (a fingerprint derived from the hmac_secret)
            # reads as foreign after an ordinary secret rotation.
            logger.exception(
                "z4j buffer: deployment attribution failed -- leaving the file "
                "untouched and starting a fresh "
                "buffer. Its undelivered events are still on disk. If this "
                "followed an agent-secret rotation or a restore they are very "
                "likely YOURS: they can be recovered by pointing an agent with "
                "the previous secret at that file",
            )
            _release_lock(self._lock_fd)
            self._lock_fd = None
            with contextlib.suppress(sqlite3.Error):
                self._conn.close()
            self._lease_was_fresh_before_claim = False
            relocated = self._resolve_writable_path(
                path.parent / f"buffer-{os.getpid()}-{os.urandom(6).hex()}.sqlite",
            )
            self._path = relocated
            self._open(relocated)
            self._lock_fd, self._lock_contended = _acquire_own_lock(relocated)
            # A freshly-random sibling is unowned by construction, so this
            # cannot recurse: its _meta is empty, so attribution takes the
            # "no stamp, stamp it as ours" path.
            self._init_after_lock(deployment_id)
        except BaseException:
            # M6: release the lifetime flock + close the connection if
            # construction fails AFTER the lock was taken (an H5 discard failure,
            # a restamp/counter sqlite error, etc.). Without this the flocked fd
            # leaks for the process lifetime, so a later self-claim or adoption of
            # this same inode from the SAME process (flock is per open-file-
            # description) blocks forever and the buffer can never be reclaimed.
            _release_lock(self._lock_fd)
            self._lock_fd = None
            with contextlib.suppress(sqlite3.Error):
                self._conn.close()
            self._closed = True
            raise

    def _init_fresh_active(  # noqa: PLR0912, PLR0915 - ordered rollback
        self,
        requested: Path,
        deployment_id: str,
    ) -> None:
        """Create and seal the sole active sink for this process generation.

        Existing paths are never opened during startup. They remain recovery
        sources and are considered later only after possession is established.
        """
        self._last_lease_ts = 0.0
        self._closed = False
        self._sealed_ready = False
        self.buffer_uuid = uuid.uuid4().hex

        active = self._exclusively_create_active_path(requested)
        self._path = active
        self._lock_fd, self._lock_contended = _acquire_own_lock(active)
        if self._lock_contended:
            self._close_path_capability()
            self._closed = True
            raise BufferOwnershipError(f"new buffer {active} could not be exclusively possessed")
        if self._lock_fd is None:
            self._automatic_recovery_enabled = False
        elif not _same_open_inode(self._path_capability_fd, self._lock_fd):
            _release_lock(self._lock_fd)
            self._lock_fd = None
            self._close_path_capability()
            self._closed = True
            raise BufferOwnershipError(f"new buffer {active} changed identity before possession")

        try:
            self._initialize_created_file(active, deployment_id)
        except sqlite3.OperationalError as exc:
            # DrvFS and a few network filesystems cannot use SQLite while an
            # flock is held. The path is exclusively created and globally unique,
            # so dropping the unsupported lock does not make it shared.
            if self._lock_fd is None or "locked" not in str(exc).lower():
                conn = getattr(self, "_conn", None)
                if conn is not None:
                    with contextlib.suppress(sqlite3.Error):
                        conn.close()
                self._cleanup_unsealed_active(active)
                _release_lock(self._lock_fd)
                self._lock_fd = None
                self._close_path_capability()
                self._closed = True
                raise
            _release_lock(self._lock_fd)
            self._lock_fd = None
            self._automatic_recovery_enabled = False
            conn = getattr(self, "_conn", None)
            if conn is not None:
                with contextlib.suppress(sqlite3.Error):
                    conn.close()
            try:
                self._initialize_created_file(active, deployment_id)
            except BaseException:
                conn = getattr(self, "_conn", None)
                if conn is not None:
                    with contextlib.suppress(sqlite3.Error):
                        conn.close()
                self._cleanup_unsealed_active(active)
                self._close_path_capability()
                self._closed = True
                raise
            logger.warning(
                "z4j buffer: %s cannot combine SQLite with an ownership flock; "
                "using this exclusively-created per-generation path without "
                "automatic recovery on this filesystem",
                active,
            )
        except BaseException:
            conn = getattr(self, "_conn", None)
            if conn is not None:
                with contextlib.suppress(sqlite3.Error):
                    conn.close()
            self._cleanup_unsealed_active(active)
            _release_lock(self._lock_fd)
            self._lock_fd = None
            self._close_path_capability()
            self._closed = True
            raise

        if self._path_capability_fd is None or not _path_still_matches_locked_inode(
            active,
            self._path_capability_fd,
        ):
            with contextlib.suppress(sqlite3.Error):
                self._conn.close()
            self._cleanup_unsealed_active(active)
            _release_lock(self._lock_fd)
            self._lock_fd = None
            self._close_path_capability()
            self._closed = True
            raise BufferOwnershipError(
                f"new buffer {active} changed identity during initialization"
            )
        try:
            st = os.fstat(self._path_capability_fd)
        except OSError:
            with contextlib.suppress(sqlite3.Error):
                self._conn.close()
            self._cleanup_unsealed_active(active)
            _release_lock(self._lock_fd)
            self._lock_fd = None
            self._close_path_capability()
            self._closed = True
            raise
        self._possession = _Possession(
            fd=(self._lock_fd if self._lock_fd is not None else self._path_capability_fd),
            dev=int(st.st_dev),
            ino=int(st.st_ino),
            pid=self._creator_pid,
            generation=self._process_generation,
            locking_supported=self._lock_fd is not None,
        )
        self._sealed_ready = True
        _fsync_directory(active.parent)

    def _exclusively_create_active_path(self, requested: Path) -> Path:
        """Return a path created with ``O_EXCL`` for this process generation."""
        trusted, reason = _buffer_directory_trust(requested.parent)
        self._directory_trusted = trusted
        self._automatic_recovery_enabled = trusted
        candidate = requested
        if not trusted:
            # Some supported filesystems (notably WSL DrvFS) cannot express the
            # owner/mode invariant with POSIX stat bits. Keep the host app
            # running by relocating the active sink to a verified owner-private
            # temp root. Recovery of the requested directory stays disabled.
            fallback = _private_degraded_buffer_root(requested.parent)
            self._directory_trusted = True
            self._automatic_recovery_enabled = False
            candidate = fallback / (f"buffer-{os.getpid()}-{uuid.uuid4().hex}.sqlite")
            logger.warning(
                "z4j buffer: %s cannot prove an owner-controlled directory "
                "(%s); relocating this process generation to %s with automatic "
                "recovery of the requested directory disabled",
                requested.parent,
                reason,
                candidate,
            )
        allocation_parent = candidate.parent
        for _attempt in range(128):
            try:
                fd = _open_exclusive_path_capability(candidate)
            except FileExistsError:
                candidate = allocation_parent / (f"buffer-{os.getpid()}-{uuid.uuid4().hex}.sqlite")
                continue
            except OSError as exc:
                raise BufferStorageError(
                    f"could not create a private z4j buffer at {candidate}: {exc}"
                ) from exc
            else:
                with contextlib.suppress(OSError):
                    os.fchmod(fd, 0o600)
                self._path_capability_fd = fd
                return candidate
        raise BufferStorageError(f"could not allocate a unique z4j buffer in {allocation_parent}")

    def _initialize_created_file(self, path: Path, deployment_id: str) -> None:
        """Persist CREATED -> INITIALIZING -> SEALED_READY on a new inode."""
        self._conn = sqlite3.connect(
            str(path),
            isolation_level=None,
            check_same_thread=False,
            timeout=5.0,
        )
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)",
        )
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for key, value in (
                (_BUFFER_UUID_KEY, self.buffer_uuid),
                (_LIFECYCLE_KEY, _LIFECYCLE_CREATED),
                ("deployment", deployment_id),
                (_OWNER_TOKEN_KEY, self._owner_token),
                (_OWNER_PID_KEY, str(self._creator_pid)),
                (_OWNER_GENERATION_KEY, self._process_generation),
            ):
                self._conn.execute(
                    "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                    (key, value),
                )
            self._conn.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute("ROLLBACK")
            raise
        self._conn.execute(
            "UPDATE _meta SET value = ? WHERE key = ?",
            (_LIFECYCLE_INITIALIZING, _LIFECYCLE_KEY),
        )

        for pragma in _PRAGMAS:
            self._conn.execute(pragma)
        self._conn.executescript(_SCHEMA)
        self._migrate_schema()
        self._write_lease(time.time())
        _restrict_buffer_files(path)
        _warn_if_z4j_home_loose(path)
        (self._cached_count,) = self._conn.execute(
            "SELECT COUNT(*) FROM entries",
        ).fetchone()
        (self._cached_bytes,) = self._conn.execute(
            "SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM entries",
        ).fetchone()
        self._drift_warned = False
        self._conn.execute(
            "UPDATE _meta SET value = ? WHERE key = ?",
            (_LIFECYCLE_SEALED_READY, _LIFECYCLE_KEY),
        )

    def _cleanup_unsealed_active(self, path: Path) -> None:
        """Best-effort rollback for a new sink that never reached READY."""
        if (
            not self._directory_trusted
            or self._path_capability_fd is None
            or not _path_still_matches_locked_inode(path, self._path_capability_fd)
        ):
            return
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(OSError):
                Path(str(path) + suffix).unlink()
        _fsync_directory(path.parent)

    def _close_path_capability(self) -> None:
        fd = self._path_capability_fd
        self._path_capability_fd = None
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)

    def _init_after_lock(self, deployment_id: str | None) -> None:
        """Post-lock construction (H8 attribution, lease, chmod, counters).

        Separated so ``__init__`` can release the flock (M6) if anything here
        raises. Every statement here runs while we hold ``self._lock_fd`` (or
        know locking was unavailable/contended).
        """
        # H8: stamp the buffer with a per-DEPLOYMENT fingerprint (derived by the
        # runtime from the agent's hmac_secret). Orphan adoption refuses a buffer
        # whose fingerprint differs from the adopting agent's, so two unrelated
        # z4j deployments under the SAME OS user can never drain and re-sign each
        # other's undelivered events under the wrong project/session.
        self.deployment_id = deployment_id
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)",
        )
        # RH8: liveness lease. Read/refresh order matters -- _attribute_deployment
        # reads the PREVIOUS owner's lease (H4) BEFORE we stamp our own below.
        self._last_lease_ts = 0.0
        if deployment_id is not None:
            self._attribute_deployment(deployment_id)
        self._write_lease(time.time())
        # Stamp our per-open owner token now that we hold the inode (the
        # connection is autocommit, so this is durable immediately). A clean
        # close DELETEs it, so a later sequential reopen sees no live owner and
        # does not needlessly relocate. Best-effort: a token write must never
        # break buffering.
        with contextlib.suppress(sqlite3.Error):
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                (_OWNER_TOKEN_KEY, self._owner_token),
            )

        # z4j-bare 1.6.5 (security advisory P1): force owner-only mode on the
        # buffer DB + its WAL/SHM sidecars (they hold PII payload BLOBs). Best-
        # effort: a no-op on platforms where it doesn't apply (Windows).
        _restrict_buffer_files(self._path)
        _warn_if_z4j_home_loose(self._path)

        # Cached running totals - sourced from disk on startup, then adjusted
        # incrementally on append/evict/confirm.
        (self._cached_count,) = self._conn.execute(
            "SELECT COUNT(*) FROM entries",
        ).fetchone()
        (self._cached_bytes,) = self._conn.execute(
            "SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM entries",
        ).fetchone()
        self._drift_warned = False

    def _lease_is_fresh(self) -> bool:
        """True if THIS buffer's own (previous owner's) liveness lease is younger
        than the stale window -- a live owner is using it. Fails CLOSED (treats
        the lease as fresh) if it is unreadable or corrupt (M5)."""
        try:
            lease = _read_lease_heartbeat(self._conn)
        except (sqlite3.Error, LeaseUnreadableError):
            return True
        return lease is not None and (time.time() - lease) < _LEASE_STALE_SECONDS

    @property
    def _locking_unavailable(self) -> bool:
        """True when flock is unavailable on THIS buffer's filesystem: no fcntl
        (Windows), or the mount returned EOPNOTSUPP/ENOLCK/etc. at open. Distinct
        from CONTENTION (a live peer holds the lock -> ``_lock_contended``). This
        is the single signal both the no-flock ownership claim and the no-flock
        orphan-adoption fallback key on, so an EOPNOTSUPP mount is not stranded by
        an ``fcntl is None`` check that only catches Windows."""
        return self._lock_fd is None and not self._lock_contended

    def _no_flock_foreign_owner(self) -> bool:
        """On a NO-FLOCK platform, ATOMICALLY claim ownership of
        this inode and report whether a FRESH FOREIGN owner already holds it (so
        the caller relocates). Returns:
          - False (we own it) when the file is unowned, its owner token is OURS (a
            legitimate sequential reopen -- a clean close cleared the token), or
            the prior owner's liveness lease has gone STALE (a dead owner the
            orphan path handles); in that case our token is written under the lock
            so a CONCURRENT opener sees us and backs off.
          - True (a fresh foreign owner) when a DIFFERENT owner token is present
            with a fresh lease -> relocate.

        The version READ the token and only later (in _init_after_lock)
        WROTE ours -- a TOCTOU that let two simultaneous no-flock opens of one
        shared inode both see "unowned" and both proceed. This now performs the
        test-and-set inside a SQLite ``BEGIN IMMEDIATE`` write transaction, whose
        RESERVED lock (SQLite's own file locking -- POSIX fcntl / Windows LockFile,
        which work where flock does not) serializes the two opens: exactly one
        claims the inode, the other reads the winner's fresh token and relocates.
        Fails CLOSED -- treats it as foreign-owned (True -> relocate) on BUSY /
        lock-unavailable / any sqlite error -- so a filesystem whose SQLite locking
        is a no-op never lets two live owners share (they each relocate to their
        own random sibling instead)."""
        if self._lock_fd is not None or self._lock_contended:
            return False
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error:
            # Could not take the write lock -> a concurrent claimer holds it, or
            # locking is a no-op. Fail closed: treat as foreign-owned, relocate.
            return True
        try:
            token = _read_owner_token(self._conn)
            if token is not None and token != self._owner_token and self._lease_is_fresh():
                # A fresh FOREIGN owner holds it -> release the lock, relocate.
                self._conn.execute("ROLLBACK")
                return True
            # Unowned / stale / ours: claim it by writing our token under the lock.
            # Record whether a fresh lease was ALREADY here before we overwrite
            # it: a cleanly-closed peer clears its token but leaves its lease, and
            # that lease is still the signal that a live owner may hold the file.
            #
            # Hold this in a LOCAL and publish it only after the COMMIT
            # below succeeds. Assigning it here bound the flag to an inode we may
            # never end up owning: any sqlite error in the writes/COMMIT falls to
            # the handler below, which relocates us onto a fresh random private
            # sibling, and the STALE True then made _attribute_deployment take its
            # "a live owner may hold this" early return on that brand-new empty
            # file -- so its deployment fingerprint was never stamped, and an
            # unfingerprinted buffer is refused by every adopter (the RH6/H8 rule
            # and the no-flock gate both require a matching deployment id). Its
            # rows became permanently unadoptable instead of recovering.
            lease_was_fresh = self._lease_is_fresh()
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                (_OWNER_TOKEN_KEY, self._owner_token),
            )
            # Stamp the liveness lease in the SAME transaction as the token.
            # Previously the token was committed here and the lease was stamped
            # later (in _init_after_lock), which left a window where a CONCURRENT
            # opener read our token but found NO fresh lease, concluded the inode
            # was not live-owned, overwrote the token and carried on using the
            # SAME inode: two live writers on one SQLite file with no lock.
            # Reproduced deterministically by pausing the first opener inside that
            # window. Writing both under the one BEGIN IMMEDIATE means a
            # concurrent opener always observes token+lease together and relocates.
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                (_LEASE_HEARTBEAT_KEY, str(time.time())),
            )
            self._conn.execute("COMMIT")
            # Published only now: we own this inode, so the flag describes it.
            self._lease_was_fresh_before_claim = lease_was_fresh
            return False
        except sqlite3.Error:
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute("ROLLBACK")
            return True  # fail closed -> relocate

    def _classify_lock_conflict(self, path: Path) -> tuple[int | None, bool]:
        """Re-test a refused lock with our own SQLite connection closed.

        Returns the same ``(fd, contended)`` shape as :func:`_acquire_own_lock`:

        - ``(fd, False)`` -- the retry took the lock and the filesystem tolerates
          holding it alongside SQLite. Unreachable in practice (if our own
          connection was the blocker, holding the lock blocks SQLite in turn), so
          the lock is released and this degrades to the case below.
        - ``(None, False)`` -- our own connection was the blocker: locking is
          unavailable on this filesystem. Best-effort single-process mode, the
          same posture as a platform without flock at all.
        - ``(None, True)`` -- the lock is still refused with nothing of ours
          holding the file, so a live peer really does own it. Unchanged
          behaviour: relocate.

        Runs during construction only, before any event has been buffered, so
        closing and reopening the connection cannot lose data.
        """
        try:
            self._conn.close()
        except sqlite3.Error:
            return None, True  # cannot prove it is ours; keep the safe answer
        probe_fd: int | None = None
        probe_contended = True
        try:
            probe_fd, probe_contended = _acquire_own_lock(path)
        finally:
            # Release BEFORE reopening: on the very filesystem this probe
            # detects, reopening SQLite while we hold the lock is what fails.
            if probe_fd is not None:
                _release_lock(probe_fd)
            self._open(path)  # always restore the connection
        if probe_contended:
            return None, True  # a real peer holds it
        logger.warning(
            "z4j buffer: %s -- this filesystem cannot hold an ownership lock and "
            "an open database at the same time (WSL DrvFS and some network "
            "mounts behave this way), so cross-process ownership checks are "
            "unavailable here. The agent runs normally; give each worker process "
            "its own buffer_path, and prefer a local disk for the buffer",
            path.name,
        )
        return None, False

    def _owner_token_is_ours(self) -> bool:
        """True when this inode's owner token is the one WE wrote.

        The no-flock claim writes our token and our liveness lease under one
        transaction, so our own lease reads as fresh. "Fresh lease" therefore no
        longer means "a foreign live owner" on its own -- it has to be paired
        with a token we did not write. Fails CLOSED (False) on a read error, so
        an unreadable token never lets us claim ownership we cannot prove.
        """
        try:
            token = _read_owner_token(self._conn)
        except sqlite3.Error:
            return False
        return token is not None and token == self._owner_token

    def _attribute_deployment(self, deployment_id: str) -> None:
        """Discard + restamp a reused-pid buffer we cannot prove is ours (RH6).

        H4: only run the destructive discard/restamp when we have PROVEN
        exclusive ownership. We do NOT if the flock is CONTENDED (a live peer
        holds it) or locking was UNAVAILABLE (Windows / no-op flock / open
        failed: ``_lock_fd is None`` and not contended). In either no-proof case
        a FRESH liveness lease means a live owner is using this file, so leave it
        untouched -- otherwise an absent/no-op flock let the cleanup run with no
        ownership proof (a second process could delete/restamp the first's rows).
        """
        if self._lock_contended:
            logger.warning(
                "z4j buffer: %s is ownership-locked by another live process; "
                "leaving its rows and fingerprint untouched (not claiming it)",
                self._path.name,
            )
            return
        # A fresh lease alone no longer proves a FOREIGN live owner. The
        # no-flock claim now writes our owner token AND our lease atomically (to
        # close the simultaneous-open race), so our OWN lease reads as fresh here.
        # Freshness must therefore be paired with a token that is NOT ours;
        # otherwise a process would refuse to stamp its own fingerprint on the
        # inode it just legitimately claimed.
        lease_indicates_live_owner = self._lease_was_fresh_before_claim or (
            self._lease_is_fresh() and not self._owner_token_is_ours()
        )
        if self._lock_fd is None and lease_indicates_live_owner:
            logger.warning(
                "z4j buffer: %s -- no exclusive lock available and its liveness "
                "lease is fresh; leaving its rows and fingerprint untouched",
                self._path.name,
            )
            return
        # RH6: read the existing stamp BEFORE overwriting it and, unless it
        # PROVES the rows are ours, wipe them. H5: _discard_unowned_rows RAISES on
        # a COUNT/DELETE failure, so the restamp below is NOT reached (the foreign
        # rows keep their foreign identity, never mis-attributed) and __init__
        # fails closed via the M6 wrapper.
        # A fingerprint we could not READ is not a fingerprint that differs.
        # Collapsing the two sent a transient SQLite failure down the destructive
        # branch on a buffer that may well have been ours. Fail closed: keep the
        # rows, do NOT restamp, and let this open proceed against them untouched.
        try:
            existing = _read_deployment_id(self._conn)
        except DeploymentIdUnreadableError as exc:
            logger.warning(
                "z4j buffer: %s -- could not read the deployment fingerprint "
                "(%s); leaving its rows and stamp untouched rather than risk "
                "discarding our own undelivered events",
                self._path.name,
                exc,
            )
            return
        # An ABSENT stamp means a buffer written before fingerprints
        # existed, i.e. by an older release of THIS agent. That is not evidence
        # of foreign data -- the older release had no fingerprint to write -- and
        # treating it as such DELETED every undelivered event on upgrade, for
        # every deployment, on the first start of the new version. Migrate it
        # instead: keep the rows and stamp them as ours, which is exactly the
        # ownership the older release already asserted by writing to this path.
        # A stamp that is PRESENT and DIFFERENT is still another deployment's
        # data and is still discarded.
        if existing is None:
            logger.info(
                "z4j buffer: %s carries no deployment fingerprint (written by an "
                "older agent); adopting its %d undelivered event(s) and stamping "
                "them for this deployment",
                self._path.name,
                self._count_rows_best_effort(),
            )
        elif existing != deployment_id:
            # A fingerprint that DIFFERS is not authority to DELETE.
            #
            # It proves only that the stamp does not equal ours, and the stamp is
            # derived from the agent's hmac_secret (see _derive_deployment_id).
            # So ROTATING THAT SECRET -- an ordinary security operation -- makes a
            # deployment's OWN undelivered events read as another deployment's
            # and, until now, silently deleted them. A restored backup, a copied
            # buffer directory, or a reused path produce the same reading.
            #
            # Inequality justifies refusing to EMIT (we cannot prove these events
            # are ours to send under our identity). It does not justify
            # destroying them. So we leave the file exactly as it is and raise,
            # which the caller turns into "open a fresh buffer of our own and
            # keep running". The events stay on disk for an operator.
            raise BufferForeignDataError(
                f"buffer {self._path} carries deployment {existing!r}, not "
                f"{deployment_id!r}; leaving its undelivered events untouched"
            )
        self._conn.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES ('deployment', ?)",
            (deployment_id,),
        )

    def _count_rows_best_effort(self) -> int:
        """Row count for a log line. Never raises: a logging detail must not be
        able to fail an open that is otherwise fine."""
        try:
            (n,) = self._conn.execute("SELECT COUNT(*) FROM entries").fetchone()
        except sqlite3.Error:
            return -1
        return int(n)

    def _open(self, path: Path) -> None:
        """Open (or reopen, after an RH9 relocation) the SQLite connection.

        Sets ``self._conn`` with the store's pragmas + schema ready. Factored so
        __init__ can reopen at a per-pid path when the requested path is held by
        another live process.
        """
        self._conn = sqlite3.connect(
            str(path),
            isolation_level=None,  # autocommit
            check_same_thread=False,
            timeout=5.0,
        )
        for pragma in _PRAGMAS:
            self._conn.execute(pragma)
        self._conn.executescript(_SCHEMA)
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Idempotently add columns absent from an OLDER buffer file.

        ``CREATE TABLE IF NOT EXISTS`` does NOT add a new column to a table
        an earlier version already created, and the SQLite buffer file
        survives process restarts / upgrades. ``content_rejects``
        must exist and default 0 so a pre-1.7 entry (which may carry a high
        ``attempts`` from old transport-failure counting) starts its content-
        drop budget fresh and is not deleted on its first content reject.
        """
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(entries)")}
        if "content_rejects" not in cols:
            self._conn.execute(
                "ALTER TABLE entries ADD COLUMN content_rejects INTEGER NOT NULL DEFAULT 0",
            )

    def _write_lease(self, now: float) -> None:
        """RH8: stamp the liveness heartbeat into ``_meta``. Callers hold
        ``self._lock`` (append / touch_lease) or run single-threaded in
        __init__. Best-effort: a lease write must never break buffering."""
        self._last_lease_ts = now
        with contextlib.suppress(sqlite3.Error):
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES (?, ?)",
                (_LEASE_HEARTBEAT_KEY, str(now)),
            )

    def touch_lease(self) -> None:
        """RH8: refresh this owner's liveness lease so a peer never adopts a
        LIVE-but-idle buffer. Intended to be called periodically by the runtime
        heartbeat loop; a no-op once the buffer is closed."""
        with self._lock:
            if not self._operational_in_current_process():
                return
            self._write_lease(time.time())

    def has_command_seen(self, command_id: str, *, ttl_seconds: float = 300.0) -> bool:
        """READ-ONLY half of the durable dedup.

        The RECORDING half (:meth:`mark_command_seen`) must run only AFTER the
        command actually executed. Recording up-front means a crash between the
        record and the broker enqueue leaves a durable marker that makes the
        RECOVERY delivery look like a duplicate, so it is merely re-acked and
        never runs: the fire is LOST. Splitting check-from-record keeps the
        stated at-least-once guarantee (prefer a duplicate over a loss).

        Fails OPEN (returns False, "not seen") on any storage error or a missing
        table, because the safe direction here is to execute rather than to
        suppress.
        """
        with self._lock:
            if not self._operational_in_current_process():
                return False
            try:
                row = self._conn.execute(
                    "SELECT seen_at FROM _seen_commands WHERE command_id = ?",
                    (command_id,),
                ).fetchone()
            except sqlite3.Error:
                return False
            if row is None:
                return False
            try:
                seen_at = float(row[0])
            except (TypeError, ValueError):
                return False
            return (time.time() - seen_at) <= ttl_seconds

    def mark_command_seen(self, command_id: str, *, ttl_seconds: float = 300.0) -> bool:
        """DURABLE command dedup. Atomically records ``command_id`` in the
        buffer's own SQLite (which survives an agent restart) and returns True if
        it was ALREADY recorded within the TTL -- a duplicate -- else False.

        The dispatcher's in-memory dedup is empty after a restart, so a scheduled
        fire (or any command) re-delivered to a restarted agent would re-execute
        (a duplicated fire). This persists the dedup so a re-delivery across a
        restart is recognised as a duplicate and merely re-acked, not re-run.
        Best-effort: on a storage error it returns False (not-a-duplicate),
        preferring delivery over dedup -- the in-memory cache is still the primary
        guard within a single process lifetime."""
        with self._lock:
            if not self._operational_in_current_process():
                return False
            now = time.time()
            try:
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS _seen_commands ("
                    "command_id TEXT PRIMARY KEY, seen_at REAL NOT NULL)",
                )
                # Index seen_at so the TTL prune (and the cap below) is a
                # range scan, not a full-table scan on a long-lived buffer.
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS ix_seen_commands_seen_at "
                    "ON _seen_commands(seen_at)",
                )
                # Prune expired entries so the table cannot grow unbounded on a
                # long-lived persistent buffer_path.
                self._conn.execute(
                    "DELETE FROM _seen_commands WHERE seen_at < ?",
                    (now - ttl_seconds,),
                )
                row = self._conn.execute(
                    "SELECT 1 FROM _seen_commands WHERE command_id = ?",
                    (command_id,),
                ).fetchone()
                if row is not None:
                    return True
                self._conn.execute(
                    "INSERT OR REPLACE INTO _seen_commands(command_id, seen_at) VALUES (?, ?)",
                    (command_id, now),
                )
                # An ABSOLUTE row cap in addition to the TTL prune, so a burst
                # of more than _DURABLE_DEDUP_MAX distinct commands inside one TTL
                # window still cannot grow the table without bound. Keep the most
                # recent by seen_at.
                #
                # This cap is WEAKER than the TTL guarantee -- when it
                # bites it evicts entries that are still inside their TTL, so a
                # redelivery of an evicted fire executes again (at-least-once,
                # which is what we promise, but it must not be SILENT). Log it so
                # an operator can see the dedup window degrading and raise the cap
                # or shorten the TTL.
                capped = self._conn.execute(
                    "DELETE FROM _seen_commands WHERE command_id IN ("
                    "  SELECT command_id FROM _seen_commands "
                    "  ORDER BY seen_at DESC LIMIT -1 OFFSET ?)",
                    (_DURABLE_DEDUP_MAX,),
                )
                evicted = capped.rowcount or 0
                if evicted > 0:
                    logger.warning(
                        "z4j buffer: durable dedup cap (%d) evicted %d entry(ies) "
                        "that were still within their %.0fs TTL; a redelivery of "
                        "an evicted command can execute again",
                        _DURABLE_DEDUP_MAX,
                        evicted,
                        ttl_seconds,
                    )
                return False
            except sqlite3.Error:
                return False

    def import_seen_commands(self, pairs: list[tuple[str, float]]) -> None:
        """Merge a dead peer's durable dedup set (from adoption) into ours,
        so a fire the peer already executed is not re-run by the adopting agent
        after a pid-change restart. Best-effort; never raises."""
        if not pairs:
            return
        with self._lock:
            if not self._operational_in_current_process():
                return
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS _seen_commands ("
                    "command_id TEXT PRIMARY KEY, seen_at REAL NOT NULL)",
                )
                self._conn.executemany(
                    "INSERT OR IGNORE INTO _seen_commands(command_id, seen_at) VALUES (?, ?)",
                    pairs,
                )

    def _discard_unowned_rows(self, *, reason: str) -> None:
        """RH6: drop rows inherited from a buffer file we cannot prove is ours.

        Called from ``__init__`` when a reused per-pid file carries a foreign
        (or absent) deployment fingerprint. Emitting those events under this
        agent's identity would cross-deliver another deployment's data, so we
        start clean.

        H5: NOT best-effort. A COUNT or DELETE failure PROPAGATES so ``__init__``
        aborts BEFORE the deployment restamp -- otherwise a suppressed DELETE
        failure (SQLITE_FULL, I/O error, page corruption) would leave the foreign
        rows in place while the restamp relabels them ours, and the transport
        would then ship them to our brain under our identity (cross-deployment
        event injection). Fail closed: an agent must not run on a buffer it could
        not clean.
        """
        (count,) = self._conn.execute("SELECT COUNT(*) FROM entries").fetchone()
        if not count:
            return
        self._conn.execute("DELETE FROM entries")
        logger.warning(
            "z4j buffer: %s at %s carried a %s; discarded %d undelivered row(s) "
            "rather than emit them under this deployment's identity",
            "reused per-pid buffer",
            self._path.name,
            reason,
            count,
        )

    @property
    def path(self) -> Path:
        """The on-disk path the buffer is using.

        May differ from the path passed to ``__init__`` if the
        original was unwritable and the resolver relocated the file
        to the per-uid tmp fallback.
        """
        return self._path

    @staticmethod
    def _resolve_writable_path(requested: Path) -> Path:
        """Pick an actually-writable path, falling back if needed.

        Tries the requested parent directory first; if mkdir or write
        fails, relocates the file (preserving its filename) under the
        per-uid tmp fallback root. Raises
        :class:`BufferStorageError` if no fallback works either.

        We deliberately do NOT trust the requested path even when the
        operator set it explicitly: the same env-var override that
        bites correct setups also bites typo'd ones. Falling back is
        always safer than crashing the host process at boot.
        """
        parent = requested.parent
        if is_writable_dir(parent):
            return requested

        # The default location wasn't usable. Relocate the file under
        # the resolved fallback root and keep going.
        try:
            fallback_root = ensure_buffer_root_writable()
        except OSError as exc:
            raise BufferStorageError(
                f"z4j buffer: cannot find any writable directory for "
                f"{requested.name}. Tried {parent} and the per-uid "
                f"tmp fallback. Set Z4J_HOME to a writable "
                f"directory.",
                details={
                    "requested": str(requested),
                    "uid": str(os.getuid()) if hasattr(os, "getuid") else "n/a",
                },
            ) from exc

        if fallback_root == parent:
            # Resolver picked the same dir we just probed - shouldn't
            # happen, but guard against an infinite-decision loop.
            return requested

        relocated = fallback_root / requested.name
        logger.warning(
            "z4j buffer: requested %s but the parent dir is not writable; using %s instead.",
            requested,
            relocated,
        )
        return relocated

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def _operational_in_current_process(self) -> bool:
        """True only for the creating process generation and a sealed sink."""
        if self._creator_pid != os.getpid():
            return False
        if self.deployment_id is not None:
            possession = self._possession
            if (
                possession is None
                or not possession.belongs_to_current_process()
                or possession.generation != self._process_generation
            ):
                return False
        return not self._closed and self._sealed_ready

    @property
    def closed(self) -> bool:
        """True after :meth:`close` has been called."""
        return not self._operational_in_current_process()

    def append(self, kind: str, payload: bytes) -> int:
        """Append one entry, return its id.

        If the append would take the buffer over its
        ``max_entries`` or ``max_bytes`` limit, the oldest entries
        are dropped to make room (see :meth:`_evict_if_needed`).

        Args:
            kind: Frame type tag.
            payload: Serialized frame bytes.

        Returns:
            The auto-assigned integer primary key of the new entry.
        """
        now = time.time()
        with self._lock:
            # Re-check inside the lock - close() acquires the same
            # lock, so this is the only race-free check.
            if not self._operational_in_current_process():
                raise RuntimeError(
                    "BufferStore is closed, unsealed, or belongs to another process generation"
                )
            self._evict_if_needed_locked(incoming_bytes=len(payload))
            cursor = self._conn.execute(
                "INSERT INTO entries (kind, payload, created_at, attempts) VALUES (?, ?, ?, 0)",
                (kind, payload, now),
            )
            new_id = cursor.lastrowid
            if new_id is None:
                raise RuntimeError("sqlite lastrowid unavailable")
            self._cached_count += 1
            self._cached_bytes += len(payload)
            # RH8: refresh the liveness lease at most every _LEASE_REFRESH_SECONDS
            # so an actively-writing owner is never mistaken for a dead one, at
            # the cost of one _meta write per interval (not per append).
            if now - self._last_lease_ts >= _LEASE_REFRESH_SECONDS:
                self._write_lease(now)
            return new_id

    def append_external_schedule_projection(
        self,
        *,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        build_payload: Callable[[int, str], bytes],
    ) -> tuple[int, int, str]:
        """Atomically reserve one source sequence and append its exact frame."""

        entry_ids, sequence, adapter_id = self.append_external_schedule_projection_frames(
            owner=owner,
            source_scope=source_scope,
            stream_id=stream_id,
            epoch_uuid=epoch_uuid,
            epoch_number=epoch_number,
            adapter_instance_id=adapter_instance_id,
            build_payloads=lambda reserved_sequence, reserved_adapter: [
                build_payload(reserved_sequence, reserved_adapter),
            ],
        )
        return entry_ids[0], sequence, adapter_id

    def append_external_schedule_projection_frames(
        self,
        *,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        build_payloads: Callable[[int, str], list[bytes]],
    ) -> tuple[list[int], int, str]:
        """Reserve one source sequence and atomically append all of its frames.

        The sequence is durable only as part of the same SQLite transaction as
        every serialized frame.  A crash, framing failure, or capacity failure
        therefore leaves both the authority counter and outbound queue
        unchanged instead of exposing a partial snapshot or burning a sequence.

        An authority row belongs to the process generation that first accepted
        the Brain-issued epoch.  Reopening the same file in another process
        generation cannot silently continue that epoch: the Brain must retire
        or replace it after proving the old executor stopped.
        """
        if (
            not owner
            or not source_scope
            or not stream_id
            or not epoch_uuid
            or epoch_number <= 0
            or not adapter_instance_id
        ):
            raise ValueError("external schedule authority fields are invalid")

        now = time.time()
        with self._lock:
            if not self._operational_in_current_process():
                raise RuntimeError(
                    "BufferStore is closed, unsealed, or belongs to another process generation"
                )

            cached_count = self._cached_count
            cached_bytes = self._cached_bytes
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._append_external_schedule_projection_frames_locked(
                    owner=owner,
                    source_scope=source_scope,
                    stream_id=stream_id,
                    epoch_uuid=epoch_uuid,
                    epoch_number=epoch_number,
                    adapter_instance_id=adapter_instance_id,
                    build_payloads=build_payloads,
                    now=now,
                )
                self._conn.execute("COMMIT")
                return result
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                self._cached_count = cached_count
                self._cached_bytes = cached_bytes
                raise

    def reserve_external_schedule_control(
        self,
        *,
        operation_id: str,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        expected_sequence: int,
        desired_projection_digest: str,
    ) -> ExternalScheduleControlReservation:
        """Reserve the exact next sequence before an external side effect.

        While the reservation is open, ordinary observations for this scope
        cannot allocate a later sequence. Replaying the same operation returns
        the same reservation; every divergent identity fails closed.
        """

        if (
            not operation_id
            or not owner
            or not source_scope
            or not stream_id
            or not epoch_uuid
            or epoch_number <= 0
            or not adapter_instance_id
            or expected_sequence < 0
            or len(desired_projection_digest) != 64
            or any(character not in "0123456789abcdef" for character in desired_projection_digest)
        ):
            raise ValueError("external schedule control reservation is invalid")

        now = time.time()
        with self._lock:
            if not self._operational_in_current_process():
                raise RuntimeError(
                    "BufferStore is closed, unsealed, or belongs to another process generation"
                )
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._reserve_external_schedule_control_locked(
                    operation_id=operation_id,
                    owner=owner,
                    source_scope=source_scope,
                    stream_id=stream_id,
                    epoch_uuid=epoch_uuid,
                    epoch_number=epoch_number,
                    adapter_instance_id=adapter_instance_id,
                    expected_sequence=expected_sequence,
                    desired_projection_digest=desired_projection_digest,
                    now=now,
                )
                self._conn.execute("COMMIT")
                return result
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                raise

    def _reserve_external_schedule_control_locked(
        self,
        *,
        operation_id: str,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        expected_sequence: int,
        desired_projection_digest: str,
        now: float,
    ) -> ExternalScheduleControlReservation:
        authority = self._conn.execute(
            "SELECT stream_id, epoch_uuid, epoch_number, "
            "adapter_instance_id, process_generation, last_sequence "
            "FROM _external_schedule_authority "
            "WHERE owner = ? AND source_scope = ?",
            (owner, source_scope),
        ).fetchone()
        if authority is None:
            raise ExternalScheduleAuthorityError("external control has no durable stream authority")
        if (
            str(authority[0]) != stream_id
            or str(authority[1]) != epoch_uuid
            or int(authority[2]) != epoch_number
            or str(authority[3]) != adapter_instance_id
            or str(authority[4]) != self._process_generation
        ):
            raise ExternalScheduleAuthorityError("external control authority is stale")
        existing = self._conn.execute(
            "SELECT owner, source_scope, stream_id, epoch_uuid, "
            "epoch_number, adapter_instance_id, process_generation, "
            "expected_sequence, reserved_sequence, "
            "desired_projection_digest, status "
            "FROM _external_schedule_control_reservations "
            "WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        reserved_sequence = expected_sequence + 1
        if existing is not None:
            expected_identity = (
                owner,
                source_scope,
                stream_id,
                epoch_uuid,
                epoch_number,
                adapter_instance_id,
                self._process_generation,
                expected_sequence,
                reserved_sequence,
                desired_projection_digest,
            )
            if tuple(existing[:10]) != expected_identity:
                raise ExternalScheduleAuthorityError("external control operation identity diverged")
            status = str(existing[10])
            required_last_sequence = (
                reserved_sequence if status == "PUBLISHED" else expected_sequence
            )
            if int(authority[5]) != required_last_sequence:
                raise ExternalScheduleAuthorityError("external control durable sequence diverged")
            return ExternalScheduleControlReservation(
                sequence=reserved_sequence,
                already_published=status == "PUBLISHED",
            )
        pending = self._conn.execute(
            "SELECT operation_id "
            "FROM _external_schedule_control_reservations "
            "WHERE owner = ? AND source_scope = ? "
            "AND status = 'RESERVED'",
            (owner, source_scope),
        ).fetchone()
        if pending is not None:
            raise ExternalScheduleAuthorityError(
                "another external control already reserves this stream"
            )
        if int(authority[5]) != expected_sequence:
            raise ExternalScheduleAuthorityError("external control expected sequence is stale")
        self._conn.execute(
            "INSERT INTO _external_schedule_control_reservations("
            "operation_id, owner, source_scope, stream_id, epoch_uuid, "
            "epoch_number, adapter_instance_id, process_generation, "
            "expected_sequence, reserved_sequence, "
            "desired_projection_digest, status, entry_id, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "'RESERVED', NULL, ?, ?)",
            (
                operation_id,
                owner,
                source_scope,
                stream_id,
                epoch_uuid,
                epoch_number,
                adapter_instance_id,
                self._process_generation,
                expected_sequence,
                reserved_sequence,
                desired_projection_digest,
                now,
                now,
            ),
        )
        return ExternalScheduleControlReservation(
            sequence=reserved_sequence,
            already_published=False,
        )

    def append_reserved_external_schedule_control(
        self,
        *,
        operation_id: str,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        expected_sequence: int,
        desired_projection_digest: str,
        build_payload: Callable[[int, str], bytes],
    ) -> tuple[int, int, str, bool]:
        """Publish one reserved control projection and advance atomically."""

        now = time.time()
        with self._lock:
            if not self._operational_in_current_process():
                raise RuntimeError(
                    "BufferStore is closed, unsealed, or belongs to another process generation"
                )
            cached_count = self._cached_count
            cached_bytes = self._cached_bytes
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._append_reserved_external_schedule_control_locked(
                    operation_id=operation_id,
                    owner=owner,
                    source_scope=source_scope,
                    stream_id=stream_id,
                    epoch_uuid=epoch_uuid,
                    epoch_number=epoch_number,
                    adapter_instance_id=adapter_instance_id,
                    expected_sequence=expected_sequence,
                    desired_projection_digest=desired_projection_digest,
                    build_payload=build_payload,
                    now=now,
                )
                self._conn.execute("COMMIT")
                return result
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                self._cached_count = cached_count
                self._cached_bytes = cached_bytes
                raise

    def _append_reserved_external_schedule_control_locked(
        self,
        *,
        operation_id: str,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        expected_sequence: int,
        desired_projection_digest: str,
        build_payload: Callable[[int, str], bytes],
        now: float,
    ) -> tuple[int, int, str, bool]:
        reservation = self._conn.execute(
            "SELECT owner, source_scope, stream_id, epoch_uuid, "
            "epoch_number, adapter_instance_id, process_generation, "
            "expected_sequence, reserved_sequence, "
            "desired_projection_digest, status, entry_id "
            "FROM _external_schedule_control_reservations "
            "WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if reservation is None:
            raise ExternalScheduleAuthorityError("external control sequence was not reserved")
        sequence = expected_sequence + 1
        expected_identity = (
            owner,
            source_scope,
            stream_id,
            epoch_uuid,
            epoch_number,
            adapter_instance_id,
            self._process_generation,
            expected_sequence,
            sequence,
            desired_projection_digest,
        )
        if tuple(reservation[:10]) != expected_identity:
            raise ExternalScheduleAuthorityError("external control reservation identity diverged")
        if str(reservation[10]) == "PUBLISHED":
            entry_id = reservation[11]
            if entry_id is None:
                raise ExternalScheduleAuthorityError(
                    "published external control lacks its buffer entry"
                )
            return int(entry_id), sequence, adapter_instance_id, True
        authority = self._conn.execute(
            "SELECT stream_id, epoch_uuid, epoch_number, "
            "adapter_instance_id, process_generation, last_sequence "
            "FROM _external_schedule_authority "
            "WHERE owner = ? AND source_scope = ?",
            (owner, source_scope),
        ).fetchone()
        if authority is None or (
            str(authority[0]),
            str(authority[1]),
            int(authority[2]),
            str(authority[3]),
            str(authority[4]),
            int(authority[5]),
        ) != (
            stream_id,
            epoch_uuid,
            epoch_number,
            adapter_instance_id,
            self._process_generation,
            expected_sequence,
        ):
            raise ExternalScheduleAuthorityError(
                "external control lost its durable stream authority"
            )
        payload = build_payload(sequence, adapter_instance_id)
        if not isinstance(payload, bytes):
            raise TypeError("external control payload builder must return bytes")
        if len(payload) > self._max_bytes:
            raise BufferStorageError("external control projection exceeds the buffer limit")
        self._evict_if_needed_locked(incoming_bytes=len(payload))
        cursor = self._conn.execute(
            "INSERT INTO entries (kind, payload, created_at, attempts) VALUES (?, ?, ?, 0)",
            (EXTERNAL_SCHEDULE_ENTRY_KIND, payload, now),
        )
        entry_id = cursor.lastrowid
        if entry_id is None:
            raise RuntimeError("sqlite lastrowid unavailable")
        authority_update = self._conn.execute(
            "UPDATE _external_schedule_authority "
            "SET last_sequence = ?, updated_at = ? "
            "WHERE owner = ? AND source_scope = ? "
            "AND process_generation = ? AND last_sequence = ?",
            (
                sequence,
                now,
                owner,
                source_scope,
                self._process_generation,
                expected_sequence,
            ),
        )
        if (authority_update.rowcount or 0) != 1:
            raise ExternalScheduleAuthorityError("external control sequence advance lost authority")
        reservation_update = self._conn.execute(
            "UPDATE _external_schedule_control_reservations "
            "SET status = 'PUBLISHED', entry_id = ?, updated_at = ? "
            "WHERE operation_id = ? AND status = 'RESERVED'",
            (entry_id, now, operation_id),
        )
        if (reservation_update.rowcount or 0) != 1:
            raise ExternalScheduleAuthorityError("external control reservation was not consumed")
        self._cached_count += 1
        self._cached_bytes += len(payload)
        if now - self._last_lease_ts >= _LEASE_REFRESH_SECONDS:
            self._write_lease(now)
        return int(entry_id), sequence, adapter_instance_id, False

    def _append_external_schedule_projection_frames_locked(
        self,
        *,
        owner: str,
        source_scope: str,
        stream_id: str,
        epoch_uuid: str,
        epoch_number: int,
        adapter_instance_id: str,
        build_payloads: Callable[[int, str], list[bytes]],
        now: float,
    ) -> tuple[list[int], int, str]:
        """Perform a framed reservation while lock + transaction are held."""
        row = self._conn.execute(
            "SELECT stream_id, epoch_uuid, epoch_number, "
            "adapter_instance_id, process_generation, last_sequence "
            "FROM _external_schedule_authority "
            "WHERE owner = ? AND source_scope = ?",
            (owner, source_scope),
        ).fetchone()
        if row is None:
            last_sequence = 0
            self._conn.execute(
                "INSERT INTO _external_schedule_authority("
                "owner, source_scope, stream_id, epoch_uuid, "
                "epoch_number, adapter_instance_id, process_generation, "
                "last_sequence, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (
                    owner,
                    source_scope,
                    stream_id,
                    epoch_uuid,
                    epoch_number,
                    adapter_instance_id,
                    self._process_generation,
                    now,
                ),
            )
        else:
            (
                stored_stream_id,
                stored_epoch_uuid,
                stored_epoch_number,
                stored_adapter_instance_id,
                process_generation,
                last_sequence,
            ) = row
            if (
                str(stored_stream_id) != stream_id
                or str(stored_epoch_uuid) != epoch_uuid
                or int(stored_epoch_number) != epoch_number
                or str(stored_adapter_instance_id) != adapter_instance_id
            ):
                raise ExternalScheduleAuthorityError(
                    "another external stream epoch already owns this source scope"
                )
            if str(process_generation) != self._process_generation:
                raise ExternalScheduleAuthorityError(
                    "external stream epoch belongs to an earlier process generation"
                )

        pending_control = self._conn.execute(
            "SELECT operation_id "
            "FROM _external_schedule_control_reservations "
            "WHERE owner = ? AND source_scope = ? "
            "AND status = 'RESERVED'",
            (owner, source_scope),
        ).fetchone()
        if pending_control is not None:
            raise ExternalScheduleAuthorityError(
                "external control reservation blocks later observations"
            )

        sequence = int(last_sequence) + 1
        if sequence > 9_223_372_036_854_775_807:
            raise OverflowError("external schedule sequence is exhausted")
        payloads = build_payloads(sequence, str(adapter_instance_id))
        if (
            not isinstance(payloads, list)
            or not payloads
            or not all(isinstance(payload, bytes) for payload in payloads)
        ):
            raise TypeError(
                "external schedule payload builder must return a non-empty list of bytes"
            )
        incoming_bytes = sum(len(payload) for payload in payloads)
        if incoming_bytes > self._max_bytes or len(payloads) > self._max_entries:
            raise BufferStorageError(
                "external schedule projection frame set exceeds the buffer limits"
            )

        self._evict_if_needed_locked(
            incoming_bytes=incoming_bytes,
            incoming_entries=len(payloads),
        )
        entry_ids: list[int] = []
        for payload in payloads:
            cursor = self._conn.execute(
                "INSERT INTO entries (kind, payload, created_at, attempts) VALUES (?, ?, ?, 0)",
                (EXTERNAL_SCHEDULE_ENTRY_KIND, payload, now),
            )
            entry_id = cursor.lastrowid
            if entry_id is None:
                raise RuntimeError("sqlite lastrowid unavailable")
            entry_ids.append(int(entry_id))
        update_result = self._conn.execute(
            "UPDATE _external_schedule_authority "
            "SET last_sequence = ?, updated_at = ? "
            "WHERE owner = ? AND source_scope = ? "
            "AND process_generation = ? AND last_sequence = ?",
            (
                sequence,
                now,
                owner,
                source_scope,
                self._process_generation,
                sequence - 1,
            ),
        )
        if (update_result.rowcount or 0) != 1:
            raise ExternalScheduleAuthorityError(
                "external schedule sequence reservation lost authority"
            )
        self._cached_count += len(payloads)
        self._cached_bytes += incoming_bytes
        if now - self._last_lease_ts >= _LEASE_REFRESH_SECONDS:
            self._write_lease(now)
        return entry_ids, sequence, str(adapter_instance_id)

    def _append_recovered(self, kind: str, payload: bytes) -> bool:
        """Append recovery data without evicting live entries.

        ``False`` is backpressure: the source row remains where it is and a
        later bounded scan retries after the active sink drains.
        """
        now = time.time()
        with self._lock:
            if not self._operational_in_current_process():
                return False
            if self._cached_count >= self._max_entries:
                return False
            if self._cached_bytes + len(payload) > self._max_bytes:
                return False
            cursor = self._conn.execute(
                "INSERT INTO entries (kind, payload, created_at, attempts) VALUES (?, ?, ?, 0)",
                (kind, payload, now),
            )
            if cursor.lastrowid is None:
                return False
            self._cached_count += 1
            self._cached_bytes += len(payload)
            return True

    def _record_recovery_required(
        self,
        *,
        conn: sqlite3.Connection | None,
        path: Path,
        lock_fd: int,
        attribution: str,
        lifecycle: str,
        reason: str,
    ) -> None:
        """Persist an advisory record keyed by durable or observed identity.

        Classification can fail before a SQLite connection exists, and damaged
        metadata can make the durable UUID unreadable.  Exclusive possession of
        the file still gives us a stable device/inode identity, so those paths
        must remain operator-visible rather than disappearing without an
        advisory.
        """
        try:
            fst = os.fstat(lock_fd)
        except OSError:
            return
        source_uuid: str | None = None
        if conn is not None:
            with contextlib.suppress(sqlite3.Error, BufferMetadataUnreadableError):
                source_uuid = _read_buffer_uuid(conn)
        if source_uuid is None:
            # Pre-1.8 buffers and unreadable current buffers have no usable
            # durable UUID. They are still important recovery work, so use
            # their observed identity as an explicit unidentified key. The
            # record remains advisory and is revalidated against this same
            # identity before any later recovery attempt.
            source_uuid = f"legacy-unidentified:{fst.st_dev}:{fst.st_ino}"
        with self._lock:
            if not self._operational_in_current_process():
                return
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute(
                    "INSERT OR REPLACE INTO _recovery_required("
                    "buffer_uuid, path, observed_dev, observed_ino, attribution, "
                    "lifecycle, row_count, reason, observed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                    (
                        source_uuid,
                        str(path),
                        int(fst.st_dev),
                        int(fst.st_ino),
                        attribution,
                        lifecycle,
                        reason,
                        time.time(),
                    ),
                )

    def _import_recovery_required(self, conn: sqlite3.Connection) -> None:
        """Carry advisory records forward when an old active sink is recovered."""
        try:
            rows = conn.execute(
                "SELECT buffer_uuid, path, observed_dev, observed_ino, "
                "attribution, lifecycle, row_count, reason, observed_at "
                "FROM _recovery_required"
            ).fetchall()
        except sqlite3.Error:
            return
        if not rows:
            return
        with self._lock:
            if not self._operational_in_current_process():
                return
            with contextlib.suppress(sqlite3.Error):
                self._conn.executemany(
                    "INSERT OR REPLACE INTO _recovery_required("
                    "buffer_uuid, path, observed_dev, observed_ino, attribution, "
                    "lifecycle, row_count, reason, observed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )

    def _clear_recovery_required(self, buffer_uuid: str | None) -> None:
        """Drop an advisory after that exact source has drained successfully."""
        if buffer_uuid is None:
            return
        with self._lock:
            if not self._operational_in_current_process():
                return
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute(
                    "DELETE FROM _recovery_required WHERE buffer_uuid = ?",
                    (buffer_uuid,),
                )

    def recovery_required(self) -> list[dict[str, object]]:
        """Return advisory recovery records for operator diagnostics."""
        with self._lock:
            if not self._operational_in_current_process():
                return []
            try:
                rows = self._conn.execute(
                    "SELECT buffer_uuid, path, observed_dev, observed_ino, "
                    "attribution, lifecycle, row_count, reason, observed_at "
                    "FROM _recovery_required ORDER BY observed_at ASC"
                ).fetchall()
            except sqlite3.Error:
                return []
        keys = (
            "buffer_uuid",
            "path",
            "observed_dev",
            "observed_ino",
            "attribution",
            "lifecycle",
            "row_count",
            "reason",
            "observed_at",
        )
        return [dict(zip(keys, row, strict=True)) for row in rows]

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def drain(
        self,
        limit: int,
        *,
        exclude_ids: set[int] | None = None,
        exclude_kinds: set[str] | None = None,
    ) -> list[BufferEntry]:
        """Return the oldest ``limit`` entries without removing them.

        ``exclude_kinds`` skips entries of the given kinds. The send loop
        passes ``exclude_kinds={"event_batch"}`` when it is at the
        in-flight cap, so it keeps draining control frames (command
        acks/results, which confirm on send and never become pending)
        without taking on more un-acked event batches.

        Entries are ordered by ``id`` (which corresponds to insertion
        order). The caller is expected to confirm successful delivery
        via :meth:`confirm` - if confirm is never called, the entries
        remain available for a subsequent drain.

        ``exclude_ids`` skips entries currently IN FLIGHT (sent but not
        yet acked). Without it the send loop re-drains and re-sends the
        same un-acked entries on every iteration (they are never removed
        until acked), which both floods the brain and starves any entry
        beyond the drain window when in-flight entries fill it.

        The exclusion is applied by OVER-FETCHING ``limit + |exclude|``
        oldest rows and filtering in Python, NOT with a SQL
        ``NOT IN (...)``. A parameterized ``NOT IN`` would bind one
        variable per excluded id, and a large in-flight set (the buffer
        permits up to 100k entries) can exceed SQLite's 32766
        bound-parameter ceiling and raise ``OperationalError``, which
        would crash the send loop and (after the supervisor reconnect
        clears pending state) restore the very re-send storm this
        exclusion prevents. Over-fetching at most ``limit +
        |exclude|`` rows guarantees ``limit`` non-excluded rows whenever
        that many exist, at bounded cost (the runtime also caps the
        in-flight set, so ``|exclude|`` stays small in practice).

        Args:
            limit: Maximum number of entries to return. Must be > 0.
            exclude_ids: Buffer entry ids to skip (currently in flight).
        """
        if limit <= 0:
            raise ValueError("drain limit must be positive")
        with self._lock:
            # Re-check ``_closed`` INSIDE the lock. ``close()`` takes the
            # same lock to flip ``_closed`` and close the connection, so a
            # check OUTSIDE the lock has a TOCTOU window: a concurrent
            # ``stop()`` on another thread (the atexit teardown path of a
            # short-lived process exiting while the send loop is still
            # draining) can close the connection between the check and the
            # ``execute`` below, raising ``sqlite3.ProgrammingError:
            # Cannot operate on a closed database``. Mirrors the in-lock
            # guard already in ``confirm`` / ``size`` / ``byte_size``.
            if not self._operational_in_current_process():
                return []
            # ``exclude_kinds`` is applied at the SQL level (a small,
            # fixed set of kind strings, well within the parameter
            # limit) so the ``LIMIT`` counts rows of the WANTED kinds
            # even when many excluded-kind rows sit ahead of them.
            # ``exclude_ids`` (potentially large: the in-flight set) is
            # applied in Python to avoid SQLite's 32766 bound-parameter
            # ceiling; we over-fetch ``limit + |exclude_ids|`` rows so
            # ``limit`` survive the Python filter.
            fetch = limit + (len(exclude_ids) if exclude_ids else 0)
            if exclude_kinds:
                kind_ph = ",".join("?" * len(exclude_kinds))
                rows = self._conn.execute(
                    f"SELECT id, kind, payload, created_at, attempts FROM entries WHERE kind NOT IN ({kind_ph}) ORDER BY id ASC LIMIT ?",  # noqa: S608  kind_ph is bound '?' params
                    (*exclude_kinds, fetch),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id, kind, payload, created_at, attempts "
                    "FROM entries ORDER BY id ASC LIMIT ?",
                    (fetch,),
                ).fetchall()
        out: list[BufferEntry] = []
        for r in rows:
            if exclude_ids and r[0] in exclude_ids:
                continue
            out.append(
                BufferEntry(id=r[0], kind=r[1], payload=r[2], created_at=r[3], attempts=r[4]),
            )
            if len(out) >= limit:
                break
        return out

    def confirm(self, ids: list[int]) -> None:
        """Delete entries by id after the brain has accepted them.

        Called by the transport once a batch has been successfully
        flushed. No-op on an empty list.
        """
        if not ids:
            return
        with self._lock:
            if not self._operational_in_current_process():
                return
            placeholders = ",".join("?" * len(ids))
            # Reduce cached counters by the size of what we're about to
            # delete. Done before DELETE so we don't have to scan twice.
            row = self._conn.execute(
                f"SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) "  # noqa: S608  placeholders are bound '?' params, ids not user input
                f"FROM entries WHERE id IN ({placeholders})",
                ids,
            ).fetchone()
            removed_count, removed_bytes = int(row[0]), int(row[1])
            self._conn.execute(
                f"DELETE FROM entries WHERE id IN ({placeholders})",  # noqa: S608  placeholders are bound '?' params, ids not user input
                ids,
            )
            self._cached_count -= removed_count
            self._cached_bytes -= removed_bytes

    def increment_attempts(self, ids: list[int]) -> None:
        """Increment the ``attempts`` METRIC counter for a batch of entries.

        Called by the WS ack-watchdog when a re-send is due. Used ONLY to
        surface stuck entries in metrics; it does NOT gate any drop (the
        content-drop budget is the separate ``content_rejects`` column,
        ). Keeping this off the drop path is what makes WS-timeout (or
        pre-1.7 cross-version) history harmless.
        """
        if not ids:
            return
        with self._lock:
            if not self._operational_in_current_process():
                return
            placeholders = ",".join("?" * len(ids))
            self._conn.execute(
                f"UPDATE entries SET attempts = attempts + 1 WHERE id IN ({placeholders})",  # noqa: S608  placeholders are bound '?' params, ids not user input
                ids,
            )

    def increment_content_rejects(self, ids: list[int]) -> None:
        """Increment the ``content_rejects`` drop-budget counter.

        Called ONLY by the long-poll per-frame content-reject path
        (413/415/422) once a single frame has been isolated. This is the
        dedicated bounded budget consulted by :meth:`evict_if_exhausted`;
        it is physically separate from the ``attempts`` metric so transient
        WS-timeout history can never destructively prime a drop.
        """
        if not ids:
            return
        with self._lock:
            if not self._operational_in_current_process():
                return
            placeholders = ",".join("?" * len(ids))
            self._conn.execute(
                f"UPDATE entries SET content_rejects = content_rejects + 1 WHERE id IN ({placeholders})",  # noqa: S608  placeholders are bound '?' params, ids not user input
                ids,
            )

    def evict_if_exhausted(self, ids: list[int], max_rejects: int) -> int:
        """Drop ONLY the given ids, and only if their ``content_rejects``
        reached ``max_rejects``.

        The bounded-retry backstop, ID-TARGETED: the caller passes the
        EXACT buffer entry ids the brain is persistently rejecting on
        their content (a single frame isolated by batch-size reduction).
        Only those ids are considered, and only the ones whose dedicated
        ``content_rejects`` budget is at/over the cap are dropped -- so a
        request-level rejection can never mass-delete valid siblings
        and transient ``attempts`` history never triggers a drop
        Returns the number dropped. Logs a WARNING per drop.
        """
        if not ids or max_rejects <= 0:
            return 0
        with self._lock:
            if not self._operational_in_current_process():
                return 0
            placeholders = ",".join("?" * len(ids))
            row = self._conn.execute(
                f"SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) FROM entries WHERE content_rejects >= ? AND kind <> ? AND id IN ({placeholders})",  # noqa: S608  bound '?' params
                (max_rejects, EXTERNAL_SCHEDULE_ENTRY_KIND, *ids),
            ).fetchone()
            dropped_count, dropped_bytes = int(row[0]), int(row[1])
            if dropped_count == 0:
                return 0
            self._conn.execute(
                f"DELETE FROM entries WHERE content_rejects >= ? AND kind <> ? AND id IN ({placeholders})",  # noqa: S608  bound '?' params
                (max_rejects, EXTERNAL_SCHEDULE_ENTRY_KIND, *ids),
            )
            self._cached_count -= dropped_count
            self._cached_bytes -= dropped_bytes
        logger.warning(
            "z4j agent buffer dropped %d entr%s after %d content "
            "rejections (brain kept rejecting this specific frame's "
            "content); events dropped",
            dropped_count,
            "y" if dropped_count == 1 else "ies",
            max_rejects,
        )
        return dropped_count

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def size(self) -> int:
        """Number of entries currently in the buffer.

        Always ``>= 0``. If the cached counter has drifted below
        zero we reconcile from disk, log once, and return the
        reconciled value. The heartbeat frame's ``buffer_size``
        field is Pydantic-validated ``ge=0``; without this clamp a
        drifted counter would crash the heartbeat loop and force
        agent reconnect churn.
        """
        with self._lock:
            if not self._operational_in_current_process():
                return 0
            if self._cached_count < 0:
                self._reconcile_counters_locked()
            return max(0, int(self._cached_count))

    def byte_size(self) -> int:
        """Approximate total payload size in bytes.

        Always ``>= 0``; see :meth:`size` for the drift-detection
        rationale.
        """
        with self._lock:
            if not self._operational_in_current_process():
                return 0
            if self._cached_bytes < 0:
                self._reconcile_counters_locked()
            return max(0, int(self._cached_bytes))

    def _reconcile_counters_locked(self) -> None:
        """Re-read ``_cached_count`` / ``_cached_bytes`` from disk.

        Called when one of the counters has been observed to go
        negative. Self-healing - the heartbeat loop recovers -
        without hiding the underlying bug, because we log a
        ``WARNING`` the first time we detect drift in this
        BufferStore's lifetime. Operators who see this warning
        should open an issue; the downstream event / task flow
        is unaffected.

        Must be called with :attr:`_lock` already held.
        """
        stale_count = self._cached_count
        stale_bytes = self._cached_bytes
        (self._cached_count,) = self._conn.execute(
            "SELECT COUNT(*) FROM entries",
        ).fetchone()
        (self._cached_bytes,) = self._conn.execute(
            "SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM entries",
        ).fetchone()
        if not self._drift_warned:
            self._drift_warned = True
            logger.warning(
                "z4j agent buffer: cached counters drifted negative "
                "(count=%d, bytes=%d); reconciled from disk to "
                "(count=%d, bytes=%d). This is a real bug - please "
                "report it at https://github.com/z4jdev/z4j/issues.",
                stale_count,
                stale_bytes,
                self._cached_count,
                self._cached_bytes,
            )

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def _evict_if_needed_locked(
        self,
        *,
        incoming_bytes: int,
        incoming_entries: int = 1,
    ) -> None:
        """Drop oldest entries until we fit the limits.

        Called while the connection lock is already held. Uses cached
        running totals (``_cached_count``, ``_cached_bytes``) so we
        never run a SUM() per iteration.
        """
        dropped = 0

        # Entry count limit - leave room for the incoming row.
        while self._cached_count + incoming_entries > self._max_entries:
            if not self._drop_oldest_locked():
                break
            dropped += 1

        # Byte-size limit (including the incoming entry's own size).
        while self._cached_bytes + incoming_bytes > self._max_bytes:
            if not self._drop_oldest_locked():
                break
            dropped += 1

        if (
            self._cached_count + incoming_entries > self._max_entries
            or self._cached_bytes + incoming_bytes > self._max_bytes
        ):
            raise BufferStorageError(
                "z4j buffer is full of causally protected external schedule projections"
            )

        if dropped:
            logger.warning(
                "z4j agent buffer evicted %d oldest entr%s to fit limits",
                dropped,
                "y" if dropped == 1 else "ies",
            )

    def _drop_oldest_locked(self) -> bool:
        """Delete the single oldest entry. Returns True if one was dropped.

        Reads the payload length first so we can keep ``_cached_bytes``
        in lockstep with the underlying table without re-running SUM().
        """
        row = self._conn.execute(
            "SELECT id, LENGTH(payload) FROM entries WHERE kind <> ? ORDER BY id ASC LIMIT 1",
            (EXTERNAL_SCHEDULE_ENTRY_KIND,),
        ).fetchone()
        if row is None:
            return False
        oldest_id, payload_len = int(row[0]), int(row[1])
        cursor = self._conn.execute(
            "DELETE FROM entries WHERE id = ?",
            (oldest_id,),
        )
        if cursor.rowcount > 0:
            self._cached_count -= 1
            self._cached_bytes -= payload_len
            return True
        return False

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def close(self, *, lock_timeout: float | None = None) -> None:
        """Close the underlying SQLite connection.

        Idempotent. After close, :meth:`append` raises and all read
        methods return empty results.

        Orphan cleanup: if the buffer is empty when we close (the
        common case after a clean drain), the SQLite files are
        removed from disk. Combined with the per-process buffer-path
        default added in z4j-core 1.0.3 (``buffer-{pid}.sqlite``),
        this prevents accumulation of stale ``buffer-{old-pid}.sqlite``
        files across many restarts. If the buffer is non-empty
        (un-drained events from a transport outage), the file is
        preserved so a future BufferStore at the same path could
        pick it up.

        RM6: ``lock_timeout`` bounds how long close() waits for the store
        lock. The periodic orphan scan runs on a DAEMON thread (M5) and holds
        this lock while appending; if that append wedges on slow SQLite I/O, an
        unbounded close() called from stop() would block indefinitely -- the
        daemon thread keeps process EXIT bounded but not stop(timeout). On a
        timeout we ABANDON the handle (mark closed so append() fails, skip the
        empty-file unlink, leave the sqlite connection to the daemon thread / OS
        to reclaim at process exit) rather than block. ``None`` waits
        indefinitely (the original behavior; used where no contention is
        possible, e.g. _abort_start).
        """
        if self._creator_pid != os.getpid():
            self.release_fork_inherited_lock()
            self._closed = True
            return

        acquired = self._lock.acquire(
            timeout=lock_timeout if lock_timeout is not None else -1,
        )
        if not acquired:
            # RM6: the lock is held (a wedged daemon append). Do not block
            # stop(). Mark closed so future append() calls fail; leave the sqlite
            # handle + lock fd to the daemon thread / OS to reclaim at process
            # exit, and skip the empty-file unlink (we cannot safely read the
            # count without the lock).
            logger.warning(
                "z4j buffer: close() could not acquire the store lock within "
                "%.3fs; abandoning the handle (a background scan may be wedged "
                "on I/O). The OS reclaims it at process exit.",
                lock_timeout,
            )
            # We MUST NOT release the ownership flock here. The wedged
            # daemon append that holds ``self._lock`` may already be PAST its
            # ``self._closed`` check and blocked inside SQLite; if we dropped the
            # flock now, another deployment could claim/restamp this exact inode
            # and the resuming append would write into the NEW owner's buffer
            # (cross-owner corruption). Keeping the lifetime flock held until the
            # process exits (the OS releases it on exit) is the whole point of the
            # lock: no other process can take the inode while we are alive. This
            # intentionally re-accepts the bounded "leak" (the fd lives to
            # process exit) as strictly better than the corruption above -- and a
            # same-process re-adoption of this exact inode does not occur on the
            # abandon-on-close path (reinit_after_fork uses a per-pid path).
            self._closed = True
            return
        try:
            if self._closed:
                return
            # Ask SQLite for the current row count BEFORE closing the
            # connection. We can't query a closed connection, and we
            # don't want to invoke .size() (which acquires the same
            # lock and we're already inside it).
            try:
                (count,) = self._conn.execute(
                    "SELECT COUNT(*) FROM entries",
                ).fetchone()
            except sqlite3.Error:
                # If the COUNT fails for any reason, err on the side
                # of preserving the file - we don't want to delete a
                # buffer that might still hold un-drained events just
                # because the count query glitched.
                count = -1
            self._closed = True
            # Clear OUR owner token (only if it is still ours) BEFORE
            # closing, so a later sequential reopen sees no live owner and does
            # not needlessly relocate off its own rows. Guarded to our token so
            # we never clear a peer that took over. Best-effort.
            with contextlib.suppress(sqlite3.Error):
                self._conn.execute(
                    "DELETE FROM _meta WHERE key = ? AND value = ?",
                    (_OWNER_TOKEN_KEY, self._owner_token),
                )
            with contextlib.suppress(sqlite3.Error):  # pragma: no cover
                self._conn.close()
            # Cleanup happens while possession is STILL held. Releasing first
            # allowed a successor to acquire the pathname, open and append, and
            # then be unlinked by this old owner.
            if count == 0 and self._directory_trusted:
                possession_fd = (
                    self._lock_fd if self._lock_fd is not None else self._path_capability_fd
                )
                inode_matches = possession_fd is not None and _path_still_matches_locked_inode(
                    self._path,
                    possession_fd,
                )
                if not inode_matches:
                    logger.warning(
                        "z4j buffer: %s changed identity before close cleanup; leaving it in place",
                        self._path,
                    )
                else:
                    # WAL mode produces three files; delete all of them.
                    # Failures here are non-fatal - an operator with a
                    # custom umask / network mount can clean up by hand.
                    for suffix in ("", "-wal", "-shm"):
                        p = Path(str(self._path) + suffix)
                        with contextlib.suppress(FileNotFoundError, OSError):
                            p.unlink()
                    _fsync_directory(self._path.parent)
            # Release only after all path mutation and directory durability.
            _release_lock(self._lock_fd)
            self._lock_fd = None
            self._close_path_capability()
            self._possession = None
        finally:
            self._lock.release()

    def release_fork_inherited_lock(self) -> None:
        """Close a forked CHILD's inherited ownership-lock fd, WITHOUT unlock.

        1.7.1 (M6). After ``fork()`` the child's fd refers to the same open
        file description (OFD) as the parent, and an flock lock lives on the
        OFD. ``os.close`` here only drops the CHILD's refcount on that shared
        OFD, so the parent keeps its ownership lock while it is alive, and the
        buffer becomes adoptable once the parent dies. Two wrong alternatives:
        calling ``close()`` / ``_release_lock`` would ``flock(LOCK_UN)`` the
        shared OFD and drop the PARENT's LIVE lock; leaving the fd open (the
        pre-1.7.1 behavior) kept the OFD -- and thus the parent's lock -- alive
        for the child's whole life, so the arbiter buffer was never recoverable
        after the arbiter died while any worker still ran. Does NOT touch the
        sqlite connection (the child must not act on the parent's DB file).
        """
        fd = self._lock_fd
        self._lock_fd = None
        path_fd = self._path_capability_fd
        self._path_capability_fd = None
        self._possession = None
        self._sealed_ready = False
        self._closed = True
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)
        if path_fd is not None:
            with contextlib.suppress(OSError):
                os.close(path_fd)


#: Absolute cap on the durable ``_seen_commands`` dedup table (rows), matching
#: the dispatcher's in-memory ``_DEDUP_MAX``. A burst of more distinct commands
#: than this inside one TTL window is bounded by dropping the oldest by seen_at.
_DURABLE_DEDUP_MAX: int = 10_000

#: An orphaned buffer file must be at least this old (seconds since last
#: write) before adoption. Guards against racing a sibling that just
#: started and whose PID our best-effort liveness check has not yet seen.
_ADOPT_MIN_AGE_S: float = 30.0

#: RH8 liveness-lease tuning. The owner refreshes its ``_meta`` heartbeat at
#: most every ``_LEASE_REFRESH_SECONDS`` (cheap: one _meta write, not per
#: append). An adopter treats a lease younger than ``_LEASE_STALE_SECONDS`` as
#: proof the owner is ALIVE and refuses adoption. The stale window is generous
#: (many refresh intervals) so a briefly-paused owner is never robbed; a truly
#: dead owner stops refreshing and its lease ages out.
_LEASE_HEARTBEAT_KEY = "lease_heartbeat"
_LEASE_REFRESH_SECONDS: float = 10.0
_LEASE_STALE_SECONDS: float = 90.0

#: RH8 follow-up: orphan adoption must be RE-SCANNED periodically, not only at
#: startup. A peer that restarts inside the stale window (< _LEASE_STALE_SECONDS
#: after a dead owner's last lease) correctly refuses adoption (the owner might
#: still be alive), but if adoption were one-shot the orphan's undelivered
#: events would be stranded. Re-scanning every _ORPHAN_RESCAN_SECONDS lets the
#: peer adopt once the dead owner's lease has aged past the stale window, so
#: recovery is merely delayed (bounded by ~_LEASE_STALE_SECONDS + this), never
#: lost.
_ORPHAN_RESCAN_SECONDS: float = 60.0

# Recovery is intentionally incremental. A stable run converges on
# lock-capable storage without allowing an old backlog to exhaust the active
# sink or the filesystem in one scan.
_RECOVERY_MAX_FILES_PER_SCAN: int = 32
_RECOVERY_MAX_ROWS_PER_FILE: int = 1_000
_RECOVERY_MAX_BYTES_PER_FILE: int = 16 * 1024 * 1024
_RECOVERY_MIN_FREE_BYTES: int = 16 * 1024 * 1024


def _pid_is_alive(pid: int) -> bool:
    """Best-effort: is a process with this pid running?

    POSIX uses ``os.kill(pid, 0)`` (no signal sent; raises if the pid is
    gone). On Windows ``os.kill(pid, 0)`` sends CTRL_C_EVENT (signal 0 IS
    that event) -- never call it there; we conservatively report "alive"
    so a Windows orphan is skipped rather than risking adoption of a live
    sibling's buffer. Errors default to "alive" (safe: skip adoption).
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by another user
    except OSError:
        return True
    return True


#: Errnos a refused non-blocking flock raises when another process HOLDS
#: the lock (genuine contention). Any OTHER OSError from flock (EOPNOTSUPP /
#: ENOLCK / ENOSYS / EINVAL on lock-unsupporting filesystems) is treated as
#: no-flock, not contention, so the agent is not disabled on a mount that cannot
#: lock. EAGAIN and EWOULDBLOCK are the same value on Linux but distinct on some
#: platforms, so both are listed.
_CONTENDED_LOCK_ERRNOS: frozenset[int] = frozenset({errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES})


def _private_degraded_buffer_root(requested_root: Path) -> Path:
    """Return a verified owner-private fallback for unsafe POSIX semantics."""
    uid = str(os.getuid()) if hasattr(os, "getuid") else "process"
    namespace = hashlib.sha256(
        str(requested_root.absolute()).encode("utf-8"),
    ).hexdigest()[:16]
    bases = [Path(tempfile.gettempdir())]
    posix_tmp = Path("/tmp")  # noqa: S108 - trusted only after owner/mode validation
    if os.name == "posix" and posix_tmp not in bases:
        bases.append(posix_tmp)

    for base in bases:
        candidate = base / f"z4j-buffer-{uid}-{namespace}"
        try:
            candidate.mkdir(mode=0o700, parents=False, exist_ok=False)
        except FileExistsError:
            pass
        except OSError:
            continue
        trusted, _reason = _buffer_directory_trust(candidate)
        if trusted:
            return candidate

        try:
            random_root = Path(
                tempfile.mkdtemp(prefix=f"z4j-buffer-{uid}-", dir=base),
            )
        except OSError:
            continue
        trusted, _reason = _buffer_directory_trust(random_root)
        if trusted:
            return random_root

    raise BufferOwnershipError(
        "cannot find an owner-controlled fallback for the z4j buffer; "
        f"requested directory was {requested_root}"
    )


def _buffer_directory_trust(directory: Path) -> tuple[bool, str]:
    """Report whether pathname-destructive operations are safe here.

    POSIX cannot atomically unlink "only if still inode I". Soundness therefore
    requires an owner-controlled directory: no other principal may replace a
    pathname while a cooperating z4j process holds its inode capability.

    A false result is a degraded operating mode, not a startup failure. The
    caller may use an O_EXCL-created process-generation path while disabling
    automatic recovery and unlink.
    """
    if os.name != "posix":
        return True, "non-POSIX platform"
    try:
        st = directory.lstat()
    except OSError as exc:
        return False, f"stat failed: {exc}"
    getuid = getattr(os, "getuid", None)
    if not stat.S_ISDIR(st.st_mode):
        return False, "parent is not a directory"
    if getuid is not None and st.st_uid != getuid():
        return False, "directory is not owned by the current uid"
    if st.st_mode & 0o022:
        return False, "directory is group/other writable"
    return True, "owner-controlled"


def _fsync_directory(directory: Path) -> None:
    """Best-effort durability for create/unlink directory entries."""
    if os.name != "posix":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(str(directory), flags)
    except OSError:
        return
    try:
        with contextlib.suppress(OSError):
            os.fsync(fd)
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)


def _lock_proves_exclusion(path: Path, held_fd: int) -> bool:
    """Verify that a successful flock actually excludes a second OFD.

    Some filesystems accept ``flock`` while silently doing nothing. A second
    independent open must be refused while ``held_fd`` owns the lock; otherwise
    the lock is not a possession capability and recovery must stay manual.
    """
    if fcntl is None:
        return False
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        probe = os.open(str(path), flags)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            return exc.errno in _CONTENDED_LOCK_ERRNOS
        else:
            with contextlib.suppress(OSError):
                fcntl.flock(probe, fcntl.LOCK_UN)
            return False
    finally:
        with contextlib.suppress(OSError):
            os.close(probe)


def _same_open_inode(left_fd: int | None, right_fd: int | None) -> bool:
    """True when two live descriptors identify the same regular inode."""
    if left_fd is None or right_fd is None:
        return False
    try:
        left = os.fstat(left_fd)
        right = os.fstat(right_fd)
    except OSError:
        return False
    return (
        stat.S_ISREG(left.st_mode)
        and stat.S_ISREG(right.st_mode)
        and left.st_dev == right.st_dev
        and left.st_ino == right.st_ino
    )


def _open_exclusive_path_capability(path: Path) -> int:
    """Atomically create ``path`` and return its lifetime identity capability.

    Windows' ordinary ``os.open`` handle does not share deletion. Keeping that
    handle live (required to prove possession through close cleanup) therefore
    makes ``Path.unlink`` fail with ``PermissionError``. CreateFileW lets the
    fresh handle share read, write, and delete while retaining CREATE_NEW's
    atomic non-existence requirement. The handle remains the stable identity
    capability; allowing deletion merely lets its current owner mutate the
    pathname without releasing possession first.
    """
    if os.name != "nt":
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        return os.open(str(path), flags, 0o600)

    import ctypes
    import msvcrt
    from ctypes import wintypes

    create_file = ctypes.WinDLL("kernel32", use_last_error=True).CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE

    generic_read_write = 0x80000000 | 0x40000000
    share_read_write_delete = 0x00000001 | 0x00000002 | 0x00000004
    create_new = 1
    file_attribute_normal = 0x00000080
    handle = create_file(
        str(path),
        generic_read_write,
        share_read_write_delete,
        None,
        create_new,
        file_attribute_normal,
        None,
    )
    invalid_handle_value = ctypes.c_void_p(-1).value
    if handle == invalid_handle_value:
        error = ctypes.get_last_error()
        message = ctypes.FormatError(error)
        if error in {80, 183}:  # ERROR_FILE_EXISTS / ERROR_ALREADY_EXISTS
            raise FileExistsError(error, message, str(path))
        raise OSError(error, message, str(path))

    try:
        return msvcrt.open_osfhandle(
            int(handle),
            os.O_RDWR | getattr(os, "O_BINARY", 0),
        )
    except BaseException:
        ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(handle)
        raise


def _acquire_own_lock(path: Path) -> tuple[int | None, bool]:
    """Take an exclusive, non-blocking flock on our OWN buffer file, held
    for the process lifetime. Returns ``(fd, contended)``:

    - ``(fd, False)``   -- the lock is held by us.
    - ``(None, True)``  -- another process ALREADY holds the lock (LOCK_NB was
      refused). P1-7: the caller must treat the file as CURRENTLY OWNED and skip
      any destructive ownership work (discarding / re-stamping foreign rows);
      the live holder -- or an in-progress orphan adopter that gated on this same
      flock -- is operating on it.
    - ``(None, False)`` -- locking is unavailable (Windows / no fcntl) or the
      file could not be opened; best-effort single-process assumption applies and
      the agent still runs (only cross-process adoption liveness is affected).
    """
    if fcntl is None:
        return None, False
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(str(path), flags)
    except OSError:
        return None, False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        with contextlib.suppress(OSError):
            os.close(fd)
        # Distinguish "another process HOLDS the lock" from "this
        # filesystem does not SUPPORT flock". A refused LOCK_NB raises
        # EWOULDBLOCK/EAGAIN (and EACCES on some platforms) -- genuine contention,
        # so the file is CURRENTLY OWNED (relocate / suppress destructive work).
        # But an unsupported-lock filesystem (EOPNOTSUPP / ENOLCK / ENOSYS /
        # EINVAL -- observed on some network mounts and WSL DrvFS) also raises
        # OSError here; blanket-treating that as contention needlessly relocates
        # and then fails closed with BufferOwnershipError, disabling the agent on
        # a mount that merely cannot lock. Treat unsupported-lock like no-fcntl
        # (best-effort single-process); only true contention marks it owned.
        if exc.errno in _CONTENDED_LOCK_ERRNOS:
            # Already held. For our own fresh per-pid file this is the pid-reuse /
            # PID-namespace edge: a live foreign owner (or an adopter) holds it.
            return None, True
        return None, False
    if not _lock_proves_exclusion(path, fd):
        _release_lock(fd)
        return None, False
    return fd, False


def _release_lock(fd: int | None) -> None:
    """Release + close a lock fd from :func:`_acquire_own_lock` /
    :func:`_claim_dead_orphan`. Best-effort."""
    if fd is None:
        return
    if fcntl is not None:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
    with contextlib.suppress(OSError):
        os.close(fd)


def _orphan_is_own_regular_file(path: Path) -> bool:
    """M4: only adopt a buffer that is a REGULAR file owned by THIS uid.

    Adoption ingests the file's rows and re-signs them with the agent's
    HMAC secret before delivering them to the brain. Without this check, on
    a host where Z4J_HOME is group/world-writable a local user could plant a
    ``buffer-<pid>.sqlite`` of forged event/command frames the victim agent
    would sign and deliver as authentic (task-history poisoning, forged
    command results). Reject symlinks and anything not owned by us.
    """
    try:
        st = path.lstat()
    except OSError:
        return False
    if not stat.S_ISREG(st.st_mode):
        return False  # symlink / dir / fifo / socket
    getuid = getattr(os, "getuid", None)
    return not (getuid is not None and st.st_uid != getuid())


def _claim_dead_orphan(path: Path) -> int | None:
    """Try to take an orphan's lifetime lock (become its owner).

    Returns a HELD lock fd iff the previous owner is provably DEAD (the
    kernel released its flock on death, so ours succeeds). Returns None when
    the owner is still alive (lock held) OR locking is unavailable (Windows
    -> we never adopt, preserving the pre-1.7.1 conservative behaviour).

    The caller drains + unlinks while STILL HOLDING this fd. This is the
    lifetime ownership lock (not an atomic rename): a slow-but-live sibling
    keeps its lock, so we never drain + unlink a file it is still writing,
    and a concurrent adopter cannot double-drain. Immune to the pid-reuse
    race a bare os.kill(pid, 0) check suffers.
    """
    if fcntl is None:
        return None
    try:
        flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(str(path), flags)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if not _lock_proves_exclusion(path, fd):
            _release_lock(fd)
            return None
        # Pre-1.8 writers never took z4j's flock, but SQLite itself holds POSIX
        # byte-range locks while a connection owns the database.  Taking both
        # lock families on this one read/write fd makes possession relative to
        # current z4j peers *and* live 1.7 writers.  This is inode authority, so
        # an explicitly configured numeric-looking filename cannot masquerade
        # as a dead historical per-PID source.
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        _release_lock(fd)
        return None
    return fd


def _path_still_matches_locked_inode(
    path: Path,
    lock_fd: int,
    *,
    reassert_sqlite_exclusion: bool = False,
) -> bool:
    """RH7: re-verify the PATHNAME still resolves to the EXACT inode we hold
    locked (opened O_NOFOLLOW), is a regular file, and is owned by us.

    ``_drain_orphan_into`` opens the orphan by PATH (sqlite3.connect needs a
    path). An attacker with write access to the buffer directory could swap a
    forged database in at the pathname between our ``lstat`` and sqlite's open
    (a TOCTOU). Re-checking the pathname -> inode binding after the open, and
    again before the unlink, detects such a swap so we neither drain a forged
    DB nor unlink a replacement. The PRIMARY defense remains an owner-only
    buffer directory (``_warn_if_z4j_home_loose``); this is defense in depth.
    """
    try:
        if reassert_sqlite_exclusion:
            if fcntl is None:
                return False
            # POSIX process-scoped record locks have the surprising rule that
            # closing *any* descriptor for this inode releases every record
            # lock this process holds on it. Reassert the pre-1.8
            # SQLite-exclusion lock after earlier snapshot or SQLite
            # descriptors may have closed.
            fcntl.lockf(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Never open a pathname-probe descriptor here: doing so would discard
        # the recovery record lock when that descriptor closes. A no-follow
        # stat observes the binding without opening the inode.
        fst = os.fstat(lock_fd)
        pst = path.lstat()
    except OSError:
        return False
    getuid = getattr(os, "getuid", None)
    return (
        stat.S_ISREG(pst.st_mode)
        and pst.st_dev == fst.st_dev
        and pst.st_ino == fst.st_ino
        and not (getuid is not None and pst.st_uid != getuid())
    )


def _source_component_signature(
    path: Path,
    *,
    required_main: tuple[int, int] | None = None,
) -> tuple[int, int, int, int] | None:
    """Return a no-follow identity for one stable SQLite file component."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(str(path), flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise BufferMetadataUnreadableError(
            f"could not open SQLite component {path.name}: {exc}",
        ) from exc
    try:
        st = os.fstat(fd)
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)
    getuid = getattr(os, "getuid", None)
    if not stat.S_ISREG(st.st_mode):
        raise BufferMetadataUnreadableError(
            f"SQLite component {path.name} is not a regular file",
        )
    if getuid is not None and st.st_uid != getuid():
        raise BufferMetadataUnreadableError(
            f"SQLite component {path.name} is not owned by the current uid",
        )
    if required_main is not None and (st.st_dev, st.st_ino) != required_main:
        raise BufferMetadataUnreadableError(
            f"SQLite main file {path.name} no longer matches the held inode",
        )
    return (int(st.st_dev), int(st.st_ino), int(st.st_size), int(st.st_mtime_ns))


def _copy_source_component(
    source: Path,
    destination: Path,
    expected: tuple[int, int, int, int],
) -> None:
    """Copy one already-identified regular component without following links."""
    source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    source_flags |= getattr(os, "O_NOFOLLOW", 0)
    destination_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    destination_flags |= getattr(os, "O_CLOEXEC", 0)
    source_fd = os.open(str(source), source_flags)
    destination_fd: int | None = None
    try:
        st = os.fstat(source_fd)
        observed = (int(st.st_dev), int(st.st_ino), int(st.st_size), int(st.st_mtime_ns))
        if observed != expected or not stat.S_ISREG(st.st_mode):
            raise BufferMetadataUnreadableError(
                f"SQLite component {source.name} changed before snapshot",
            )
        destination_fd = os.open(str(destination), destination_flags, 0o600)
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            remaining = memoryview(chunk)
            while remaining:
                written = os.write(destination_fd, remaining)
                if written <= 0:
                    raise OSError("short write while creating classification snapshot")
                remaining = remaining[written:]
        os.fsync(destination_fd)
        final = os.fstat(source_fd)
        final_signature = (
            int(final.st_dev),
            int(final.st_ino),
            int(final.st_size),
            int(final.st_mtime_ns),
        )
        if final_signature != expected:
            raise BufferMetadataUnreadableError(
                f"SQLite component {source.name} changed during snapshot",
            )
    finally:
        if destination_fd is not None:
            with contextlib.suppress(OSError):
                os.close(destination_fd)
        with contextlib.suppress(OSError):
            os.close(source_fd)


@contextlib.contextmanager
def _open_classification_snapshot(
    path: Path,
    lock_fd: int,
    *,
    scratch_parent: Path,
):
    """Open a disposable coherent copy so classification never mutates a source.

    A normal SQLite open of a crash-shaped WAL database checkpoints the WAL,
    rewrites the main file, and removes sidecars. Until attribution is known,
    that would mutate a FOREIGN/UNKNOWN source. The held possession lock makes
    the dead owner's main/WAL/journal set stable enough to copy; SQLite may then
    perform any required recovery only inside this private scratch directory.
    """
    try:
        locked = os.fstat(lock_fd)
    except OSError as exc:
        raise BufferMetadataUnreadableError("held source inode is unreadable") from exc
    required_main = (int(locked.st_dev), int(locked.st_ino))
    suffixes = ("", "-wal", "-shm", "-journal")
    before: dict[str, tuple[int, int, int, int] | None] = {}
    for suffix in suffixes:
        before[suffix] = _source_component_signature(
            Path(str(path) + suffix),
            required_main=required_main if not suffix else None,
        )
    if before[""] is None:
        raise BufferMetadataUnreadableError("SQLite main file disappeared before classification")
    copied_suffixes = ("", "-wal", "-journal")
    snapshot_bytes = sum(
        signature[2] for suffix in copied_suffixes if (signature := before[suffix]) is not None
    )
    if snapshot_bytes > _RECOVERY_MAX_BYTES_PER_FILE:
        raise BufferMetadataUnreadableError(
            "classification snapshot exceeds the per-source recovery byte bound",
        )
    try:
        free_bytes = shutil.disk_usage(scratch_parent).free
    except OSError as exc:
        raise BufferMetadataUnreadableError(
            "classification scratch free space is unreadable",
        ) from exc
    if free_bytes - snapshot_bytes < _RECOVERY_MIN_FREE_BYTES:
        raise BufferMetadataUnreadableError(
            "classification snapshot would consume the recovery free-space reserve",
        )

    with tempfile.TemporaryDirectory(
        prefix="z4j-buffer-classify-",
        dir=str(scratch_parent),
    ) as temporary:
        snapshot = Path(temporary) / "source.sqlite"
        for suffix in copied_suffixes:
            expected = before[suffix]
            if expected is not None:
                _copy_source_component(
                    Path(str(path) + suffix),
                    Path(str(snapshot) + suffix),
                    expected,
                )

        after_copy = {
            suffix: _source_component_signature(
                Path(str(path) + suffix),
                required_main=required_main if not suffix else None,
            )
            for suffix in suffixes
        }
        if after_copy != before or not _path_still_matches_locked_inode(
            path,
            lock_fd,
            reassert_sqlite_exclusion=True,
        ):
            raise BufferMetadataUnreadableError(
                "SQLite source changed while its classification snapshot was created",
            )

        conn = sqlite3.connect(str(snapshot), isolation_level=None)
        try:
            yield conn
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.close()

        after_read = {
            suffix: _source_component_signature(
                Path(str(path) + suffix),
                required_main=required_main if not suffix else None,
            )
            for suffix in suffixes
        }
        if after_read != before or not _path_still_matches_locked_inode(
            path,
            lock_fd,
            reassert_sqlite_exclusion=True,
        ):
            raise BufferMetadataUnreadableError(
                "SQLite source changed while its classification snapshot was read",
            )


class LeaseUnreadableError(Exception):
    """M5: a liveness lease is PRESENT but not a finite number (corrupt text, or
    NaN/inf). The caller must fail CLOSED (treat the owner as alive), never
    mistake it for an intentionally-absent legacy lease."""


class DeploymentIdUnreadableError(Exception):
    """The deployment fingerprint could not be read.

    Unreadable, absent, and different stamps have distinct diagnostics, but all
    are preserved as recovery sources. Only a readable matching stamp plus a
    sealed lifecycle permits automatic emission.
    """


class BufferMetadataUnreadableError(Exception):
    """A lifecycle/identity metadata read failed or returned malformed data."""


class BufferForeignDataError(Exception):
    """This buffer holds events stamped for a DIFFERENT deployment.

    Raised instead of discarding them. The stamp is derived from the agent's
    hmac_secret, so a rotated secret, a restored backup or a copied buffer
    directory all make a deployment's OWN events read as foreign -- deleting on
    that evidence destroyed recoverable data. The caller relocates to a fresh
    buffer and leaves this file untouched for an operator to inspect.
    """


class BufferOwnershipError(Exception):
    """The process could not establish the buffer possession invariant.

    The runtime fails closed rather than operating on a shared inode or inside a
    directory another principal can rewrite.
    """


class RecoveryBackpressureError(Exception):
    """The active sink or per-scan budget cannot accept another recovery row."""


#: _meta key holding the current owner's unique per-open token.
_OWNER_TOKEN_KEY = "owner_token"  # noqa: S105  a _meta key name, not a secret
_OWNER_PID_KEY = "owner_pid"
_OWNER_GENERATION_KEY = "owner_generation"
_BUFFER_UUID_KEY = "buffer_uuid"
_LIFECYCLE_KEY = "lifecycle"
_LIFECYCLE_CREATED = "created"
_LIFECYCLE_INITIALIZING = "initializing"
_LIFECYCLE_SEALED_READY = "sealed_ready"


def _read_owner_token(conn: sqlite3.Connection) -> str | None:
    """The current owner's per-open token from ``_meta``, or None (no live
    owner / a legacy buffer / a cleanly-closed buffer whose token was cleared).
    Propagates ``sqlite3.Error`` (missing ``_meta`` on a brand-new file, or an
    unreadable DB) so the caller can treat it as 'not owned'."""
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?",
        (_OWNER_TOKEN_KEY,),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def _read_lease_heartbeat(conn: sqlite3.Connection) -> float | None:
    """RH8: the owner's last liveness-lease timestamp from ``_meta`` (unix
    seconds), or None for a buffer written before the lease existed.

    P1-8: propagates ``sqlite3.Error`` rather than swallowing it into None. A
    read that FAILS is not evidence of "no lease" -- the caller must fail CLOSED.

    M5: only a truly ABSENT key (row missing / value NULL) returns None. A
    PRESENT-but-corrupt value (non-numeric text, or a non-finite float like NaN
    which does not raise on float()) raises :class:`LeaseUnreadableError` so the
    caller fails closed. Previously both returned None, so a corrupt/NaN lease
    was treated as legacy-absent -> the liveness gate was skipped and a live
    owner's buffer became adoptable."""
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?",
        (_LEASE_HEARTBEAT_KEY,),
    ).fetchone()
    if not row or row[0] is None:
        return None  # true legacy absence
    try:
        val = float(row[0])
    except (TypeError, ValueError) as exc:
        raise LeaseUnreadableError(f"lease value {row[0]!r} is not a number") from exc
    if not math.isfinite(val):
        raise LeaseUnreadableError(f"lease value {val!r} is not finite")
    return val


def _read_deployment_id(conn: sqlite3.Connection) -> str | None:
    """The deployment fingerprint stamped in a buffer's ``_meta`` (H8), or
    None for a legacy buffer written before the stamp existed.

    Raises:
        DeploymentIdUnreadableError: the fingerprint could not be READ. This is
            not the same as an absent legacy stamp. Both states require
            preservation and operator-visible recovery, while only a readable
            matching stamp can authorize automatic emission.
    """
    try:
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = 'deployment'",
        ).fetchone()
    except sqlite3.Error as exc:
        raise DeploymentIdUnreadableError(str(exc)) from exc
    return row[0] if row else None


def _read_buffer_meta(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = ?",
            (key,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise BufferMetadataUnreadableError(f"{key}: {exc}") from exc
    if not row or row[0] is None:
        return None
    return str(row[0])


def _read_buffer_uuid(conn: sqlite3.Connection) -> str | None:
    value = _read_buffer_meta(conn, _BUFFER_UUID_KEY)
    if value is None:
        return None
    try:
        return uuid.UUID(value).hex
    except ValueError as exc:
        raise BufferMetadataUnreadableError(f"invalid buffer UUID {value!r}") from exc


def _read_lifecycle(conn: sqlite3.Connection) -> str | None:
    value = _read_buffer_meta(conn, _LIFECYCLE_KEY)
    if value is None:
        return None
    if value not in {
        _LIFECYCLE_CREATED,
        _LIFECYCLE_INITIALIZING,
        _LIFECYCLE_SEALED_READY,
    }:
        raise BufferMetadataUnreadableError(f"unsupported lifecycle value {value!r}")
    return value


def _has_legacy_entries_schema(conn: sqlite3.Connection) -> bool:
    """Return whether ``entries`` has the exact historical row shape."""
    try:
        columns = tuple(
            (
                str(row[1]),
                str(row[2]).upper(),
                int(row[3]),
                None if row[4] is None else str(row[4]),
                int(row[5]),
            )
            for row in conn.execute("PRAGMA table_info(entries)").fetchall()
        )
    except sqlite3.Error:
        return False
    return columns == (
        ("id", "INTEGER", 0, None, 1),
        ("kind", "TEXT", 1, None, 0),
        ("payload", "BLOB", 1, None, 0),
        ("created_at", "REAL", 1, None, 0),
        ("attempts", "INTEGER", 1, "0", 0),
        ("content_rejects", "INTEGER", 1, "0", 0),
    )


def _has_exact_pre_18_buffer_schema(conn: sqlite3.Connection) -> bool:
    """Recognize only the bounded pre-1.8 database-object shape.

    A real 1.7 buffer has ``entries`` (and SQLite's internal sequence) but no
    ``_meta`` or other 1.8 tables. Merely finding compatible entry columns is
    insufficient: a damaged current buffer with a missing ``_meta`` table must
    remain UNKNOWN, not be reclassified as trusted legacy data.
    """
    try:
        objects = {
            (str(row[0]), str(row[1]))
            for row in conn.execute(
                "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
    except sqlite3.Error:
        return False
    if objects != {
        ("table", "entries"),
        ("index", "idx_entries_created_at"),
    } or not _has_legacy_entries_schema(conn):
        return False
    try:
        index_columns = tuple(
            str(row[2])
            for row in conn.execute(
                "PRAGMA index_info('idx_entries_created_at')",
            ).fetchall()
        )
    except sqlite3.Error:
        return False
    return index_columns == ("created_at",)


def _orphan_is_adoptable(  # noqa: PLR0911, PLR0912 - fail-closed gates
    conn: sqlite3.Connection,
    path: Path,
    lock_fd: int,
    current: BufferStore,
) -> bool:
    """Gate an OPENED orphan before draining it (RH7 + RH6).

    RH7: sqlite opened the orphan BY PATH, so confirm the pathname STILL
    resolves to the exact inode we locked O_NOFOLLOW -- an attacker able to
    write the buffer directory could have swapped a forged DB in at the
    pathname between our lstat and this open.

    Current-format sources require matching deployment attribution and
    ``SEALED_READY``. A source with neither a lifecycle nor a UUID is recognized
    as pre-1.8 only when it has the legacy entries schema and lives beside the
    current sink in the same owner-controlled directory. That directory is the
    migration authority for pre-fingerprint 1.7.0 data; explicit shared paths
    are never discovered by this scanner.
    """
    if not _path_still_matches_locked_inode(
        path,
        lock_fd,
        reassert_sqlite_exclusion=True,
    ):
        logger.warning(
            "z4j buffer: orphan %s pathname diverged from the locked inode "
            "after opening; refusing to adopt",
            path.name,
        )
        return False
    # Real 1.7 buffers have no _meta table, so attempting to read the 1.8 lease
    # first turns a valid upgrade source into an "unreadable" current buffer.
    # Recognize the exact old schema before any metadata read, but only in the
    # verified private migration-authority directory.
    legacy_ready = (
        current._directory_trusted
        and path.parent == current.path.parent
        and _has_exact_pre_18_buffer_schema(conn)
    )
    if legacy_ready:
        # A 1.7 writer never acquired the 1.8 lifetime flock, so taking that
        # flock proves exclusion only against 1.8 peers. The historical default
        # filename still carries its writer PID; while that process is alive,
        # the exact legacy database must remain untouched even if its mtime is
        # old and its SQLite connection is idle. A reused live PID can delay
        # recovery, which is preferable to draining a live pre-upgrade writer.
        source_pid = _per_process_buffer_pid(path)
        if source_pid is None or _pid_is_alive(source_pid):
            logger.info(
                "z4j buffer: pre-1.8 source %s still has a live or "
                "unverifiable filename owner; leaving it in place",
                path.name,
            )
            return False
        logger.info(
            "z4j buffer: recovering pre-1.8 source %s from the current "
            "owner-controlled buffer directory",
            path.name,
        )
        return True
    # RH8: refuse a buffer whose owner is provably still ALIVE by its own
    # liveness lease -- even if flock succeeded (a no-op on some NFS mounts) and
    # the pid check passed (meaningless across PID namespaces, or after pid
    # reuse). A lease younger than the stale window means the owner refreshed it
    # recently, so draining + unlinking would rob a live writer.
    try:
        lease = _read_lease_heartbeat(conn)
    except (sqlite3.Error, LeaseUnreadableError) as exc:
        # P1-8: an UNREADABLE lease (WAL contention) is not proof the owner is
        # dead. M5: a PRESENT-but-corrupt/NaN lease is likewise not evidence of
        # legacy-absence. Fail CLOSED (treat the owner as alive) in both cases so
        # we never drain + unlink a live writer; the periodic re-scan retries.
        logger.warning(
            "z4j buffer: could not read orphan %s liveness lease; refusing to "
            "adopt (failing closed)",
            path.name,
        )
        current._record_recovery_required(
            conn=conn,
            path=path,
            lock_fd=lock_fd,
            attribution="unknown",
            lifecycle="unknown",
            reason=f"liveness lease unreadable: {exc}",
        )
        return False
    if lease is not None and (time.time() - lease) < _LEASE_STALE_SECONDS:
        logger.info(
            "z4j buffer: orphan %s has a fresh liveness lease (owner alive); leaving it in place",
            path.name,
        )
        return False
    # Attribution and readiness are independent. Current-format sources require
    # SAME + SEALED_READY. A narrowly recognized pre-1.8 schema is the only
    # lifecycle exception.
    try:
        orphan_dep = _read_deployment_id(conn)
    except DeploymentIdUnreadableError:
        logger.warning(
            "z4j buffer: could not read orphan %s deployment fingerprint; "
            "refusing to adopt (failing closed)",
            path.name,
        )
        current._record_recovery_required(
            conn=conn,
            path=path,
            lock_fd=lock_fd,
            attribution="unknown",
            lifecycle="unknown",
            reason="deployment fingerprint unreadable",
        )
        return False
    if current.deployment_id is None:
        attribution = "same"
    elif orphan_dep is None:
        attribution = (
            "legacy-local"
            if current._directory_trusted and path.parent == current.path.parent
            else "unknown"
        )
    elif orphan_dep == current.deployment_id:
        attribution = "same"
    else:
        attribution = "foreign"

    try:
        lifecycle = _read_lifecycle(conn)
        source_uuid = _read_buffer_uuid(conn)
    except BufferMetadataUnreadableError as exc:
        current._record_recovery_required(
            conn=conn,
            path=path,
            lock_fd=lock_fd,
            attribution=attribution,
            lifecycle="unknown",
            reason=f"lifecycle identity unreadable: {exc}",
        )
        return False

    if current.deployment_id is not None and (
        attribution != "same" or lifecycle != _LIFECYCLE_SEALED_READY or source_uuid is None
    ):
        if attribution == "unknown":
            logger.info(
                "z4j buffer: leaving unattributed orphan %s in place; manual recovery is required",
                path.name,
            )
        current._record_recovery_required(
            conn=conn,
            path=path,
            lock_fd=lock_fd,
            attribution=attribution,
            lifecycle=lifecycle or "unknown",
            reason=(
                "source is not attributable to this deployment"
                if attribution != "same"
                else (
                    "source did not reach SEALED_READY"
                    if lifecycle != _LIFECYCLE_SEALED_READY
                    else "source has no durable buffer UUID"
                )
            ),
        )
        return False
    return True


def _copy_and_clear_orphan_rows(
    conn: sqlite3.Connection,
    current: BufferStore,
    *,
    progress: list[int] | None = None,
    max_rows: int = _RECOVERY_MAX_ROWS_PER_FILE,
    max_bytes: int = _RECOVERY_MAX_BYTES_PER_FILE,
) -> int:
    """Copy every entry from the orphan ``conn`` into ``current``, deleting each
    row from the orphan AS IT IS durably copied. Returns the count copied.

    buffer:1212: the per-row delete is committed immediately so that if the
    caller later fails to unlink the orphan file, it no longer holds the copied
    rows, so a periodic rescan does not re-copy them (duplicate delivery). At
    most one in-flight row can duplicate if the process dies between the append
    and the commit. ``current.append`` is durable (the buffer is autocommit).

    buffer:1288: if ``progress`` (a single-element list) is supplied, the count
    of rows durably moved is written to ``progress[0]`` after EACH row, so a
    caller that catches a mid-loop error still learns how many were adopted
    (rather than losing the local ``count`` when this function's return is
    skipped by the raise).
    """
    count = 0
    copied_bytes = 0
    while True:
        row = conn.execute(
            "SELECT id, kind, length(payload) FROM entries ORDER BY id ASC LIMIT 1",
        ).fetchone()
        if row is None:
            break
        row_id, kind, payload_size = row
        if not isinstance(payload_size, int) or payload_size < 0:
            raise RecoveryBackpressureError("orphan payload size is unreadable")
        if count >= max_rows or copied_bytes + payload_size > max_bytes:
            raise RecoveryBackpressureError("per-scan recovery budget exhausted")
        payload_row = conn.execute(
            "SELECT payload FROM entries WHERE id = ?",
            (row_id,),
        ).fetchone()
        if payload_row is None:
            raise sqlite3.DatabaseError("orphan row disappeared during recovery")
        payload = payload_row[0]
        payload_bytes = bytes(payload)
        if len(payload_bytes) != payload_size:
            raise sqlite3.DatabaseError("orphan payload changed during recovery")
        append_recovered = getattr(current, "_append_recovered", None)
        if append_recovered is not None:
            if not append_recovered(str(kind), payload_bytes):
                raise RecoveryBackpressureError("active buffer has no recovery capacity")
        else:
            current.append(str(kind), payload_bytes)
        conn.execute("DELETE FROM entries WHERE id = ?", (row_id,))
        # buffer:1212: commit the delete NOW so durability does not depend on the
        # caller opening the orphan in autocommit mode. On an autocommit
        # connection this is a harmless no-op; on a transactional one it makes the
        # row's removal survive the caller's conn.close() (which would otherwise
        # roll back the open transaction, leaving the copied rows to be re-copied
        # on a later rescan -- the exact duplicate delivery this closes).
        conn.commit()
        count += 1
        copied_bytes += len(payload_bytes)
        if progress is not None:
            progress[0] = count
    # Carry the peer's DURABLE dedup set across adoption (a pid-change
    # restart), so a fire the dead peer already executed is not re-run by the
    # adopting agent. Best-effort; a pre-H10 orphan has no such table.
    with contextlib.suppress(sqlite3.Error):
        seen = conn.execute(
            "SELECT command_id, seen_at FROM _seen_commands",
        ).fetchall()
        if seen:
            current.import_seen_commands([(str(c), float(t)) for c, t in seen])
    # Recovery advisories live in the active sink. If that process crashes,
    # recovering its buffer must carry those records forward before unlinking
    # the old sink, or the manual-recovery signal disappears.
    current._import_recovery_required(conn)
    with contextlib.suppress(sqlite3.Error, BufferMetadataUnreadableError):
        current._clear_recovery_required(_read_buffer_uuid(conn))
    return count


def _drain_orphan_into(  # noqa: PLR0911, PLR0912, PLR0915 - fail-closed recovery gates
    current: BufferStore,
    path: Path,
    lock_fd: int,
) -> int:
    """Copy every entry from an orphaned buffer DB into ``current``,
    then delete the orphan (+ its WAL/SHM sidecars). Returns the count
    adopted. Best-effort: a corrupt/locked orphan is left in place. The
    caller holds the orphan's ownership lock (``lock_fd``) for the duration.
    """
    # M1: we hold an O_NOFOLLOW-opened, flocked fd on the orphan inode. SQLite
    # is about to open the file BY PATH, so verify the path still resolves
    # (lstat, no symlink follow) to the EXACT (st_dev, st_ino) of our flocked
    # fd -- and is still a regular file we own -- so an attacker cannot swap a
    # symlink / replacement inode in at the pathname between the lock and the
    # sqlite open and have us drain + re-sign a forged database.
    try:
        fst = os.fstat(lock_fd)
        lst = path.lstat()
    except OSError:
        return 0
    getuid = getattr(os, "getuid", None)
    if (
        not stat.S_ISREG(lst.st_mode)
        or lst.st_dev != fst.st_dev
        or lst.st_ino != fst.st_ino
        or (getuid is not None and fst.st_uid != getuid())
    ):
        logger.warning(
            "z4j buffer: orphan %s changed identity after we locked it; refusing to adopt",
            path.name,
        )
        return 0
    # Classify a disposable coherent copy first. Opening the source itself can
    # checkpoint a crash WAL and delete its sidecars, which is authorized only
    # after SAME/recognized-legacy attribution has been proved.
    try:
        with _open_classification_snapshot(
            path,
            lock_fd,
            scratch_parent=current.path.parent,
        ) as classification:
            adoptable = _orphan_is_adoptable(
                classification,
                path,
                lock_fd,
                current,
            )
    except (OSError, sqlite3.Error, BufferMetadataUnreadableError) as exc:
        current._record_recovery_required(
            conn=None,
            path=path,
            lock_fd=lock_fd,
            attribution="unknown",
            lifecycle="unknown",
            reason=f"classification snapshot failed: {exc}",
        )
        return 0
    if not adoptable:
        return 0

    try:
        # buffer:1212: autocommit so each per-row DELETE below is durable
        # immediately after that row is copied.
        conn = sqlite3.connect(str(path), isolation_level=None)
    except sqlite3.Error as exc:
        current._record_recovery_required(
            conn=None,
            path=path,
            lock_fd=lock_fd,
            attribution="unknown",
            lifecycle="unknown",
            reason=f"mutable recovery open failed: {exc}",
        )
        return 0
    # Re-check the live logical database after SQLite has recovered any WAL.
    # The first check authorized this mutable open; the second prevents recovery
    # if its resulting state differs from the proved snapshot.
    if not _orphan_is_adoptable(conn, path, lock_fd, current):
        with contextlib.suppress(sqlite3.Error):
            conn.close()
        return 0
    try:
        free_bytes = shutil.disk_usage(current.path.parent).free
    except OSError:
        free_bytes = _RECOVERY_MIN_FREE_BYTES
    if free_bytes < _RECOVERY_MIN_FREE_BYTES:
        current._record_recovery_required(
            conn=conn,
            path=path,
            lock_fd=lock_fd,
            attribution="same",
            lifecycle=_LIFECYCLE_SEALED_READY,
            reason="insufficient free space for bounded recovery",
        )
        with contextlib.suppress(sqlite3.Error):
            conn.close()
        return 0
    count = 0
    moved = [0]
    try:
        count = _copy_and_clear_orphan_rows(conn, current, progress=moved)
    except (sqlite3.Error, RecoveryBackpressureError):
        # buffer:1288: the helper's local count is lost when it raises mid-loop,
        # so recover the rows it durably moved from ``moved`` -- returning 0 would
        # under-report (and mislead the adopted-count log) even though N rows are
        # already in ``current`` and committed-deleted from the orphan. Return
        # early WITHOUT unlinking so the un-moved rows survive for a later rescan.
        count = moved[0]
        logger.warning(
            "z4j buffer: bounded recovery paused for %s after %d row(s); "
            "leaving the rest for a later rescan",
            path.name,
            count,
        )
        current._record_recovery_required(
            conn=conn,
            path=path,
            lock_fd=lock_fd,
            attribution="same",
            lifecycle=_LIFECYCLE_SEALED_READY,
            reason="bounded recovery paused before the source drained",
        )
        with contextlib.suppress(sqlite3.Error):
            conn.close()
        return count
    # RH7: re-verify once more before unlinking so we remove OUR inode's path,
    # not a replacement swapped in during the drain. If the binding diverged,
    # leave the files in place (a small on-disk leak) rather than unlink an
    # inode we no longer own. Keep the mutable source connection open through
    # this gate and unlink: closing it first would discard the process-scoped
    # record lock immediately after the gate reasserted it.
    try:
        if _path_still_matches_locked_inode(
            path,
            lock_fd,
            reassert_sqlite_exclusion=True,
        ):
            unlinked = False
            for suffix in ("", "-wal", "-shm"):
                p = Path(str(path) + suffix)
                if p.exists():
                    try:
                        p.unlink()
                    except OSError:
                        pass
                    else:
                        unlinked = True
            if unlinked:
                # The source remains under the same SQLite exclusion and
                # possession capability until its removed directory entries
                # are durable. Releasing first can let a crash resurrect a
                # drained pathname after another process starts using it.
                _fsync_directory(path.parent)
        else:
            logger.warning(
                "z4j buffer: orphan %s pathname diverged before unlink; leaving files in place",
                path.name,
            )
    finally:
        with contextlib.suppress(sqlite3.Error):
            conn.close()
    if count:
        logger.info(
            "z4j buffer: adopted %d undelivered event(s) from orphaned buffer %s",
            count,
            path.name,
        )
    return count


def _per_process_buffer_pid(path: Path) -> int | None:
    """Return the PID encoded by a valid per-process buffer name.

    Historical default names are ``buffer-<pid>.sqlite``. Fresh-generation
    siblings add a 12- or 32-hex random tail. A broad glob alone also matches
    explicit shared names such as ``buffer-shared.sqlite``; those must never
    become automatic recovery sources.
    """
    name = path.name
    if not name.startswith("buffer-") or not name.endswith(".sqlite"):
        return False
    identity = name[len("buffer-") : -len(".sqlite")]
    pid_text, separator, generation = identity.partition("-")
    if not pid_text.isdecimal() or int(pid_text) <= 0:
        return None
    if not separator:
        return int(pid_text)
    if len(generation) not in {12, 32}:
        return None
    try:
        int(generation, 16)
    except ValueError:
        return None
    return int(pid_text)


def _is_per_process_buffer_name(path: Path) -> bool:
    """Accept only buffer names that carry a valid process identity."""
    return _per_process_buffer_pid(path) is not None


def adopt_orphaned_buffers(current: BufferStore, *, home_dir: Path) -> int:
    """Recover undelivered events left in dead peers' per-PID buffers.

    The default buffer path is ``buffer-<pid>.sqlite`` (so a web process
    and a Celery worker under the same user don't collide). The cost is
    that on restart the NEW pid opens a fresh empty file and nothing ever
    reopens the old one -- so any events buffered while the brain was
    unreachable are orphaned forever and the stale file leaks on disk
    (B8). At startup we scan sibling ``buffer-*.sqlite`` files and adopt a
    peer's buffer ONLY when we can take its lifetime ownership lock (proof
    the owner is dead), it is a regular file we own (M4), and it is stale.
    Live siblings keep their lock and are left untouched.

    1.7.1: the liveness gate is the ownership lock, not ``os.kill(pid, 0)``.
    The pid check was racy (pid reuse could make a genuinely-orphaned buffer
    look "alive" and be skipped forever, or a reused pid look dead), and an
    atomic-rename claim alone did not prevent draining + unlinking a file a
    slow-but-live owner still held open. The flock the owner holds for its
    lifetime (released by the kernel on death) settles both.

    Returns the total number of events adopted. Never raises.
    """
    if not current._automatic_recovery_enabled:
        return 0
    adopted = 0
    try:
        discovered = {
            path for path in home_dir.glob("buffer-*.sqlite") if _is_per_process_buffer_name(path)
        }
    except OSError:
        return 0
    candidates = sorted(
        (path for path in discovered if path != current.path),
        key=str,
    )
    if current._recovery_scan_cursor is not None:
        start = next(
            (
                index
                for index, path in enumerate(candidates)
                if str(path) > current._recovery_scan_cursor
            ),
            0,
        )
        candidates = candidates[start:] + candidates[:start]
    selected = candidates[:_RECOVERY_MAX_FILES_PER_SCAN]
    if selected:
        # Round-robin across bounded scans. Otherwise a permanent FOREIGN or
        # UNKNOWN prefix could consume every scan budget and starve compatible
        # sources that sort later, violating stable-run convergence.
        current._recovery_scan_cursor = str(selected[-1])
    now = time.time()
    for path in selected:
        # M4: refuse anything that is not a regular file we own (symlink /
        # foreign-uid plant) BEFORE opening it as a SQLite DB.
        if not _orphan_is_own_regular_file(path):
            continue
        # Staleness guard: skip a file younger than the adopt window. Covers
        # the tiny race between a fresh sibling creating its DB file and
        # acquiring its lifetime lock a few lines later in __init__.
        try:
            if (now - path.stat().st_mtime) < _ADOPT_MIN_AGE_S:
                continue
        except OSError:
            continue
        # Authoritative liveness: try to take the orphan's lifetime lock. A
        # live owner still holds it -> None -> skip. On success the owner is
        # provably dead and we hold the lock across the drain + unlink.
        lock_fd = _claim_dead_orphan(path)
        if lock_fd is None:
            # Where locking is UNAVAILABLE (no fcntl on Windows, or an
            # EOPNOTSUPP/ENOLCK mount) this always returns None, and we now leave
            # the file ALONE rather than adopting it on weaker evidence.
            #
            # A no-flock fallback existed here and was removed. Without a lock
            # there is no authoritative liveness proof: the fallback had to infer
            # death from a heartbeat timestamp plus a pid check, then claim the
            # file by renaming it. Every part of that inference proved reachable
            # in the wrong direction -- a stalled or clock-skewed LIVE owner reads
            # as dead, and the identity of the file being vetted could not be tied
            # to the file being claimed without primitives this design does not
            # have (an identity taken from the open handle, and a rename that
            # refuses to overwrite). The failure mode was deleting a running
            # process's undelivered events, which is worse than not recovering a
            # dead one's.
            #
            # So on such a mount an orphaned buffer is left intact on disk and
            # logged. That is the behaviour of the previous release; nobody loses
            # recovery they have today. Automatic recovery here returns with the
            # never-share redesign, which supplies the missing primitives.
            if current._locking_unavailable:
                logger.warning(
                    "z4j buffer: %s looks orphaned but this filesystem has no "
                    "working file locking, so its owner cannot be proven dead; "
                    "leaving it untouched. If that process is gone, its "
                    "undelivered events are still in the file and can be "
                    "recovered manually",
                    path.name,
                )
            continue
        # ``_claim_dead_orphan`` verifies that flock excludes a second open-file
        # description. PID visibility and lease age are not possession: they may
        # disagree across namespaces or pauses. Once this held capability exists,
        # the pid is informational only.
        try:
            adopted += _drain_orphan_into(current, path, lock_fd)
        except Exception:
            logger.warning(
                "z4j buffer: adoption of orphaned buffer %s failed",
                path.name,
                exc_info=True,
            )
        finally:
            _release_lock(lock_fd)
    return adopted


__all__ = [
    "BufferEntry",
    "BufferStore",
    "ExternalScheduleControlReservation",
    "adopt_orphaned_buffers",
]
