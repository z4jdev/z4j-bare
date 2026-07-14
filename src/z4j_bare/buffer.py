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
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
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
"""

#: ``attempts`` is a general stuck-entry METRIC (incremented by the WS
#: ack-watchdog on a re-send, and historically by pre-1.7 transport
#: failures); it must NOT gate any drop. ``content_rejects`` is the
#: DEDICATED bounded drop budget: only a long-poll per-frame content
#: rejection (413/415/422) increments it, and only it is consulted by
#: ``evict_if_exhausted``. Keeping them physically separate stops WS-timeout
#: (or cross-version pre-1.7) ``attempts`` history from destructively priming
#: the content-drop budget so a deliverable event is dropped on its FIRST
#: content reject (R8-H1).

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


#: Module-level flag so the Z4J_HOME perms warning fires at most
#: once per process (not once per BufferStore construction).
_z4j_home_perms_warned: bool = False


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

    targets = [
        db_path,
        db_path.with_suffix(db_path.suffix + "-wal"),
        db_path.with_suffix(db_path.suffix + "-shm"),
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


class BufferStore:
    """A crash-safe local queue of outgoing frames.

    Lifecycle:

    1. Constructed with a ``path`` (a ``~/.z4j/buffer.sqlite`` by default).
       The directory is created if needed. Schema is applied.
    2. Events are appended via :meth:`append`. The id is assigned
       automatically and returned.
    3. The transport loop calls :meth:`drain` to pull the oldest N
       entries. It ships them to the brain, then calls
       :meth:`confirm` with the ids to delete them.
    4. On shutdown, :meth:`close` releases the connection. The file
       remains on disk for the next startup.

    The store uses a single ``threading.Lock`` to serialize access.
    Agent runtime code runs on a single background thread, so
    contention is expected to be near zero - the lock is defense in
    depth against accidental callers from other threads.
    """

    def __init__(
        self,
        path: Path,
        *,
        max_entries: int = 100_000,
        max_bytes: int = 256 * 1024 * 1024,
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
        self._closed = False

        # z4j-bare 1.6.5 (security advisory P1): force private mode
        # on the buffer DB + its WAL/SHM sidecar files. The buffer
        # stores task/event payload BLOBs which may contain PII; on
        # multi-tenant hosts the inherited umask is not tight enough
        # by default (e.g., 0644 with umask 022). We re-tighten to
        # owner-only after SQLite has created/touched each file.
        # Best-effort: the chmod is a no-op on platforms where it
        # doesn't apply (Windows), and we tolerate errors so an
        # ephemeral filesystem (e.g., a tmpfs mounted without
        # permission semantics) doesn't break agent startup.
        _restrict_buffer_files(path)
        # Surface a one-shot WARN if Z4J_HOME itself is group/world
        # accessible so operators on multi-tenant hosts notice.
        _warn_if_z4j_home_loose(path)

        # Cached running totals - sourced from disk on startup, then
        # adjusted incrementally on append/evict/confirm. Avoids running
        # SUM(LENGTH(payload)) on every eviction iteration.
        (self._cached_count,) = self._conn.execute(
            "SELECT COUNT(*) FROM entries",
        ).fetchone()
        (self._cached_bytes,) = self._conn.execute(
            "SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM entries",
        ).fetchone()

        # Sentinel so the drift-detected warning only fires once per
        # BufferStore lifetime. A persistent drift bug would otherwise
        # spam the logs every heartbeat.
        self._drift_warned = False

    def _migrate_schema(self) -> None:
        """Idempotently add columns absent from an OLDER buffer file.

        ``CREATE TABLE IF NOT EXISTS`` does NOT add a new column to a table
        an earlier version already created, and the SQLite buffer file
        survives process restarts / upgrades. ``content_rejects`` (R8-H1)
        must exist and default 0 so a pre-1.7 entry (which may carry a high
        ``attempts`` from old transport-failure counting) starts its content-
        drop budget fresh and is not deleted on its first content reject.
        """
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(entries)")}
        if "content_rejects" not in cols:
            self._conn.execute(
                "ALTER TABLE entries ADD COLUMN content_rejects INTEGER NOT NULL DEFAULT 0",
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

    @property
    def closed(self) -> bool:
        """True after :meth:`close` has been called."""
        return self._closed

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
            if self._closed:
                raise RuntimeError("BufferStore is closed")
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
            return new_id

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
        without taking on more un-acked event batches (R7-MED).

        Entries are ordered by ``id`` (which corresponds to insertion
        order). The caller is expected to confirm successful delivery
        via :meth:`confirm` - if confirm is never called, the entries
        remain available for a subsequent drain.

        ``exclude_ids`` skips entries currently IN FLIGHT (sent but not
        yet acked). Without it the send loop re-drains and re-sends the
        same un-acked entries on every iteration (they are never removed
        until acked), which both floods the brain and starves any entry
        beyond the drain window when in-flight entries fill it (R5-M2).

        The exclusion is applied by OVER-FETCHING ``limit + |exclude|``
        oldest rows and filtering in Python, NOT with a SQL
        ``NOT IN (...)``. A parameterized ``NOT IN`` would bind one
        variable per excluded id, and a large in-flight set (the buffer
        permits up to 100k entries) can exceed SQLite's 32766
        bound-parameter ceiling and raise ``OperationalError``, which
        would crash the send loop and (after the supervisor reconnect
        clears pending state) restore the very re-send storm this
        exclusion prevents (R6-F5). Over-fetching at most ``limit +
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
            if self._closed:
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
            if self._closed:
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
        R8-H1). Keeping this off the drop path is what makes WS-timeout (or
        pre-1.7 cross-version) history harmless.
        """
        if not ids:
            return
        with self._lock:
            if self._closed:
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
        WS-timeout history can never destructively prime a drop (R8-H1).
        """
        if not ids:
            return
        with self._lock:
            if self._closed:
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
        (R7-MED), and transient ``attempts`` history never triggers a drop
        (R8-H1). Returns the number dropped. Logs a WARNING per drop.
        """
        if not ids or max_rejects <= 0:
            return 0
        with self._lock:
            if self._closed:
                return 0
            placeholders = ",".join("?" * len(ids))
            row = self._conn.execute(
                f"SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) FROM entries WHERE content_rejects >= ? AND id IN ({placeholders})",  # noqa: S608  bound '?' params
                (max_rejects, *ids),
            ).fetchone()
            dropped_count, dropped_bytes = int(row[0]), int(row[1])
            if dropped_count == 0:
                return 0
            self._conn.execute(
                f"DELETE FROM entries WHERE content_rejects >= ? AND id IN ({placeholders})",  # noqa: S608  bound '?' params
                (max_rejects, *ids),
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
            if self._closed:
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
            if self._closed:
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

    def _evict_if_needed_locked(self, *, incoming_bytes: int) -> None:
        """Drop oldest entries until we fit the limits.

        Called while the connection lock is already held. Uses cached
        running totals (``_cached_count``, ``_cached_bytes``) so we
        never run a SUM() per iteration.
        """
        dropped = 0

        # Entry count limit - leave room for the incoming row.
        while self._cached_count >= self._max_entries:
            if not self._drop_oldest_locked():
                break
            dropped += 1

        # Byte-size limit (including the incoming entry's own size).
        while self._cached_bytes + incoming_bytes > self._max_bytes:
            if not self._drop_oldest_locked():
                break
            dropped += 1

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
            "SELECT id, LENGTH(payload) FROM entries ORDER BY id ASC LIMIT 1",
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

    def close(self) -> None:
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
        """
        with self._lock:
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
            with contextlib.suppress(sqlite3.Error):  # pragma: no cover
                self._conn.close()
            # Cleanup happens AFTER close() so we are not unlinking an
            # open file (Windows can't delete a file SQLite still
            # holds, and even on POSIX it's tidier this way).
            if count == 0:
                # WAL mode produces three files; delete all of them.
                # Failures here are non-fatal - an operator with a
                # custom umask / network mount can clean up by hand.
                for suffix in ("", "-wal", "-shm"):
                    p = Path(str(self._path) + suffix)
                    if p.exists():
                        with contextlib.suppress(OSError):
                            p.unlink()


__all__ = ["BufferEntry", "BufferStore"]
