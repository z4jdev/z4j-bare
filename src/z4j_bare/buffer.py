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

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("z4j.agent.buffer")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS entries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT    NOT NULL,
    payload     BLOB    NOT NULL,
    created_at  REAL    NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_entries_created_at
    ON entries (created_at);
"""

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
        self._path = path
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._lock = threading.Lock()

        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(path),
            isolation_level=None,  # autocommit
            check_same_thread=False,
            timeout=5.0,
        )
        for pragma in _PRAGMAS:
            self._conn.execute(pragma)
        self._conn.executescript(_SCHEMA)
        self._closed = False

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
                "INSERT INTO entries (kind, payload, created_at, attempts) "
                "VALUES (?, ?, ?, 0)",
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

    def drain(self, limit: int) -> list[BufferEntry]:
        """Return the oldest ``limit`` entries without removing them.

        Entries are ordered by ``id`` (which corresponds to insertion
        order). The caller is expected to confirm successful delivery
        via :meth:`confirm` - if confirm is never called, the entries
        remain available for a subsequent drain.

        Args:
            limit: Maximum number of entries to return. Must be > 0.
        """
        if limit <= 0:
            raise ValueError("drain limit must be positive")
        if self._closed:
            return []
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, kind, payload, created_at, attempts "
                "FROM entries ORDER BY id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            BufferEntry(id=r[0], kind=r[1], payload=r[2], created_at=r[3], attempts=r[4])
            for r in rows
        ]

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
                f"SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) "
                f"FROM entries WHERE id IN ({placeholders})",
                ids,
            ).fetchone()
            removed_count, removed_bytes = int(row[0]), int(row[1])
            self._conn.execute(
                f"DELETE FROM entries WHERE id IN ({placeholders})",
                ids,
            )
            self._cached_count -= removed_count
            self._cached_bytes -= removed_bytes

    def increment_attempts(self, ids: list[int]) -> None:
        """Increment the attempts counter for a batch of entries.

        Called by the transport when a flush attempt fails and the
        entries are going to be retried later. Used by metrics to
        surface entries that are stuck.
        """
        if not ids:
            return
        with self._lock:
            if self._closed:
                return
            placeholders = ",".join("?" * len(ids))
            self._conn.execute(
                f"UPDATE entries SET attempts = attempts + 1 "
                f"WHERE id IN ({placeholders})",
                ids,
            )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def size(self) -> int:
        """Number of entries currently in the buffer.

        Always ``>= 0``. If the cached counter has drifted below
        zero (a real bug we have seen in the live stack - see
        audit "live stack" 2026-04-21) we reconcile from disk,
        log once, and return the reconciled value. The heartbeat
        frame's ``buffer_size`` field is Pydantic-validated
        ``ge=0``; without this clamp a drifted counter would
        crash the heartbeat loop and force agent reconnect
        churn.
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
            try:
                self._conn.close()
            except sqlite3.Error:  # pragma: no cover
                pass
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
                        try:
                            p.unlink()
                        except OSError:
                            pass


__all__ = ["BufferEntry", "BufferStore"]
