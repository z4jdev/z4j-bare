"""Filesystem watcher for ``Z4J_DEV_MODE=true``.

A tiny ``watchdog``-based observer that fires a callback when a
``tasks.py`` (or any other watched file) changes inside one of the
project's app paths. Used by engine adapters (``z4j-celery``,
``z4j-rq``, ``z4j-dramatiq``, ...) to push a registry delta to the
brain when a developer adds a new task and saves - no worker restart,
no manual /tasks refresh.

Hard requirements:

- **Off by default.** Only constructed when ``Z4J_DEV_MODE`` is
  truthy. Production agents never import ``watchdog``.
- **No host crash.** Every callback runs through ``safe_call`` and
  the observer thread is daemonised so a misbehaving FS event
  (Windows network drives, NFS, …) cannot keep the worker alive
  past its intended exit.
- **Debounced.** Saves typically fire 2-4 events in a tight
  burst (atomic-rename on Linux, attribute + write on macOS).
  We coalesce within a 250 ms window so a single Cmd-S generates
  one callback, not four.
- **Optional install.** ``import watchdog`` lives inside the
  constructor; if the dep is absent the watcher logs a warning
  and the dev-mode feature degrades to a no-op rather than
  crashing the agent at import time. Operators install with
  ``pip install z4j-bare[watcher]`` (or ``z4j[watcher]``).

The callback is sync. Async consumers wrap it themselves -
typical pattern is to put the change onto an
``asyncio.Queue`` via ``loop.call_soon_threadsafe``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path

from z4j_bare.safety import safe_call

logger = logging.getLogger("z4j.runtime.watcher")

#: Default debounce window. Matches "fast typist hits Cmd-S twice".
_DEFAULT_DEBOUNCE_MS = 250

#: Filenames we consider task-bearing. Conservative on purpose -
#: we don't want a JSON config save to trigger a registry rescan.
_DEFAULT_FILENAMES: frozenset[str] = frozenset({"tasks.py"})


class TasksFileWatcher:
    """Watch ``app_paths`` for changes to task-bearing files.

    Constructed once per agent runtime when dev-mode is on.
    Public surface:

    - :meth:`start` - spin up the background observer thread.
    - :meth:`stop`  - tear it down. Idempotent.
    - The constructor takes a ``on_change(path: Path) -> None``
      callback. Coalesced - one call per debounce window even if
      five events fired.

    Failure modes are non-fatal:

    - ``watchdog`` not installed → log + degrade to no-op.
    - One of the ``app_paths`` does not exist → log + skip that
      path. Other paths still register.
    - Callback raises → swallowed by ``safe_call``; observer
      keeps running.
    """

    __slots__ = (
        "_paths",
        "_filenames",
        "_on_change",
        "_debounce_ms",
        "_observer",
        "_pending_lock",
        "_pending",
        "_running",
    )

    def __init__(
        self,
        *,
        app_paths: Iterable[Path | str],
        on_change: Callable[[Path], None],
        filenames: Iterable[str] = _DEFAULT_FILENAMES,
        debounce_ms: int = _DEFAULT_DEBOUNCE_MS,
    ) -> None:
        self._paths: list[Path] = [Path(p) for p in app_paths]
        self._filenames: frozenset[str] = frozenset(filenames)
        self._on_change = on_change
        self._debounce_ms = max(50, int(debounce_ms))
        self._observer = None
        self._pending_lock = threading.Lock()
        self._pending: dict[Path, float] = {}
        self._running = False

    def start(self) -> bool:
        """Start the observer. Returns True if it actually came up."""
        if self._running:
            return True
        try:
            from watchdog.events import FileSystemEventHandler  # type: ignore[import-not-found]
            from watchdog.observers import Observer  # type: ignore[import-not-found]
        except ImportError:
            logger.warning(
                "z4j: Z4J_DEV_MODE is set but the 'watchdog' package "
                "is not installed; filesystem watcher disabled. "
                "Install with `pip install z4j-bare[watcher]`.",
            )
            return False

        existing = [p for p in self._paths if p.exists() and p.is_dir()]
        if not existing:
            logger.warning(
                "z4j watcher: no valid app_paths to watch; disabled",
            )
            return False

        watcher = self

        class _Handler(FileSystemEventHandler):  # type: ignore[misc, no-any-unimported]
            def on_modified(self, event: object) -> None:
                if getattr(event, "is_directory", False):
                    return
                src = getattr(event, "src_path", None)
                if not src:
                    return
                watcher._maybe_record_change(Path(str(src)))

            def on_created(self, event: object) -> None:
                self.on_modified(event)

            def on_moved(self, event: object) -> None:
                if getattr(event, "is_directory", False):
                    return
                dest = getattr(event, "dest_path", None) or getattr(
                    event, "src_path", None,
                )
                if not dest:
                    return
                watcher._maybe_record_change(Path(str(dest)))

        observer = Observer()
        for path in existing:
            try:
                observer.schedule(_Handler(), str(path), recursive=True)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j watcher: failed to schedule path %s", path,
                )
        try:
            observer.daemon = True
            observer.start()
        except Exception:  # noqa: BLE001
            logger.exception("z4j watcher: observer.start() failed")
            return False

        self._observer = observer
        self._running = True

        # Debounce flush thread - reads ``_pending`` every
        # half-window and fires the callback for any path that's
        # been quiet for the full window. Daemonised so process
        # exit doesn't wait for it.
        flush_thread = threading.Thread(
            target=self._debounce_loop,
            name="z4j-watcher-debounce",
            daemon=True,
        )
        flush_thread.start()
        logger.info(
            "z4j watcher: started, watching %d path(s) for %s",
            len(existing),
            sorted(self._filenames),
        )
        return True

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        observer = self._observer
        self._observer = None
        if observer is None:
            return
        try:
            observer.stop()
            observer.join(timeout=2.0)
        except Exception:  # noqa: BLE001
            logger.exception("z4j watcher: stop failed")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _maybe_record_change(self, path: Path) -> None:
        if path.name not in self._filenames:
            return
        with self._pending_lock:
            self._pending[path] = time.monotonic()

    def _debounce_loop(self) -> None:
        half_window = self._debounce_ms / 2000.0
        deadline_window = self._debounce_ms / 1000.0
        while self._running:
            time.sleep(half_window)
            now = time.monotonic()
            ripe: list[Path] = []
            with self._pending_lock:
                for path, ts in list(self._pending.items()):
                    if now - ts >= deadline_window:
                        ripe.append(path)
                        del self._pending[path]
            for path in ripe:
                safe_call(self._on_change, path)


__all__ = ["TasksFileWatcher"]
