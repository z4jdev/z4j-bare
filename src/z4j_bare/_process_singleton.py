"""Process-wide registry of the running :class:`AgentRuntime`.

z4j has two independent install paths that can fire in the same
Python process when a Django app uses Celery:

1. ``z4j_django.apps.Z4JDjangoConfig.ready()`` - runs when Django
   loads ``INSTALLED_APPS`` (which happens for *both* the web
   process and the ``celery worker`` process, because Celery imports
   the Django app to find tasks).
2. ``z4j_celery.worker_bootstrap._on_worker_init()`` - runs from
   Celery's ``worker_init`` signal, designed for the FastAPI / Flask
   / bare-Python case where there is no AppConfig hook to piggy-back
   on.

Without coordination, both fire in a Django+Celery worker process,
each builds its own :class:`AgentRuntime`, each opens its own
WebSocket to the brain. The brain receives two registrations for
the same agent token, treats the second as a takeover, and the
agent ends up showing as ``OFFLINE`` even though heartbeats are
arriving.

This module is the shared coordination point. Both paths call
:func:`try_register` before constructing a runtime; whichever
arrives first wins, the loser logs a one-line skip and returns
the winner so the caller can hold a reference.

The registry lives in ``z4j_bare`` because it sits below both
``z4j_django`` and ``z4j_celery`` in the dependency graph - using
either of those packages as the singleton owner would create a
circular import.

Thread safety: a single :class:`threading.Lock` serialises the
two atomic operations (register + clear). All callers are
expected to be import-time / signal-handler code that runs at
process startup, so contention is near zero - the lock is
defence in depth.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from z4j_bare.runtime import AgentRuntime

logger = logging.getLogger("z4j.runtime.singleton")

_lock = threading.Lock()
_runtime: "AgentRuntime | None" = None
_owner: str | None = None


def try_register(
    runtime: "AgentRuntime", *, owner: str,
) -> "AgentRuntime":
    """Atomically register ``runtime`` as the process singleton.

    If no runtime is registered, ``runtime`` becomes the active
    one and is returned. If another runtime is already registered,
    the new ``runtime`` is **discarded** (the caller is expected to
    drop its reference) and the existing one is returned with an
    INFO log line naming both owners so the duplicate-install path
    is visible in operator logs.

    Args:
        runtime: A constructed-but-possibly-not-started runtime.
        owner: Short label of the install path (``"django.apps"``,
            ``"celery.worker_init"``, ``"install_agent"``, ...).
            Surfaced in the skip-log so operators can see which
            paths collided.

    Returns:
        The active runtime - either the one just registered or the
        existing one. Callers should always use the returned value;
        their ``runtime`` argument may have been discarded.
    """
    global _runtime, _owner
    with _lock:
        if _runtime is None:
            _runtime = runtime
            _owner = owner
            return runtime
        existing_owner = _owner
        logger.info(
            "z4j agent runtime already installed by %r; %r install "
            "path skipped to avoid a second WebSocket session for "
            "the same agent token. Returning the existing runtime.",
            existing_owner,
            owner,
        )
        return _runtime


def current_runtime() -> "AgentRuntime | None":
    """Return the currently-registered runtime, or ``None``."""
    with _lock:
        return _runtime


def current_owner() -> str | None:
    """Return the label of the install path that registered the runtime."""
    with _lock:
        return _owner


def clear_runtime() -> None:
    """Forget the current registration.

    Called by clean-shutdown paths (Django ``atexit``, Celery
    ``worker_shutdown``) so a re-bootstrap inside the same process
    (rare - usually only happens in tests) can register a fresh
    runtime. Idempotent.
    """
    global _runtime, _owner
    with _lock:
        _runtime = None
        _owner = None


__all__ = [
    "clear_runtime",
    "current_owner",
    "current_runtime",
    "try_register",
]
