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
_runtime: AgentRuntime | None = None
_owner: str | None = None


def try_register(
    runtime: AgentRuntime,
    *,
    owner: str,
) -> AgentRuntime:
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
    global _runtime, _owner  # noqa: PLW0603  module-level singleton lazy-init
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


def current_runtime() -> AgentRuntime | None:
    """Return the currently-registered runtime, or ``None``."""
    with _lock:
        return _runtime


def current_owner() -> str | None:
    """Return the label of the install path that registered the runtime."""
    with _lock:
        return _owner


def post_fork() -> AgentRuntime | None:
    """Re-establish the agent in a freshly-forked WEB worker child.

    Wire this into your WSGI/ASGI server's post-fork hook so an agent
    installed under ``--preload`` (imported once in the arbiter, then
    forked into workers) actually runs in each worker:

    - gunicorn ``gunicorn.conf.py``::

        def post_fork(server, worker):
            from z4j_bare import post_fork as z4j_post_fork

            z4j_post_fork()

    - uWSGI::

        from uwsgidecorators import postfork
        from z4j_bare import post_fork as z4j_post_fork


        @postfork
        def _z4j():
            z4j_post_fork()

    Do NOT call this from a Celery ``worker_process_init`` / prefork
    pool hook -- the agent belongs to the main worker, not its pool
    children. Returns the restarted runtime, or None if no agent is
    installed in this process. Best-effort; never raises.
    """
    runtime = current_runtime()
    if runtime is None:
        return None
    try:
        runtime.reinit_after_fork()
    except Exception:  # pragma: no cover - post-fork must never crash the worker
        logger.warning("z4j agent: post_fork re-init failed", exc_info=True)
    return runtime


def clear_runtime(expected: AgentRuntime | None = None) -> bool:
    """Forget the current registration. Returns True if it actually cleared.

    Called by clean-shutdown paths (Django ``atexit``, Celery
    ``worker_shutdown``) so a re-bootstrap inside the same process
    (rare - usually only happens in tests) can register a fresh
    runtime. Idempotent.

    When ``expected`` is given, this is a COMPARE-AND-CLEAR -- it forgets
    the registration ONLY if the currently-registered runtime IS that object. A
    failing installer A thus clears only ITS OWN registration and can never erase
    a DIFFERENT runtime B that legitimately replaced it in the meantime.
    ``expected=None`` keeps the unconditional clear for clean-shutdown paths that
    own the whole process.
    """
    global _runtime, _owner  # noqa: PLW0603  module-level singleton lazy-init
    with _lock:
        if expected is not None and _runtime is not expected:
            return False
        cleared = _runtime is not None
        _runtime = None
        _owner = None
        return cleared


__all__ = [
    "clear_runtime",
    "current_owner",
    "current_runtime",
    "post_fork",
    "try_register",
]
