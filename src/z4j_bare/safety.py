"""Exception-safety wrappers.

The single most important invariant in z4j: **the agent must never
break the host application.** See ``docs/CLAUDE.md §2.2`` and
``docs/SECURITY.md``.

Every public entry point of the agent runtime that can be called from
an engine signal/middleware/hook (Celery signals, RQ Job hooks,
Dramatiq middleware), a framework lifecycle hook (Django AppConfig,
Flask before-request, FastAPI lifespan), or the host app's request
path runs through one of the helpers in this module. If an exception
escapes our own code, we log it and drop it. The host app continues.

This is defense in depth, not defense in "oh the test suite caught
most bugs." We trap unconditionally at the boundary.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger("z4j.runtime.safety")


def safe_call(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R | None:
    """Run ``func(*args, **kwargs)`` inside a top-level exception trap.

    Returns the function's return value on success, ``None`` on any
    unhandled exception. Logs the exception at ``ERROR`` level with
    traceback.

    Never re-raises. Never propagates. This is the final firewall
    between z4j code and the host application. A bug here does not
    crash the user's worker, web process, or async app - regardless
    of which engine or framework they use.
    """
    try:
        return func(*args, **kwargs)
    except (SystemExit, KeyboardInterrupt):
        # Process lifecycle signals must propagate - swallowing
        # them prevents clean shutdown (SIGTERM → SystemExit) and
        # blocks Ctrl-C in dev. See audit v2 finding #3.
        raise
    except BaseException as exc:  # noqa: BLE001
        _log_suppressed(func, exc)
        return None


def safe_boundary(func: Callable[P, R]) -> Callable[P, R | None]:
    """Decorator form of :func:`safe_call`.

    Use on any function that is called from the host framework's
    signal handlers, lifecycle hooks, or request path - in particular
    an engine's pre-run / post-run / failure hooks (e.g. Celery
    ``task_prerun`` signals, Dramatiq ``before_process_message``
    middleware, RQ Job ``success_callback`` / ``failure_callback``).

    Example::

        from z4j_bare.safety import safe_boundary

        @safe_boundary
        def handle_task_prerun(sender, task_id, task, **kwargs):
            runtime.record_event(Event(...))

    The decorated function swallows every exception - including
    ``KeyboardInterrupt`` and ``SystemExit`` - because host signal
    handlers MUST return cleanly. Our tests ensure this is the only
    place in z4j that traps ``BaseException``.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
        return safe_call(func, *args, **kwargs)

    return wrapper


def _log_suppressed(func: Callable[..., Any], exc: BaseException) -> None:
    """Log a suppressed boundary exception without leaking details to stdout.

    The entire z4j logger tree is silent by default - users who want
    to see these errors configure logging on the ``z4j`` namespace.
    We log the function qualname and the exception class only - never
    ``str(exc)`` or args, because the exception may have been raised
    by host code that put a secret into its message
    (``raise RuntimeError(f"bad token {token}")``). The full traceback
    is attached only via ``logger.exception``'s ``exc_info=True``,
    which routes through the user's configured handlers - not through
    a printed-once-and-forever line.
    """
    name = getattr(func, "__qualname__", getattr(func, "__name__", repr(func)))
    exc_type = type(exc).__name__
    try:
        logger.error(
            "z4j agent boundary suppressed exception in %s: %s",
            name,
            exc_type,
            exc_info=True,
        )
    except Exception:  # noqa: BLE001
        # If even logging fails (e.g. a broken logging configuration in
        # the host app), fall back to writing to stderr directly. We
        # still do not include the exception message - only the type.
        try:
            sys.stderr.write(
                f"z4j agent boundary suppressed exception in {name}: "
                f"{exc_type}\n",
            )
        except Exception:  # noqa: BLE001
            pass


# Stdlib ``logging.LogRecord`` reserved attribute names. If any of
# these appears as a key in a dict passed to ``logger.X(extra=...)``
# the stdlib logging module raises ``KeyError`` ("Attempt to overwrite
# %r in LogRecord"). Engine callbacks (Celery signal kwargs,
# Dramatiq middleware **options, RQ Job callback kwargs) carry shapes
# that vary by engine and engine version; passing those straight into
# ``extra=`` is a latent ``KeyError`` bomb that fires only under
# specific arrival timing.
_LOGRECORD_RESERVED: frozenset[str] = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "asctime", "taskName",
    },
)


def safe_log_extra(extra: dict[str, object]) -> dict[str, object]:
    """Return a copy of ``extra`` with stdlib ``LogRecord`` reserved keys renamed.

    Use this when you want to forward a dict whose shape comes from
    a third-party callback (engine callback kwargs, broker event
    payload, framework hook payload, ...) into
    ``logger.X("...", extra=safe_log_extra(payload))``. The helper
    prefixes any reserved key with ``user_`` so the log record
    survives without losing the value::

        logger.info("task done", extra=safe_log_extra(callback_kwargs))

    Without this guard, an engine release that adds a ``name`` kwarg
    (or a custom hook sender that injects ``message``) crashes the
    logging call - which then propagates out of ``_dispatch`` and
    triggers the broker's redeliver loop, multiplying the failure.
    """
    if not extra:
        return {}
    safe: dict[str, object] = {}
    for key, value in extra.items():
        if key in _LOGRECORD_RESERVED:
            safe[f"user_{key}"] = value
        else:
            safe[key] = value
    return safe


__all__ = ["safe_boundary", "safe_call", "safe_log_extra"]
