"""Out-of-band agent control: pidfile + SIGHUP-driven reconnect.

The agent normally lives inside another host process (Django,
Flask, FastAPI, a Celery worker, a bare Python script). When an
operator wants to force the agent to drop its current connection
and reconnect immediately - skipping the supervisor's exponential
backoff timer - they need a way to reach into the running process
without restarting it.

The Unix idiom is **pidfile + SIGHUP**:

1. On startup, the agent writes its PID to ``$Z4J_HOME/agent-<id>.pid``.
2. The agent's main thread installs a ``SIGHUP`` handler that calls
   :meth:`AgentRuntime.request_reconnect`.
3. The CLI command ``z4j-<adapter> restart`` reads the pidfile and
   sends ``SIGHUP`` to that PID.
4. The supervisor's ``await asyncio.wait`` on the stop / reconnect
   events early-returns; the next iteration attempts a fresh
   connection without waiting for the rest of the backoff.

Same pattern nginx, postfix, sshd use for ``reload``. No new
sockets, no new ports, no IPC framework dependency.

Windows fallback (post-1.1.2): named pipe IPC. For 1.1.2 the
restart command on Windows logs a clear "not yet supported on
Windows; restart your host process via your supervisor" message
rather than silently failing.

Pidfile lives at ``$Z4J_HOME/agent-<adapter_id>.pid``, defaulting
to ``~/.z4j/`` (NOT ``$TMPDIR``: the tmpfs case of ``/tmp``
losing files on reboot is exactly what we want to avoid for an
agent's own state). One pidfile per adapter id (django, flask,
fastapi, bare, celery-worker, etc.) so multiple z4j agents in
the same process don't collide; one pidfile per process inside
a given adapter via ``-<pid>`` suffix when the process supervisor
allows multiple replicas.

Path overrides are centralised on ``Z4J_HOME``; setting the
deprecated ``Z4J_RUNTIME_DIR`` hard-fails at startup (see
``z4j_core.paths.reject_deprecated_path_env``).
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from z4j_core.paths import ensure_z4j_home, z4j_home

if TYPE_CHECKING:
    from z4j_bare.runtime import AgentRuntime

logger = logging.getLogger("z4j.runtime.control")


def _runtime_dir() -> Path:
    """Return the directory where pidfiles live.

    Always ``z4j_home()`` (defaulting to ``~/.z4j/``). Operators
    who want to relocate state set ``Z4J_HOME``; that variable
    moves every state file (brain DB, secrets, PKI, allowed-hosts,
    agent pidfiles, agent buffers) to one directory.
    """
    return ensure_z4j_home()


def pidfile_path(adapter_id: str) -> Path:
    """Path to the pidfile for ``adapter_id``.

    ``adapter_id`` is the framework or engine label: ``django``,
    ``flask``, ``fastapi``, ``bare``, ``celery``, etc. The CLI
    command for each adapter passes its own id so ``z4j-django
    restart`` only signals the django agent in this process tree,
    not the celery worker that happens to be on the same host.
    """
    return _runtime_dir() / f"agent-{adapter_id}.pid"


def write_pidfile(adapter_id: str) -> Path:
    """Write the current PID to ``pidfile_path(adapter_id)``.

    Atomic write: write to ``<path>.tmp`` then rename. Concurrent
    runs in the same adapter on the same host (very rare - the
    runtime is single-instance per process and the framework
    adapters all write to ``adapter_id`` not ``adapter_id-pid``)
    will see consistent contents at any instant.
    """
    target = pidfile_path(adapter_id)
    tmp = target.with_suffix(".pid.tmp")
    tmp.write_text(f"{os.getpid()}\n", encoding="utf-8")
    os.replace(tmp, target)
    return target


def remove_pidfile(adapter_id: str) -> None:
    """Best-effort pidfile cleanup. Never raises."""
    try:
        pidfile_path(adapter_id).unlink(missing_ok=True)
    except OSError:
        pass


def install_sighup_handler(runtime: "AgentRuntime") -> bool:
    """Wire ``SIGHUP`` to ``runtime.request_reconnect``.

    Returns ``True`` if the handler was installed, ``False`` on
    Windows (no SIGHUP) or if installing the signal handler fails
    (e.g. running off the main thread - signal handlers must be
    installed from the main thread).

    The handler is *additive*: if a previous handler was installed
    by the host (gunicorn, uvicorn, supervisord, the framework
    itself), we wrap it so both run. Replacing it would break the
    host's own reload semantics.
    """
    if sys.platform == "win32" or not hasattr(signal, "SIGHUP"):
        logger.debug(
            "z4j agent: SIGHUP handler not installed "
            "(unsupported on this platform)",
        )
        return False
    try:
        previous = signal.getsignal(signal.SIGHUP)
    except (ValueError, OSError):
        previous = None

    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        logger.info("z4j agent: SIGHUP received, requesting reconnect")
        runtime.request_reconnect()
        # Chain to any prior handler (gunicorn's graceful-reload,
        # supervisord's reload, etc.). Default + Ignore are no-ops.
        if callable(previous) and previous not in (
            signal.SIG_DFL,
            signal.SIG_IGN,
        ):
            try:
                previous(signum, frame)  # type: ignore[misc]
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j agent: chained SIGHUP handler raised",
                )

    try:
        signal.signal(signal.SIGHUP, _handler)
    except (ValueError, OSError) as exc:
        logger.warning(
            "z4j agent: SIGHUP handler install failed: %s "
            "(will not install; restart via host supervisor)",
            exc,
        )
        return False
    return True


def send_restart(adapter_id: str) -> tuple[int, str]:
    """Find the agent's pidfile + send SIGHUP. Used by the CLI.

    Returns ``(exit_code, message)``. ``exit_code`` is ``0`` on
    success, ``1`` on operator-fixable failure (no pidfile, stale
    pid), ``2`` on platform / config issue.
    """
    if sys.platform == "win32" or not hasattr(signal, "SIGHUP"):
        return (
            2,
            "z4j-<adapter> restart is not yet supported on Windows. "
            "Restart your host process via your supervisor "
            "(systemd, supervisord, gunicorn --reload, etc.).",
        )
    pf = pidfile_path(adapter_id)
    if not pf.exists():
        return (
            1,
            f"no pidfile at {pf}. Either the agent is not running, "
            f"or it ran but did not register a pidfile (older z4j "
            f"version, or a different Z4J_HOME). Restart "
            f"your host process to bring the agent up fresh.",
        )
    try:
        pid_text = pf.read_text(encoding="utf-8").strip()
        pid = int(pid_text)
    except (OSError, ValueError) as exc:
        return (
            1,
            f"pidfile at {pf} unreadable: {exc}",
        )
    try:
        os.kill(pid, signal.SIGHUP)
    except ProcessLookupError:
        return (
            1,
            f"pid {pid} from {pf} is not running. Removing the "
            f"stale pidfile; restart your host process.",
        )
    except PermissionError:
        return (
            1,
            f"insufficient permission to signal pid {pid}. "
            f"Run as the same user as the agent process.",
        )
    return (
        0,
        f"SIGHUP sent to z4j-{adapter_id} agent (pid {pid}). "
        f"Supervisor will skip remaining backoff and reconnect.",
    )


__all__ = [
    "install_sighup_handler",
    "pidfile_path",
    "remove_pidfile",
    "send_restart",
    "write_pidfile",
]
