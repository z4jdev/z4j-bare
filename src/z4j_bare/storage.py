"""Resolve writable directories for the agent's on-disk buffer.

Service deployments often run the agent process under a low-privilege
user (``www-data``, ``nobody``, systemd ``DynamicUser=yes``) whose
``$HOME`` resolves to a directory the process cannot write to -
``/var/www``, ``/nonexistent``, or a transient ``/run/...`` mount.
The agent then crashes at startup trying to ``mkdir ~/.z4j``.

This module owns the policy: where can the buffer live, and what
order do we try.

Resolution order:

1. ``Path.home() / ".z4j"`` - the historical default. Works for
   developer laptops, root services, and any service user with a
   real writable home.
2. ``tempfile.gettempdir() / f"z4j-{uid}"`` (or username on Windows)
   with mode 0700 - the fallback. Works under any low-privilege
   service user because /tmp is world-writable but our subdir is
   uid-locked. Buffer files survive across restarts on most Linux
   distros (tmpfs aside) which is the usual case anyway.

Operators can always override both with ``Z4J_BUFFER_PATH`` (read by
the framework adapters and the bare ``install_agent`` entry point).
The override is clamped to live under one of the resolved roots, so
a typo or attack like ``Z4J_BUFFER_PATH=/etc/x.sqlite`` is still
rejected.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger("z4j.agent.storage")


def _user_tag() -> str:
    """Return a short stable per-user tag for the tmp fallback dir.

    On POSIX we use the numeric uid (always defined, no PII). On
    Windows we use ``USERNAME`` (the env var Windows always sets);
    fallback to ``"default"`` for the rare case neither is available.
    """
    if hasattr(os, "getuid"):
        return str(os.getuid())
    return os.environ.get("USERNAME") or os.environ.get("USER") or "default"


def primary_buffer_root() -> Path:
    """Return ``~/.z4j`` (the preferred buffer directory).

    Pure function, no I/O. Caller decides whether the directory is
    actually usable via :func:`is_writable_dir`.
    """
    return (Path.home() / ".z4j").resolve()


def fallback_buffer_root() -> Path:
    """Return the per-uid tmp fallback directory.

    Pure function, no I/O. The directory is created on demand by
    :func:`ensure_buffer_root_writable`.
    """
    return (Path(tempfile.gettempdir()) / f"z4j-{_user_tag()}").resolve()


def buffer_roots() -> tuple[Path, ...]:
    """All directories the agent considers acceptable for buffer files.

    Used by the path-clamp in ``install.py`` to validate operator-set
    ``Z4J_BUFFER_PATH`` values: a path under any of these roots is
    accepted, anything else is rejected as a security boundary
    violation.
    """
    return (primary_buffer_root(), fallback_buffer_root())


def is_writable_dir(path: Path) -> bool:
    """Return True if ``path`` exists (or can be created) and is writable.

    Performs a real mkdir + write + delete probe rather than trusting
    ``os.access`` (which lies under setuid binaries and on some
    network filesystems). The probe file is ephemeral and uses a
    pid-suffixed name so concurrent probes from sibling processes
    don't collide.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError):
        return False
    probe = path / f".z4j-write-probe-{os.getpid()}"
    try:
        probe.touch()
        probe.unlink()
    except (OSError, PermissionError):
        return False
    return True


def ensure_buffer_root_writable() -> Path:
    """Return the first writable buffer root, creating it if needed.

    Tries :func:`primary_buffer_root` first; falls back to
    :func:`fallback_buffer_root` if the primary is unwritable. Logs
    a WARNING when the fallback is selected so operators see the
    decision in their service log.

    Returns:
        Absolute path to a writable directory. Guaranteed to exist
        on return.

    Raises:
        OSError: if both candidate roots are unwritable. The caller
        (typically :class:`BufferStore`) wraps this in a
        :class:`z4j_core.errors.BufferStorageError` with a diagnostic
        message that points at ``Z4J_BUFFER_PATH``.
    """
    primary = primary_buffer_root()
    if is_writable_dir(primary):
        return primary

    fallback = fallback_buffer_root()
    # Lock the fallback dir to the running user. Best-effort - on
    # Windows chmod is a no-op, but tempdir is already per-user there.
    if is_writable_dir(fallback):
        try:
            fallback.chmod(0o700)
        except OSError:
            pass
        logger.warning(
            "z4j buffer: HOME (%s) is not writable; falling back to %s. "
            "Set Z4J_BUFFER_PATH to a persistent writable location to "
            "silence this warning.",
            primary,
            fallback,
        )
        return fallback

    raise OSError(
        f"z4j buffer: neither {primary} nor {fallback} is writable "
        f"(running uid={_user_tag()}). Set Z4J_BUFFER_PATH to a "
        f"writable directory.",
    )


def default_buffer_path() -> Path:
    """Resolve the per-process default buffer file path.

    Combines :func:`ensure_buffer_root_writable` (which picks the
    directory) with a per-process filename so siblings on the same
    user don't collide. Used by ``Config.buffer_path`` default factory
    when the operator did not pass an explicit path.
    """
    return ensure_buffer_root_writable() / f"buffer-{os.getpid()}.sqlite"


__all__ = [
    "buffer_roots",
    "default_buffer_path",
    "ensure_buffer_root_writable",
    "fallback_buffer_root",
    "is_writable_dir",
    "primary_buffer_root",
]
