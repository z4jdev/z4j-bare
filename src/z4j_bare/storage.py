"""Resolve writable directories for the agent's on-disk buffer.

Service deployments often run the agent process under a low-privilege
user (``www-data``, ``nobody``, systemd ``DynamicUser=yes``) whose
``$HOME`` resolves to a directory the process cannot write to -
``/var/www``, ``/nonexistent``, or a transient ``/run/...`` mount.
The agent then crashes at startup trying to ``mkdir ~/.z4j``.

This module owns one policy: where can the buffer live, and what
order do we try.

Resolution order (1.5+):

1. ``z4j_home()`` (the canonical state directory; ``$Z4J_HOME``
   if set, else ``~/.z4j/``).
2. ``tempfile.gettempdir() / f"z4j-{uid}"`` mode 0700 - the fallback
   when ``z4j_home()`` is unwritable. Works under any low-privilege
   service user because /tmp is world-writable but our subdir is
   uid-locked.

The relocation story is centralised in ``Z4J_HOME`` (see
``z4j_core.paths``); deprecated buffer-specific overrides
``Z4J_BUFFER_PATH`` / ``Z4J_BUFFER_DIR`` hard-fail at startup.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from z4j_core.paths import buffer_root, z4j_home

logger = logging.getLogger("z4j.runtime.storage")


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


def primary_buffer_root() -> Path:
    """Return the preferred buffer directory.

    Always ``z4j_home()``. Pure function, no I/O. Caller decides
    whether the directory is actually usable via :func:`is_writable_dir`.
    """
    return z4j_home()


def ensure_buffer_root_writable() -> Path:
    """Return the first writable buffer root, creating it if needed.

    Delegates to :func:`z4j_core.paths.buffer_root`, which tries
    ``z4j_home()`` first and falls back to ``$TMPDIR/z4j-{uid}``
    when the primary is unwritable. Logs a WARNING when the
    fallback is selected so operators see the decision.

    Returns:
        Absolute path to a writable directory. Guaranteed to exist
        on return.
    """
    primary = z4j_home()
    resolved = buffer_root()
    if resolved != primary:
        logger.warning(
            "z4j buffer: %s is not writable; falling back to %s. "
            "Set Z4J_HOME to a persistent writable location to "
            "silence this warning.",
            primary,
            resolved,
        )
    return resolved


def default_buffer_path() -> Path:
    """Resolve the per-process default buffer file path.

    Combines :func:`ensure_buffer_root_writable` (which picks the
    directory) with a per-process filename so siblings on the same
    user don't collide. Used by ``Config.buffer_path`` default factory.
    """
    import os
    return ensure_buffer_root_writable() / f"buffer-{os.getpid()}.sqlite"


__all__ = [
    "default_buffer_path",
    "ensure_buffer_root_writable",
    "is_writable_dir",
    "primary_buffer_root",
]
