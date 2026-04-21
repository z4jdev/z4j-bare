"""Best-effort detection of "am I running under a process supervisor?"

Used by the ``restart_worker`` self-exit path to refuse a restart
when no supervisor can be detected - otherwise the agent would
silently kill the worker process with nothing to respawn it.

Detection signals (any one → orchestrated):

- ``Z4J_ORCHESTRATED=1`` explicit opt-in (user knows their setup
  better than we do)
- ``Z4J_ORCHESTRATED=0`` explicit opt-out (forces "no supervisor")
- ``os.getpid() == 1`` - we're PID 1 inside a container; something
  started us
- ``/.dockerenv`` present - classic docker marker
- ``/proc/1/cgroup`` contains ``docker`` / ``kubepods`` /
  ``containerd`` / ``crio`` - container runtime markers
- ``KUBERNETES_SERVICE_HOST`` env var - kubernetes pod
- ``INVOCATION_ID`` or ``NOTIFY_SOCKET`` env var - systemd service
- ``SUPERVISOR_ENABLED`` / ``SUPERVISOR_PROCESS_NAME`` env vars -
  supervisord

We deliberately DON'T walk up the process tree looking for
systemd/supervisor parents: that's unreliable across platforms
and brittle. Explicit env-var opt-in + common container markers
cover 99% of real deployments; users with weird setups can set
``Z4J_ORCHESTRATED=1``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("z4j.agent.orchestrator_detect")


@dataclass(frozen=True, slots=True)
class OrchestratorDetection:
    """Result of the preflight check.

    ``signal`` is a short string naming which heuristic matched,
    useful for audit logs and operator error messages. ``None``
    when nothing matched.
    """

    detected: bool
    signal: str | None


def detect_orchestrator(
    *,
    pid: int | None = None,
    env: dict[str, str] | None = None,
    fs_marker_override: str | None = None,
) -> OrchestratorDetection:
    """Return the first-matching orchestration signal.

    Args:
        pid: Override for ``os.getpid()``. Tests inject.
        env: Override for ``os.environ``. Tests inject.
        fs_marker_override: If not None, pretend the filesystem
            probe returned this label. Tests only - production
            code uses the real filesystem probes below.
    """
    e = env if env is not None else os.environ

    # Explicit opt-out is honored immediately - lets users in weird
    # setups forcibly disable restart even if heuristics would
    # match.
    override = e.get("Z4J_ORCHESTRATED")
    if override is not None:
        normalized = override.strip().lower()
        if normalized in ("0", "false", "no"):
            return OrchestratorDetection(
                False, "Z4J_ORCHESTRATED=0 (explicit opt-out)",
            )

    # Audit H2: env-vars alone are spoofable in shared hosting
    # (a malicious tenant who can set KUBERNETES_SERVICE_HOST or
    # Z4J_ORCHESTRATED=1 in their own process env could then
    # trigger restart_worker self-exits that never respawn). We
    # require at least ONE positive signal that is NOT environment-
    # settable by a normal unprivileged user on the host:
    #
    #   - PID 1 (container init) - not forgeable
    #   - /.dockerenv present - requires access to /
    #   - /proc/1/cgroup naming a container runtime
    #
    # Plus we ALLOW env-var signals to be the deciding vote only
    # when the positive signal already matched. Z4J_ORCHESTRATED=1
    # is still an opt-in for users whose runtime matches none of
    # the above (e.g. exotic supervisors) - but in that mode they
    # also need a filesystem marker ``/etc/z4j-orchestrated`` that
    # only the installer can create. This prevents an unprivileged
    # env-var-only path.

    fs_signal: str | None = fs_marker_override
    if fs_signal is None:
        try:
            if Path("/.dockerenv").exists():
                fs_signal = "/.dockerenv"
        except OSError:
            pass
    if fs_signal is None:
        try:
            cgroup_text = Path("/proc/1/cgroup").read_text(encoding="utf-8")
        except OSError:
            cgroup_text = ""
        for token in ("docker", "kubepods", "containerd", "crio"):
            if token in cgroup_text:
                fs_signal = f"cgroup:{token}"
                break
    if fs_signal is None:
        try:
            if Path("/etc/z4j-orchestrated").exists():
                fs_signal = "/etc/z4j-orchestrated (operator marker)"
        except OSError:
            pass

    pid_value = pid if pid is not None else os.getpid()
    if pid_value == 1:
        # PID 1 is unforgeable and always means "something started
        # us" - this is the strongest signal and needs no marker.
        return OrchestratorDetection(True, "pid-1 (container init)")

    env_signal: str | None = None
    if "KUBERNETES_SERVICE_HOST" in e:
        env_signal = "kubernetes"
    elif "INVOCATION_ID" in e or "NOTIFY_SOCKET" in e:
        env_signal = "systemd"
    elif "SUPERVISOR_ENABLED" in e or "SUPERVISOR_PROCESS_NAME" in e:
        env_signal = "supervisord"

    # Require BOTH a filesystem marker AND (either an env signal or
    # the explicit opt-in). Neither on its own is enough.
    explicit_opt_in = override is not None and override.strip().lower() in (
        "1", "true", "yes",
    )

    if fs_signal is not None and (env_signal is not None or explicit_opt_in):
        combined = (
            f"{fs_signal} + {env_signal or 'Z4J_ORCHESTRATED=1'}"
        )
        return OrchestratorDetection(True, combined)

    # Explicit opt-in without a filesystem marker is declined -
    # that's the attacker-spoofable path. Tell the operator how
    # to opt in for real.
    return OrchestratorDetection(False, None)


__all__ = ["OrchestratorDetection", "detect_orchestrator"]
