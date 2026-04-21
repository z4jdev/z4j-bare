"""Preflight orchestrator-detection heuristics.

Updated for audit H2: env-var alone is no longer enough - we also
need a filesystem-anchored signal (PID 1, /.dockerenv, cgroup
marker, or operator-installed /etc/z4j-orchestrated). This closes
the shared-hosting env-var-spoofing vector.
"""

from __future__ import annotations

import pytest

from z4j_bare.orchestrator_detect import detect_orchestrator


def test_explicit_opt_in_requires_fs_marker():
    # Without a filesystem signal: env-var opt-in alone is declined.
    d = detect_orchestrator(
        pid=999, env={"Z4J_ORCHESTRATED": "1"}, fs_marker_override=None,
    )
    assert d.detected is False

    # With a filesystem marker present: opt-in is honored.
    d2 = detect_orchestrator(
        pid=999, env={"Z4J_ORCHESTRATED": "1"},
        fs_marker_override="/etc/z4j-orchestrated",
    )
    assert d2.detected is True
    assert "Z4J_ORCHESTRATED=1" in d2.signal


def test_explicit_opt_out_wins_over_pid_1():
    d = detect_orchestrator(pid=1, env={"Z4J_ORCHESTRATED": "0"})
    assert d.detected is False
    assert "opt-out" in d.signal


def test_pid_1_detects_container():
    # PID 1 is unforgeable - no filesystem marker required.
    d = detect_orchestrator(pid=1, env={})
    assert d.detected is True
    assert "pid-1" in d.signal


def test_kubernetes_env_alone_is_not_enough():
    # Audit H2: a tenant on shared hosting who can set env vars
    # must NOT be able to trigger orchestrator detection.
    d = detect_orchestrator(
        pid=999, env={"KUBERNETES_SERVICE_HOST": "10.0.0.1"},
        fs_marker_override=None,
    )
    assert d.detected is False


def test_kubernetes_env_with_fs_marker_detects():
    d = detect_orchestrator(
        pid=999, env={"KUBERNETES_SERVICE_HOST": "10.0.0.1"},
        fs_marker_override="/.dockerenv",
    )
    assert d.detected is True
    assert "kubernetes" in d.signal
    assert "/.dockerenv" in d.signal


def test_systemd_invocation_id_alone_is_not_enough():
    d = detect_orchestrator(
        pid=999, env={"INVOCATION_ID": "abc123"}, fs_marker_override=None,
    )
    assert d.detected is False


def test_systemd_with_fs_marker_detects():
    d = detect_orchestrator(
        pid=999, env={"NOTIFY_SOCKET": "/run/systemd/notify"},
        fs_marker_override="cgroup:systemd",
    )
    assert d.detected is True
    assert "systemd" in d.signal


def test_supervisord_alone_is_not_enough():
    d = detect_orchestrator(
        pid=999, env={"SUPERVISOR_ENABLED": "1"}, fs_marker_override=None,
    )
    assert d.detected is False


def test_supervisord_with_fs_marker_detects():
    d = detect_orchestrator(
        pid=999, env={"SUPERVISOR_PROCESS_NAME": "celery-worker"},
        fs_marker_override="cgroup:docker",
    )
    assert d.detected is True
    assert "supervisord" in d.signal


def test_bare_shell_returns_undetected():
    d = detect_orchestrator(pid=999, env={}, fs_marker_override=None)
    assert d.detected is False
    assert d.signal is None


@pytest.mark.parametrize("value", ["", "maybe", "unknown"])
def test_orchestrated_env_invalid_values_fall_through(value):
    d = detect_orchestrator(
        pid=999, env={"Z4J_ORCHESTRATED": value},
        fs_marker_override=None,
    )
    # Invalid values don't force either outcome; they fall through
    # to the (now strict) filesystem-anchored path which returns
    # detected=False without a signal here.
    assert d.detected is False
