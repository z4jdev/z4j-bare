# Changelog

## 1.6.5 (2026-05-16)

### Security

* **Buffer SQLite file permissions tightened** (audit P1).
  Pre-1.6.5 the per-process buffer DB inherited the operator's
  umask when SQLite created the file. On multi-tenant POSIX
  hosts with default umask 022 this produced a world-readable
  buffer containing task and event payload bytes (potentially
  PII).

  1.6.5 forces owner-only permissions (0600) on the buffer DB
  AND its WAL/SHM sidecar files immediately after SQLite
  creates them. Best-effort: chmod failures are logged at WARN
  but do not block agent startup. No-op on Windows.

  Also: if the buffer's parent directory (`Z4J_HOME`) itself is
  group/world accessible, the agent logs a one-shot WARN at
  startup naming the directory and the remediation command.

In-place upgrade; existing buffer files become private on the
next agent restart.

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: framework-free agent runtime for plain scripts and Celery / RQ / Dramatiq workers.
