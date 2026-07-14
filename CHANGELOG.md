# Changelog

## 1.7.0 (2026-07-11)

* Orchestrator-detection preflight is now hermetic under test (a `probe_filesystem` switch skips the real filesystem probes), with the filesystem-marker cascade extracted into a helper.
* **Fixed: the long-poll transport could never pass frame verification with a slug-configured project.** The config's `project_id` is a slug, and the transport silently coerced it to a random UUID before binding it into the frame-HMAC envelope, while the brain binds the real project UUID, so every frame failed signature verification in both directions. The transport now binds the canonical UUIDs the brain advertises on the long-poll responses (`X-Z4J-Agent-Id` / `X-Z4J-Project-Id`); against an older brain it falls back to UUID-shaped config values and otherwise logs a clear warning instead of failing silently.
* Fixed: the bare-Python autostart path (`install_agent`) never registered a shutdown hook, so agents got no ordered drain or outbound-buffer flush at interpreter exit (silent tail-event loss). It now registers the same atexit teardown the framework adapters use.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

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
