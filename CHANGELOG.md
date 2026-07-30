# Changelog

## 1.8.0 (2026-07-23)

* Retry authority is now derived from each loaded adapter and advertised on the exact WebSocket generation or long-poll request; an old adapter paired with a current runtime fails closed.
* Agent events buffered during an outage are now recovered on restart (orphaned per-PID buffers are adopted) instead of silently lost; adoption is gated by a process-lifetime flock ownership lock so a live sibling's buffer is never drained or unlinked.
* Buffer ownership is now a fresh-sink possession capability bound to one process generation. Same-deployment `SEALED_READY` sources and recognized pre-1.8 per-process buffers recover automatically; only PID-validated per-process filenames are swept, explicit shared paths and all uncertain sources are preserved, and bounded scans never evict live events.
* POSIX buffer directories should be owner-controlled and not group/world writable. Where that invariant cannot be proven (including WSL DrvFS), buffering remains operational in a logged owner-private temporary root and recovery of the requested directory is disabled. Rollback with undelivered 1.8 buffers requires the documented quiesce-and-recovery procedure.
* Fork-safety: a `post_fork` hook revives the agent under gunicorn / uWSGI `--preload`, forked children get their own buffer, and offload pools reset in the child.
* Hardened the predictable `/tmp` buffer fallback and applied the low-tier sweep fixes (B18/B21/B23/B25/B27).
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

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
