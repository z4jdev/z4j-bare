# Changelog

## 1.12.0 (2026-10-03)

* Dispatch the `dlq.list` command onto `adapter.list_dead_letters` and
  serialise the returned page into the command result. The dispatcher
  refuses the action fail-closed unless the adapter advertises the
  `list_dead_letters` capability, and the capability gate runs before any
  parameter is read; an adapter that advertises the capability but lacks
  the method is refused the same way. `queue` must be a string or null,
  `limit` a positive integer (clamped to 200, default 100), and `cursor` a
  string or null; anything else fails the command with a named error
  without reaching the adapter.
* An agent the brain refuses for its standing no longer reconnects on the
  fast schedule. The long-poll transport treats a `403` whose body says
  `project_inactive` or `ip_denied` as an authentication failure on the
  connect probe, the events POST and the command poll (any other `403` stays
  transient), and the WebSocket transport classifies a close that lands on
  the hello send with the same table as one that lands on the hello_ack
  receive, so a brain that closes 4401, 4403 or a terminal code before
  reading the hello no longer surfaces as a bare connection error. The
  supervisor's auth WARNING now carries the close code or HTTP status, the
  brain's error code and its reason text, so an address denial or an
  archived project is not read as a token to rotate.

## 1.11.0 (2026-09-10)

* Count buffer telemetry loss durably. Capacity eviction and content
  rejection now record what they discard (frames by reason, event records in
  readable batches, command results, other and unclassified frames) in the
  buffer file, in the same SQLite savepoint as the deletion, so a rolled-back
  deletion reports no loss and a normal acknowledgement never counts as loss.
  Content rejection covers an event batch the brain keeps rejecting, an
  isolated control frame the brain rejects, and a frame the transport cannot
  parse, that is not a signed frame type, or that exceeds the brain's frame
  limit. `BufferStore.discard()` removes such undelivered entries with this
  accounting, and `BufferStore.loss_snapshot()` returns the counters. They
  are stored in the buffer file, and an agent creates a new buffer file, with
  a new identity, each time it starts, so they start at zero after a restart.
* Heartbeat and agent-status frames now carry these counters as
  `telemetry_loss`, with the runtime identity and the in-memory event-loss
  count of each engine adapter that exposes `dropped_event_count` (Celery and
  RQ). Loss is still reported when the broker-health provider fails. The
  heartbeat's `dropped_events` total now adds buffer event records and adapter
  event loss (capped at 10,000,000); before, it counted only
  `record_dropped()` calls, which no z4j package made, so it normally read 0.
* `Heartbeat.record_dropped()` now raises `ValueError` for a bool, a non-int
  or a negative count instead of adding it. `Heartbeat` also accepts optional
  `engines` and `runtime_id` arguments.
* Align runtime version metadata and sibling dependency floors with the coordinated 1.11.0 release.


## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* Carried with the coordinated fleet release. No adapter behaviour changed.

## 1.9.0 (2026-08-25)

* An agent rejected by the brain no longer reconnects forever: an unsupported protocol or version now backs off on its own schedule and says what to upgrade, instead of retrying on the transient schedule.
* Buffer and websocket transport hardening around that path.

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
