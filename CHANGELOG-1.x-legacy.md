# Changelog

All notable changes to `z4j-bare` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.2] - 2026-05-01

### Added

- **`z4j_bare.declarative` module**: shared `ScheduleReconciler`,
  `ReconcileResult`, and brain-payload helpers used by every
  framework adapter's declarative reconciler (z4j-django,
  z4j-flask, z4j-fastapi). HTTP-using shared code lives here so
  z4j-core can stay pure-stdlib + Pydantic.

## [1.2.0] - 2026-04-29

### Added

- **Worker-first protocol generation in `AgentRuntime`.** Each
  agent process now generates a stable `worker_id` of the form
  `<framework>-<pid>-<unix_ms>` at startup and sends it in the
  Hello frame alongside `worker_role`, `worker_pid`, and
  `worker_started_at`. The brain registry uses this to allow
  multiple concurrent connections per agent_id (the gunicorn
  4-worker case). Resolves the agent-flap pattern that occurred
  when multiple workers under the same agent_token competed for
  the single registry slot in 1.1.x.
- **`worker_role` resolution chain**: `Z4J_WORKER_ROLE` env /
  `Config.worker_role` overrides; falls back to the framework
  adapter's `default_worker_role` (django/flask/fastapi all
  default to `web`); else None.

### Changed

- `WebSocketTransport.__init__` accepts new optional kwargs
  `worker_id`, `worker_role`, `worker_pid`, `worker_started_at`.
  Adds them to `__slots__`.
- Dependency floor `z4j-core>=1.2.0`.


## [1.1.2] - 2026-04-28

### Fixed

- **Supervisor no longer terminates the reconnect loop on
  AuthenticationError or ProtocolError.** Pre-1.1.2, both were
  treated as fatal: the loop logged "fatal" and returned, leaving
  the agent alive but offline forever until the host process was
  restarted. This was the root cause of an operator hitting
  agents-stuck-offline after a brain crash on 2026-04-28
  (tasks.jfk.work). Fix: forever-retry with separate per-error-
  class backoff schedules:
  - `ConnectionError`: 1s -> 30s
  - `ProtocolError`: 1s -> 60s
  - `AuthenticationError`: 10s -> 600s (longer cap so a real
    config issue does not hammer the brain auth endpoint; a
    transient secret-rotation window auto-recovers within 10min)
  Per-class consecutive-failure counters surfaced via the new
  `supervisor_state()` method and the `z4j-bare status` command.

### Added

- **`z4j-bare` console script.** Both `z4j-bare <subcommand>`
  (pip-installed entry point) and `python -m z4j_bare <subcommand>`
  (module form) work and dispatch to the same code path.
- **`z4j-bare check`** subcommand. Compact pass/fail health check
  (exit 0 OK, exit 1 fail) suitable for shell scripts and Nagios.
- **`z4j-bare status`** subcommand. One-line introspection: lists
  every running z4j agent on this host with PID + liveness,
  reading the pidfile registry under `$Z4J_RUNTIME_DIR` (default
  `~/.z4j/`).
- **`z4j-bare restart`** (alias `reload`). Sends SIGHUP to the
  running agent via its pidfile; the supervisor drops the current
  connection and reconnects immediately, skipping the remaining
  backoff timer. Unix-only for 1.1.2.
- **Pidfile + SIGHUP handler** wired into `AgentRuntime.start()`.
  Pidfile written to `$Z4J_RUNTIME_DIR/agent-<adapter>.pid`,
  removed on stop. Failure to write the pidfile is logged but
  does not prevent the runtime from starting.
- **`Z4J_BUFFER_DIR`** env var for explicit buffer-directory
  override (Kubernetes PV, `/var/lib/z4j` under systemd, etc.).
  Default remains `~/.z4j/`; `Z4J_BUFFER_PATH` still takes
  precedence over both.

### Internal

- `runtime.AgentRuntime.request_reconnect()`: thread-safe method
  that signals the supervisor to skip its current backoff.
- `runtime.AgentRuntime.supervisor_state()`: returns runtime state
  + per-error-class consecutive-failure counters.
- New module `z4j_bare.control` with pidfile management and
  cross-platform SIGHUP installer.

## [1.1.0] - 2026-04-28

### Security (round-9 audit)

- **Wire protocol cross-session replay closed.** Pre-fix the
  agent's transport layer reset the ``seq + nonce`` counters per
  session, which let an attacker who recorded an envelope from
  session A replay it into session B (since both sessions had
  independent counter spaces starting at the same low values).
  Fix: the HMAC envelope now binds the ``session_id`` into the
  signed payload - verifier rejects the frame if the session_id
  in the envelope doesn't match the connection's session_id.
  Wired in both ``transport/websocket.py`` and
  ``transport/longpoll.py`` so both transports get the same
  protection. Mirrored by the brain-side change in z4j-core 1.1.0
  (signer + verifier).

### Fixed

- **Critical: ``schedule.fire`` now actually fires.** Pre-1.1 the dispatcher routed every ``schedule.*`` action to ``_dispatch_scheduler``, which only handled ``enable`` / ``disable`` / ``trigger_now`` / ``delete``. The brain-side z4j-scheduler 1.1.0 emits ``schedule.fire`` on every tick - the agent always rejected it with one of two errors:
  - ``unrecognized schedule action 'schedule.fire'``
  - ``no scheduler adapter registered for None`` (a Celery WORKER agent doesn't register a SchedulerAdapter - celery-beat is a separate process)

  Result: every brain-side scheduled tick produced a ``command.failed`` audit row, and no scheduled work ever ran end-to-end. New ``_dispatch_schedule_fire`` routes ``schedule.fire`` to the QueueEngineAdapter's ``submit_task(task_name, args, kwargs, queue)`` using the payload the brain already populates. No SchedulerAdapter required. Verified live in the multi-framework docker e2e: zero ``command.failed`` rows in 60s of ticks across django/flask/fastapi after the fix. (z4j ecosystem v1.1.0 family release; brain 1.1.0 + scheduler 1.1.0 + bare 1.1.0 ship together.)

### Changed

- Bumped minimum ``z4j-core`` to ``>=1.1.0`` to align with the v1.1.0 ecosystem family release. Operators who install the 1.1.0 family get a known-good slice of brain + scheduler + agent + adapters resolved together.

## [1.0.7] - 2026-04-25

### Changed

- Promoted ``z4j_bare.storage.clamp_buffer_path`` from private to public API. Used by the framework adapters (z4j-django, z4j-flask, z4j-fastapi) to clamp operator-set ``Z4J_BUFFER_PATH`` values without re-implementing the security boundary check.

## [1.0.6] - 2026-04-24

### Added

- **Smart buffer-path fallback.** New `z4j_bare.storage` module owns the policy: try `~/.z4j` first, fall back to `$TMPDIR/z4j-{uid}` (mode 0700) when HOME is unwritable. `BufferStore.__init__` now relocates the buffer file (preserving the filename) instead of crashing with `PermissionError`. Fixes the gunicorn-under-www-data class of failure where the agent silently failed to start because `Path.home()` resolved to `/var/www`. Logs a WARNING when the fallback is selected so operators see the decision in their service log.
- **`z4j_bare.diagnostics` module** with reusable probes for `z4j-doctor` style commands: `probe_buffer_path()`, `probe_dns()`, `probe_tcp()`, `probe_tls()`, `probe_websocket()`, plus a `run_all(config)` orchestrator. Probes return structured `ProbeResult` records, never raise. Used by z4j-django's `manage.py z4j_doctor`, z4j-flask's `flask z4j-doctor`, and z4j-fastapi's CLI doctor command.

### Changed

- `_clamp_buffer_path` in `install.py` now accepts paths under either of the two allowed roots (primary HOME-based or fallback tmp-based) instead of only the HOME root. The security boundary is preserved - operator-set `Z4J_BUFFER_PATH` values still must live under one of the user-private roots.
- Bumped minimum `z4j-core` to `>=1.0.4` for the new `BufferStorageError` exception class.

### Fixed

- Agent no longer fails to start under low-privilege service users (gunicorn/www-data, uvicorn under DynamicUser, etc.). Was a regression visible since v1.0.0 but only surfaced in deployments where `Path.home()` resolved to a directory the running user could not write to.

## [1.0.5] - 2026-04-24

### Added

- **`install_agent(framework=...)` kwarg.** Accepts a `FrameworkAdapter` instance or a class. When a class is passed, it's instantiated with the resolved `Config`. Defaults to `BareFrameworkAdapter` (preserves existing behavior). Lets engine bootstrappers (e.g. z4j-celery's `worker_bootstrap`) pass through the right framework so the agent's hello frame reports `framework: django` / `flask` / `fastapi` instead of always `bare`. Without this kwarg, every agent installed via `install_agent` reported `framework: bare` in the dashboard regardless of the host stack - a real misattribution since v1.0.0.

## [1.0.4] - 2026-04-24

### Added

- **Orphan cleanup on `BufferStore.close()`.** When the buffer is empty at shutdown (the common case after a clean drain), the SQLite files (`*.sqlite`, `*.sqlite-wal`, `*.sqlite-shm`) are removed from disk. Combined with the per-process `buffer-{pid}.sqlite` default added in z4j-core 1.0.3, this prevents accumulation of stale `buffer-{old-pid}.sqlite` files across many process restarts. If the buffer is non-empty (un-drained events from a transport outage), the file is preserved so a future BufferStore at the same path could pick it up.

### Fixed

- **No more `cached counters drifted negative` warning** in multi-process deployments (web + worker sharing one user). Root cause was the shared default buffer path; fixed in z4j-core 1.0.3 by switching to per-process paths. This package's contribution is the orphan cleanup so the per-process files don't pile up.

### Changed

- Bumped minimum `z4j-core` to `>=1.0.3` to pick up the per-process buffer-path default.

## [1.0.1] - 2026-04-21

### Changed

- Lowered minimum Python version from 3.13 to 3.11. This package now supports Python 3.11, 3.12, 3.13, and 3.14.
- Documentation polish: standardized on ASCII hyphens across README, CHANGELOG, and docstrings for consistent rendering on PyPI.


## [1.0.0] - 2026-04

### Added

- First public release.
- `install_agent()` - one-call bootstrap for in-process agents (Celery / RQ / Dramatiq / Huey / arq / taskiq workers, ML pipelines, custom scripts).
- `AgentRuntime` - pure-Python supervisor, no framework hooks.
- **Transport layer** under `z4j_bare.transport`:
  - WebSocket client with exponential-backoff reconnect.
  - HTTPS long-poll fallback for networks that block WebSocket.
- **Local SQLite buffer** (`buffer.py`) - WAL mode, size-capped, crash-safe. Oldest-drop when full; never blocks the host app.
- **Command dispatcher** (`dispatcher.py`) - routes inbound brain commands to registered engine + scheduler adapters, including the reconciliation handler.
- **Heartbeat loop** (`heartbeat.py`) - liveness + last-seen stamping.
- **Orchestrator detection** (`orchestrator_detect.py`) - identifies Docker / Kubernetes / systemd / supervisord hosts; used as a preflight gate before `restart_worker` runs `os._exit(0)` for non-Celery engines.
- **Process singleton** (`_process_singleton.py`) - prevents duplicate agents from the same project on one host.
- **Safety net** (`safety.py`) - top-level try/except wrappers on every entry point so agent failures never propagate to the host framework.
- **CLI** - `python -m z4j_bare run ...` for standalone workers.
- Optional `[watcher]` extra pulling in `watchdog` for dev-mode tasks-file reloads.
- 61 unit tests.

### Reliability guarantees

- Zero exceptions propagate from the agent to Celery / Django / Flask / FastAPI signal handlers.
- Network I/O is never done from a signal handler - events queue in-memory and flush on a background thread.
- Brain unreachable → events buffer to local SQLite with backoff; host app never blocks.

## Links

- Repository: <https://github.com/z4jdev/z4j-bare>
- Issues: <https://github.com/z4jdev/z4j-bare/issues>
- PyPI: <https://pypi.org/project/z4j-bare/>

[Unreleased]: https://github.com/z4jdev/z4j-bare/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/z4jdev/z4j-bare/releases/tag/v1.0.0
