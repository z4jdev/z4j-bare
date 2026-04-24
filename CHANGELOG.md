# Changelog

All notable changes to `z4j-bare` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
