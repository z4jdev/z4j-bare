# Changelog

All notable changes to `z4j-bare` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
