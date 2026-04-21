# z4j-bare

[![PyPI version](https://img.shields.io/pypi/v/z4j-bare.svg)](https://pypi.org/project/z4j-bare/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-bare.svg)](https://pypi.org/project/z4j-bare/)
[![License](https://img.shields.io/pypi/l/z4j-bare.svg)](https://github.com/z4jdev/z4j-bare/blob/main/LICENSE)


**License:** Apache 2.0
**Status:** v1.0.0 - first public release.

Framework-free agent runtime for [z4j](https://z4j.com). Ships both
as a pure-Python supervisor for standalone workers (no Django / Flask /
FastAPI) and as proof that [`z4j-core`](https://github.com/z4jdev/z4j-core)
is genuinely framework-free - `z4j-bare` is what every framework
adapter is built on top of.

## Install

```bash
pip install z4j-bare

# Optionally add dev-mode filesystem watcher (for live task-registry
# reloads when tasks.py changes on disk; off in production):
pip install "z4j-bare[watcher]"
```

## What it does

- **`AgentRuntime`** - pure-Python agent runtime, no framework hooks.
- **WebSocket transport** with exponential-backoff reconnect.
- **HTTPS long-poll fallback** for networks that block WebSocket.
- **Local SQLite buffer** (WAL mode, size-capped, crash-safe event storage).
- **Command dispatcher** - routes brain commands to registered engine + scheduler adapters.
- **Heartbeat loop** with liveness + last-seen stamping.
- **Orchestrator detection** - identifies Docker / Kubernetes / systemd / supervisord hosts for `restart_worker` safety-gating.
- **Per-project HMAC v2 signing** using the shared wire-protocol helpers from `z4j-core`.
- **Process singleton guard** - prevents duplicate agents from the same project on one host.
- **CLI**: `python -m z4j_bare run --config ...`

## Use cases

- Celery / RQ / Dramatiq workers not served by a web framework.
- ML pipelines that run Celery but don't use Django.
- Containers where the agent runs as its own process, separate from the app.
- Custom Python scripts that dispatch tasks and want to report lifecycle events to the z4j brain.

## Usage - in-process

```python
from celery import Celery
from z4j_bare import install_agent
from z4j_celery import CeleryEngineAdapter

app = Celery("myproject", broker="redis://localhost/0")

agent = install_agent(
    engines=[CeleryEngineAdapter(celery_app=app)],
    brain_url="https://z4j.internal",
    token="z4j_agent_...",
    project_id="ml-pipeline-prod",
)
```

## Usage - standalone process

```bash
python -m z4j_bare run \
    --brain-url https://z4j.internal \
    --token $Z4J_TOKEN \
    --project-id ml-pipeline-prod \
    --engine celery \
    --celery-app myproject.celery:app
```

## Reliability contract

Per the z4j safety rule (`docs/CLAUDE.md §2.2`), the agent **must never**
break the host application:

- No exception inside the runtime propagates to Celery / Django / FastAPI signal handlers.
- Network I/O never blocks signal handlers (events queue to an in-memory ring, flushed from a background thread).
- When the brain is unreachable, events buffer to local SQLite with exponential backoff.
- Buffer drops oldest events when full; the host app is never slowed or blocked.

## Documentation

- [Adapter guide](https://z4j.dev/adapters) - how z4j-bare composes with engine + framework adapters.
- [Architecture](https://z4j.dev/architecture) - agent/brain topology, transport layer, reconciliation.
- [Security](https://z4j.dev/security) - threat model, HMAC v2 envelope signing, redaction.

## License

Apache 2.0. See [LICENSE](LICENSE). `z4j-bare` installs cleanly into
proprietary Python workers - there is no copyleft obligation. The
brain server it talks to ([z4j-brain](https://github.com/z4jdev/z4j-brain))
is AGPL-3.0; communicating with it over the network does not subject
your worker code to AGPL.

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Issues: <https://github.com/z4jdev/z4j-bare/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
