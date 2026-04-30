# z4j-bare

[![PyPI version](https://img.shields.io/pypi/v/z4j-bare.svg)](https://pypi.org/project/z4j-bare/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-bare.svg)](https://pypi.org/project/z4j-bare/)
[![License](https://img.shields.io/pypi/l/z4j-bare.svg)](https://github.com/z4jdev/z4j-bare/blob/main/LICENSE)

The framework-free agent runtime for [z4j](https://z4j.com).

Bootstraps a z4j agent inside any Python process — Celery worker,
RQ worker, Dramatiq actor, plain script, custom service — and connects
it to the z4j brain over an authenticated WebSocket. Used directly when
there's no framework adapter (z4j-django / z4j-flask / z4j-fastapi);
used indirectly by those framework adapters too.

## What it ships

- **Agent runtime** — connect, authenticate, supervise, reconnect
  with bounded backoff
- **Outbound buffer** — every event written to a local SQLite ring
  before going out on the wire; durable across short brain outages
  and agent restarts
- **Engine signal hooks** — wired up by whichever engine adapter you
  install (z4j-celery, z4j-rq, etc.); the runtime drains them into
  the buffer
- **Schedule inventory** — emits a full snapshot at boot, on a
  periodic timer (default 15 min), and on demand from the brain's
  *Sync now* command. Existing schedules show up automatically.
- **Command dispatcher** — receives operator actions from the brain
  (retry, cancel, restart, schedule.fire, schedule.resync, etc.)
  and routes them to the right adapter

## Install

```bash
pip install z4j-bare
```

Most users install a framework adapter (z4j-django / z4j-flask /
z4j-fastapi) which pulls z4j-bare automatically.

## Quick start (framework-free worker)

```python
from z4j_bare import install_agent
from z4j_celery import CeleryEngineAdapter

install_agent(
    engines=[CeleryEngineAdapter(celery_app=app)],
    brain_url="https://brain.example.com",
    token="z4j_agent_...",
    project_id="my-project",
)
```

## Reliability

- No exception from the agent ever propagates back into your worker /
  signal handler / request path. Every brain interaction is wrapped in
  a top-level try/except.
- Events buffer locally when the brain is unreachable. Workers never
  block on network I/O.
- Supervisor reconnects on every transient failure (network, TLS,
  protocol mismatch) with bounded backoff.

## Documentation

Full docs at [z4j.dev/frameworks/bare/](https://z4j.dev/frameworks/bare/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-bare/
- Issues: https://github.com/z4jdev/z4j-bare/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
