# z4j-bare

[![PyPI version](https://img.shields.io/pypi/v/z4j-bare.svg)](https://pypi.org/project/z4j-bare/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-bare.svg)](https://pypi.org/project/z4j-bare/)
[![License](https://img.shields.io/pypi/l/z4j-bare.svg)](https://github.com/z4jdev/z4j-bare/blob/main/LICENSE)

The framework-free agent runtime for [z4j](https://z4j.com).

Bootstraps a z4j agent inside any Python process, Celery worker,
RQ worker, Dramatiq actor, plain script, custom service, and connects
it to z4j over an authenticated WebSocket. Used directly when
there's no framework adapter (z4j-django / z4j-flask / z4j-fastapi);
used indirectly by those framework adapters too.

## Compatibility

Python 3.11+. No framework or engine pinned; pair with whichever engine adapter your worker runs (`z4j-celery`, `z4j-rq`, `z4j-dramatiq`, `z4j-huey`, `z4j-arq`, `z4j-taskiq`) and that adapter carries the engine-version floor.

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

- **Agent runtime**, connect, authenticate, supervise, reconnect
  with bounded backoff
- **Outbound buffer**, captured events are written to a bounded local SQLite
  ring before going out on the wire and survive short brain outages and agent
  restarts. At the configured count or byte limit, the oldest buffered rows
  are evicted and the loss is logged.
- **Engine signal hooks**, wired up by whichever engine adapter you
  install (z4j-celery, z4j-rq, etc.); the runtime drains them into
  the buffer
- **Schedule inventory**, emits a full snapshot at boot, on a
  periodic timer (default 15 min), and on demand from the brain's
  *Sync now* command. Existing schedules show up automatically.
- **Command dispatcher**, receives operator actions from the brain
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
import os

from z4j_bare import install_agent
from z4j_celery import CeleryEngineAdapter

install_agent(
    engines=[CeleryEngineAdapter(celery_app=app)],
    brain_url="https://brain.example.com",
    token="z4j_agent_...",
    project_id="my-project",
    hmac_secret=os.environ["Z4J_HMAC_SECRET"],
)
```

## Reliability

- Invalid or missing configuration and unsafe buffer initialization fail the
  explicit `install_agent()` call; after successful startup, transport work
  runs off the host application's request and task paths.
- Engine adapters use bounded in-process event queues and the runtime uses a
  bounded SQLite buffer. Queue overflow drops new events; buffer pressure
  evicts the oldest rows. Both paths log the loss.
- The supervisor retries transient network and TLS failures with bounded
  backoff. Authentication, configuration, and protocol-version failures are
  terminal so they do not create reconnect storms.

## Documentation

Full docs at [z4j.dev/frameworks/bare/](https://z4j.dev/frameworks/bare/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-bare/
- Issues: https://github.com/z4jdev/z4j-bare/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
