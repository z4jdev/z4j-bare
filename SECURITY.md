# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in `z4j-bare`,
**do not open a public GitHub issue**. Email `security@z4j.com` instead.

We follow the [disclose.io](https://disclose.io) baseline:

- Initial acknowledgement within **72 hours**.
- Coordinated disclosure timeline agreed before public release.
- Credit in the release notes (unless you prefer to remain anonymous).

PGP key and the full disclosure policy live in the
[z4j project security policy](https://github.com/z4jdev/z4j/blob/main/SECURITY.md).

## Supported versions

Only the latest minor release receives security fixes. See
[CHANGELOG.md](CHANGELOG.md) for the current version.

## Security-critical surface

`z4j-bare` runs **inside your production application process** and
talks to the brain over the network. Its security surface is:

- **Transport / HMAC v2 envelope verification** (`z4j_bare.transport`, via
  `z4j_core.transport`) - a weakness would allow a hostile brain (or MITM) to
  inject commands the agent would execute.
- **Command dispatcher** (`dispatcher.py`) - executes brain-issued actions
  (cancel, retry, rate-limit, restart_worker). Must reject unverified frames and
  must audit-log every execution.
- **Local SQLite buffer** (`buffer.py`) - stores event payloads before send;
  secrets redaction must run *before* buffering.
- **Orchestrator detection** (`orchestrator_detect.py`) - gates `restart_worker`
  off `os._exit(0)`; bypasses here could cause task loss.

These areas receive priority review on every PR. Redaction is
performed inside `z4j-core`; see its SECURITY.md for that surface.
