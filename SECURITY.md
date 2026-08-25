# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in `z4j-bare`,
**do not open a public GitHub issue**. Email `security@z4j.com` instead.

We acknowledge reports within **48 hours**, provide a preliminary assessment
within **5 business days**, and target fixes within **30 days** (**7 days** for
confirmed critical issues). Reporting timelines, safe harbor, supported-version
policy, and published advisories are maintained in the
[canonical z4j project security policy](https://github.com/z4jdev/z4j/blob/main/SECURITY.md).

## Security-critical surface

`z4j-bare` runs **inside your production application process** and
talks to the brain over the network. Its security surface is:

- **Transport / HMAC v2 envelope verification** (`z4j_bare.transport`, via
  `z4j_core.transport`) - a weakness could allow an on-path attacker or
  unauthorized peer to forge commands. An authenticated brain is the command
  authority and can issue the advertised controls by design.
- **Command dispatcher** (`dispatcher.py`) - executes brain-issued actions
  (cancel, retry, rate-limit, restart_worker). It must reject unverified frames
  and emit command acknowledgements/results so the brain can persist the
  execution outcome in its audit trail.
- **Local SQLite buffer** (`buffer.py`) - stores event payloads before send;
  secrets redaction must run *before* buffering.
- **Orchestrator detection** (`orchestrator_detect.py`) - gates `restart_worker`
  off `os._exit(0)`; bypasses here could cause task loss.

Redaction is performed inside `z4j-core`; see its SECURITY.md for that
surface.
