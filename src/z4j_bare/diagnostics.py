"""Reusable agent diagnostics for ``z4j-doctor`` style commands.

Each framework adapter (z4j-django, z4j-flask, z4j-fastapi) ships a
``z4j-doctor`` entry point that wraps the same probes here. Probes
return structured :class:`ProbeResult` records, never raise: a doctor
command's job is to *report* a failure, not bubble it up as a
traceback that's harder to read than the underlying issue.

The probes intentionally do their own minimal I/O (DNS, TCP, TLS,
HTTP, file write) instead of going through the full transport stack
- that way a doctor run does not require a working transport, and
the failure mode it reports is the lowest-level one in play.

Probes:

- :func:`probe_buffer_path` - mkdir + write + delete on the resolved
  buffer dir. Catches the gunicorn-under-www-data class of failures.
- :func:`probe_dns` - resolves the brain hostname.
- :func:`probe_tcp` - opens a TCP connection to the brain port.
- :func:`probe_tls` - completes a TLS handshake (SNI honored).
- :func:`probe_websocket` - upgrades to ``wss://<brain>/ws/agent``
  and waits for the server's hello_ack or auth rejection.
- :func:`run_all` - one-call orchestrator that runs each probe in
  order, short-circuiting after the first hard failure.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import ssl
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from z4j_core.models import Config
from z4j_core.paths import buffer_root, z4j_home

from z4j_bare.storage import is_writable_dir, primary_buffer_root

logger = logging.getLogger("z4j.runtime.diagnostics")


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """Outcome of a single probe.

    Attributes:
        name: Short probe name (``"buffer_path"``, ``"dns"``, ...).
              Stable across releases for scripting.
        ok: True if the probe succeeded.
        message: Human-readable summary. One line preferred.
        details: Extra structured data (resolved IPs, cert subject,
                 chosen path, etc.) for verbose output.
    """

    name: str
    ok: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def probe_buffer_path(buffer_path: Path) -> ProbeResult:
    """Test that the resolved buffer directory is actually usable.

    Tries: mkdir parents -> write probe file -> delete probe file.
    On failure, also reports whether the per-uid tmp fallback would
    work, so the doctor can suggest a one-liner fix.
    """
    parent = buffer_path.parent
    if is_writable_dir(parent):
        return ProbeResult(
            name="buffer_path",
            ok=True,
            message=f"OK: buffer dir {parent} is writable",
            details={"path": str(buffer_path)},
        )

    # Probe the per-uid temp fallback that buffer_root() falls back to.
    # If it's also unwritable, advise creating the primary directory.
    resolved_fallback = buffer_root()
    fb_ok = resolved_fallback != z4j_home() and is_writable_dir(resolved_fallback)
    suggestion = (
        f"set Z4J_HOME={resolved_fallback} (per-uid tmp dir is writable) "
        f"or chown {parent} to the running user"
        if fb_ok
        else f"create {parent} and chown it to the running user "
        f"(uid={_uid_or_user()}); the per-uid tmp fallback is also "
        f"unwritable on this host"
    )
    return ProbeResult(
        name="buffer_path",
        ok=False,
        message=f"FAIL: buffer dir {parent} is not writable; {suggestion}",
        details={
            "path": str(buffer_path),
            "uid": _uid_or_user(),
            "fallback": str(resolved_fallback),
            "fallback_writable": fb_ok,
        },
    )


def probe_dns(brain_url: str, timeout: float = 5.0) -> ProbeResult:
    """Resolve the brain hostname to one or more IPs.

    Honors the system resolver, including ``/etc/hosts`` overrides
    and any DNS-over-HTTPS the OS provides. Reports all resolved
    addresses so the operator can spot stale entries or split-DNS
    misconfiguration.
    """
    parsed = urlparse(brain_url)
    host = parsed.hostname
    if not host:
        return ProbeResult(
            name="dns",
            ok=False,
            message=f"FAIL: brain URL {brain_url!r} has no hostname",
        )
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError as exc:
        return ProbeResult(
            name="dns",
            ok=False,
            message=f"FAIL: DNS lookup for {host!r} failed: {exc}",
            details={"host": host, "errno": getattr(exc, "errno", None)},
        )
    addresses = sorted({info[4][0] for info in infos})
    return ProbeResult(
        name="dns",
        ok=True,
        message=f"OK: {host} -> {', '.join(addresses)}",
        details={"host": host, "addresses": addresses},
    )


def probe_tcp(brain_url: str, timeout: float = 5.0) -> ProbeResult:
    """Open a TCP connection to ``host:port`` derived from the brain URL.

    Defaults the port from the URL scheme (https=443, http=80) if
    none is explicit. Closes the socket immediately on success - we
    only care that the three-way handshake completes.
    """
    parsed = urlparse(brain_url)
    host = parsed.hostname
    if not host:
        return ProbeResult(
            name="tcp",
            ok=False,
            message=f"FAIL: brain URL {brain_url!r} has no hostname",
        )
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
    except TimeoutError:
        return ProbeResult(
            name="tcp",
            ok=False,
            message=f"FAIL: TCP connect to {host}:{port} timed out after {timeout}s",
            details={"host": host, "port": port},
        )
    except OSError as exc:
        return ProbeResult(
            name="tcp",
            ok=False,
            message=f"FAIL: TCP connect to {host}:{port}: {exc}",
            details={"host": host, "port": port, "errno": getattr(exc, "errno", None)},
        )
    sock.close()
    return ProbeResult(
        name="tcp",
        ok=True,
        message=f"OK: TCP connect to {host}:{port}",
        details={"host": host, "port": port},
    )


def probe_tls(brain_url: str, timeout: float = 5.0) -> ProbeResult:
    """Complete a TLS handshake and report the cert subject.

    Skipped (returned as ``ok=True`` with a "skipped" note) when the
    brain URL is plain ``http://`` - dev/loopback only.

    Reports the negotiated protocol version and cert subject CN.
    Does NOT pin certs - the system trust store is authoritative.
    """
    parsed = urlparse(brain_url)
    if parsed.scheme != "https":
        return ProbeResult(
            name="tls",
            ok=True,
            message=f"SKIP: {brain_url} is not https",
        )
    host = parsed.hostname
    port = parsed.port or 443
    if not host:
        return ProbeResult(
            name="tls",
            ok=False,
            message=f"FAIL: brain URL {brain_url!r} has no hostname",
        )
    ctx = ssl.create_default_context()
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw:
            with ctx.wrap_socket(raw, server_hostname=host) as tls:
                cert = tls.getpeercert() or {}
                proto = tls.version()
    except ssl.SSLCertVerificationError as exc:
        return ProbeResult(
            name="tls",
            ok=False,
            message=f"FAIL: TLS cert for {host} did not verify: {exc.reason}",
            details={"host": host, "port": port, "verify_message": exc.verify_message},
        )
    except OSError as exc:
        return ProbeResult(
            name="tls",
            ok=False,
            message=f"FAIL: TLS handshake with {host}:{port}: {exc}",
            details={"host": host, "port": port},
        )
    subject = _cert_subject_cn(cert)
    return ProbeResult(
        name="tls",
        ok=True,
        message=f"OK: TLS {proto} to {host} (cert CN={subject!r})",
        details={"host": host, "port": port, "tls_version": proto, "cn": subject},
    )


def probe_websocket(config: Config, timeout: float = 5.0) -> ProbeResult:
    """Try to upgrade to ``wss://<brain>/ws/agent`` and read the first frame.

    Uses the same transport the runtime would use, but with an
    aggressive timeout and an early close - the goal is to detect:

    - Bad token (server closes with 4001/4002)
    - Wrong project_id (server closes with 4003)
    - HMAC missing/invalid (server closes with 4004)
    - Reverse proxy that doesn't pass Upgrade header (server returns
      HTTP 200/301/502 instead of 101)
    - Idle close from intermediary (Cloudflare 5xx, etc.)
    """
    try:
        return asyncio.run(_probe_websocket_async(config, timeout))
    except RuntimeError as exc:
        # asyncio.run inside an existing loop (rare from a doctor
        # CLI but possible from some test harnesses).
        return ProbeResult(
            name="websocket",
            ok=False,
            message=f"FAIL: cannot run async probe: {exc}",
        )


async def _probe_websocket_async(config: Config, timeout: float) -> ProbeResult:
    from z4j_bare.runtime import AgentRuntime  # local import to avoid cycle

    # We instantiate a transport-only path: build the framework
    # adapter and a no-op engines list so AgentRuntime.start() opens
    # the WS but does nothing else. Then immediately close.
    from z4j_bare.framework import BareFrameworkAdapter

    framework = BareFrameworkAdapter(config)
    runtime = AgentRuntime(
        config=config,
        framework=framework,
        engines=[],
        schedulers=[],
    )
    try:
        await asyncio.wait_for(asyncio.to_thread(runtime.start), timeout)
    except TimeoutError:
        return ProbeResult(
            name="websocket",
            ok=False,
            message=f"FAIL: ws upgrade did not complete within {timeout}s",
        )
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            name="websocket",
            ok=False,
            message=f"FAIL: ws upgrade rejected: {type(exc).__name__}: {exc}",
        )
    finally:
        try:
            runtime.stop()
        except Exception:  # noqa: BLE001
            pass
    return ProbeResult(
        name="websocket",
        ok=True,
        message=f"OK: ws upgrade to {config.brain_url} succeeded",
    )


def run_all(config: Config) -> list[ProbeResult]:
    """Run every probe in order, short-circuit after the first hard fail.

    Probes are ordered cheapest-and-most-fundamental first so the
    output reads like a layered diagnostic: a DNS failure renders
    the TCP / TLS / WS results moot, and surfacing them would be
    misleading.
    """
    results: list[ProbeResult] = []

    # Local first: nothing about the network matters if the buffer
    # can't even be written.
    bp = probe_buffer_path(config.buffer_path)
    results.append(bp)
    if not bp.ok:
        return results

    brain_url = str(config.brain_url)
    for probe in (probe_dns, probe_tcp, probe_tls):
        r = probe(brain_url)
        results.append(r)
        if not r.ok:
            return results

    results.append(probe_websocket(config))
    return results


def _uid_or_user() -> str:
    if hasattr(os, "getuid"):
        return str(os.getuid())
    return os.environ.get("USERNAME") or os.environ.get("USER") or "unknown"


def _cert_subject_cn(cert: dict[str, Any]) -> str:
    for tup in cert.get("subject", ()):
        for k, v in tup:
            if k == "commonName":
                return v
    return "<no CN>"


__all__ = [
    "ProbeResult",
    "probe_buffer_path",
    "probe_dns",
    "probe_tcp",
    "probe_tls",
    "probe_websocket",
    "run_all",
]
