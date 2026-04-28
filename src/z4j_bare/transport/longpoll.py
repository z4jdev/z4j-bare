"""HTTPS long-poll fallback transport.

When the WebSocket path is blocked (some corporate proxies strip
the ``Upgrade`` header) the agent falls back to two HTTPS
endpoints exposed by the brain at ``/api/v1/agent/*``:

- ``POST /api/v1/agent/events`` - upload one or more signed v2
  frames produced by :class:`FrameSigner`.
- ``GET  /api/v1/agent/commands?wait=N`` - long-poll for
  brain->agent command frames; the brain holds the response open
  for up to ``wait`` seconds, returning immediately on the first
  pending command.

Same authentication (bearer token), same v2 envelope-HMAC framing,
same routing semantics on the brain side. The only loss vs the
WebSocket transport is single-frame latency (~50-200 ms per round
trip vs single-digit ms over an open socket) and the ack-vs-loss
window: the WebSocket ack is implicit in the next sent frame; the
long-poll ack is the HTTP 200 itself, so a network drop between
"brain processed the frame" and "agent received 200" can cause
the agent to re-send. The brain dedups by ``event_id`` UNIQUE so
the duplicate is harmless.

The handshake (``hello``/``hello_ack``) is intentionally a no-op
on this transport - the brain instantiates per-agent
``FrameSigner``/``FrameVerifier`` lazily on the first
authenticated request and pins them to the agent's bearer token,
so we never need an explicit session-establishment frame.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import httpx

from z4j_core.errors import (
    AuthenticationError,
    InvalidFrameError,
    SignatureError,
    Z4JError,
)
from z4j_core.transport.frames import (
    Frame,
    _SignedFrameBase,
    parse_frame,
)
from z4j_core.transport.framing import FrameSigner, FrameVerifier
from z4j_core.version import __version__ as CORE_VERSION

logger = logging.getLogger("z4j.agent.transport.longpoll")


class LongPollTransport:
    """HTTPS long-poll fallback transport.

    Public surface mirrors :class:`WebSocketTransport`:
    :meth:`connect`, :meth:`send_frames`, :meth:`receive_frames`,
    :meth:`close`. The runtime treats the two transports
    interchangeably.

    Per-agent ``FrameSigner`` / ``FrameVerifier`` are constructed
    on first :meth:`connect`. We use the agent's own
    ``project_id`` / ``agent_id`` (passed in by the runtime, since
    long-poll has no ``hello_ack`` to learn them from). The brain
    pins identical bindings on its side as soon as it sees the
    first authenticated request from this token.
    """

    __slots__ = (
        "brain_url",
        "project_id",
        "agent_id",
        "framework_name",
        "engines",
        "schedulers",
        "capabilities",
        "agent_version",
        "max_frame_size",
        "_token",
        "_hmac_secret",
        "_dev_mode",
        "_client",
        "_signer",
        "_verifier",
        "_session_id",
        "_session_nonce",
        "_heartbeat_interval",
        "_send_lock",
        "_closed",
        "_poll_wait_seconds",
    )

    #: Header name shared with brain. Both sides agree on this value
    #: so the brain's per-session signer/verifier registry can key
    #: by ``(agent_id, session_nonce)`` instead of just ``agent_id``,
    #: which means a fresh ``connect()`` always gets fresh state on
    #: BOTH ends - a benign reconnect is no longer poisoned by the
    #: previous session's seq counter, and an attacker who lands a
    #: forged max-seq frame can only DoS their own (unknown) nonce.
    _SESSION_HEADER = "X-Z4J-Session-Nonce"

    def __init__(
        self,
        *,
        brain_url: str,
        token: str,
        project_id: str,
        agent_id: str,
        framework_name: str,
        engines: list[str],
        schedulers: list[str],
        capabilities: dict[str, list[str]],
        hmac_secret: bytes,
        agent_version: str = CORE_VERSION,
        max_frame_size: int = 1_048_576,
        dev_mode: bool = False,
        poll_wait_seconds: int = 30,
    ) -> None:
        if len(hmac_secret) < 32:
            raise ValueError(
                "LongPollTransport hmac_secret must be at least 32 bytes",
            )
        self.brain_url = brain_url.rstrip("/")
        self.project_id = project_id
        self.agent_id = agent_id
        self.framework_name = framework_name
        self.engines = list(engines)
        self.schedulers = list(schedulers)
        self.capabilities = dict(capabilities)
        self.agent_version = agent_version
        self.max_frame_size = max_frame_size
        self._token = token
        self._hmac_secret = hmac_secret
        self._dev_mode = dev_mode

        self._client: httpx.AsyncClient | None = None
        self._signer: FrameSigner | None = None
        self._verifier: FrameVerifier | None = None
        self._session_id: str | None = None
        self._session_nonce: str | None = None
        self._heartbeat_interval: int = 10
        self._send_lock = asyncio.Lock()
        self._closed = False
        # Brain caps the long-poll wait at 60 s; we default to 30 s
        # to keep proxies that idle-timeout connections at 60 s
        # happy with no extra work on the operator's side.
        self._poll_wait_seconds = max(1, min(int(poll_wait_seconds), 60))

    def __repr__(self) -> str:
        return (
            f"<LongPollTransport project={self.project_id!r} "
            f"brain={self.brain_url!r} token=[REDACTED]>"
        )

    @property
    def heartbeat_interval(self) -> int:
        return self._heartbeat_interval

    @property
    def session_id(self) -> str | None:
        return self._session_id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the HTTPS client and build per-session signer/verifier.

        Raises:
            AuthenticationError: The brain rejected the bearer token
                (HTTP 401 on a probe ping).
            ConnectionError: Brain unreachable.
            ValueError: ``http://`` (non-TLS) URL requested without
                ``dev_mode``. The bearer token would travel in
                cleartext on every request.
        """
        if self._closed:
            raise RuntimeError("LongPollTransport is closed")

        if self.brain_url.startswith("http://") and not self._dev_mode:
            raise ValueError(
                f"refusing plain http:// long-poll connection to "
                f"{self.brain_url}. Set brain_url to https:// or pass "
                f"dev_mode=True explicitly if this is a local test.",
            )

        # Session nonce: fresh on every connect(). Sent on every
        # request so the brain keys per-session signer/verifier
        # state by (agent_id, nonce). 16 bytes from secrets is
        # ~22 base64 chars, well under any header-size cap.
        self._session_nonce = secrets.token_urlsafe(16)

        # ``timeout=None`` on the client default; per-call we use
        # explicit timeouts so the long-poll GET can wait 30+ s
        # while the events POST stays snappy.
        self._client = httpx.AsyncClient(
            base_url=self.brain_url,
            headers={
                "Authorization": f"Bearer {self._token}",
                self._SESSION_HEADER: self._session_nonce,
            },
            timeout=httpx.Timeout(15.0, read=None),
            http2=False,
            follow_redirects=False,
        )

        # Probe the bearer token + reachability with a short wait
        # poll. 401 immediately tells us the token is bad; 200 with
        # an empty list confirms the agent row exists, the brain
        # is up, and our auth works.
        try:
            r = await self._client.get(
                "/api/v1/agent/commands",
                params={"wait": 0, "max_frames": 1},
                timeout=10.0,
            )
        except httpx.HTTPError as exc:
            await self._close_client()
            raise ConnectionError(f"long-poll probe failed: {exc}") from exc
        if r.status_code == 401:
            await self._close_client()
            raise AuthenticationError(
                "brain rejected agent token",
                details={"status": 401},
            )
        if r.status_code != 200:
            await self._close_client()
            raise ConnectionError(
                f"long-poll probe returned HTTP {r.status_code}",
            )

        agent_uuid = _safe_uuid(self.agent_id)
        project_uuid = _safe_uuid(self.project_id)

        # Round-9 audit fix R9-Wire-H1+H2 (Apr 2026): bind the
        # per-connection session_nonce into the signer/verifier so
        # captured frames from a previous nonce can't be replayed
        # under a fresh nonce. The brain's long-poll service
        # binds the same nonce on its side
        # (api/agent_longpoll.py::_get_or_create_session).
        binding = self._session_nonce or ""
        self._signer = FrameSigner(
            secret=self._hmac_secret,
            agent_id=agent_uuid,
            project_id=project_uuid,
            session_id=binding,
        )
        self._verifier = FrameVerifier(
            secret=self._hmac_secret,
            agent_id=agent_uuid,
            project_id=project_uuid,
            session_id=binding,
            direction="brain->agent",
        )
        # Synthetic session id - long-poll has no real handshake,
        # but the runtime expects to read one for logging.
        self._session_id = f"lp_{uuid4().hex[:12]}"

        logger.info(
            "z4j agent long-poll connected: project=%s session=%s",
            self.project_id,
            self._session_id,
        )

    async def close(self) -> None:
        """Close the HTTPS client and mark the transport unusable."""
        self._closed = True
        await self._close_client()

    async def _close_client(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        # Round-8 audit fix R8-Async-LOW (Apr 2026): shield the
        # aclose so an agent SIGTERM mid-shutdown doesn't leak
        # the httpx pool.
        try:
            import asyncio as _asyncio  # noqa: PLC0415
            await _asyncio.shield(client.aclose())
        except Exception:  # noqa: BLE001  pragma: no cover
            pass

    # ------------------------------------------------------------------
    # Send / receive
    # ------------------------------------------------------------------

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        """Sign + POST each buffered frame, return accepted indices.

        Mirrors :meth:`WebSocketTransport.send_frames` exactly so
        the runtime's send loop is transport-agnostic. A frame
        that fails to parse from the buffer is logged and
        "accepted" so the buffer purges it - re-sending bytes we
        cannot authenticate is worse than dropping them.
        """
        if self._client is None or self._signer is None:
            raise ConnectionError("long-poll transport not connected")

        accepted: list[int] = []
        signed: list[tuple[int, str]] = []
        for idx, raw in enumerate(frames):
            try:
                parsed = parse_frame(raw)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j longpoll: dropping unparseable buffered frame",
                )
                accepted.append(idx)
                continue
            if not isinstance(parsed, _SignedFrameBase):
                logger.error(
                    "z4j longpoll: refusing to send unsigned %s frame",
                    getattr(parsed, "type", None),
                )
                accepted.append(idx)
                continue
            try:
                signed.append((idx, self._signer.sign_and_serialize(parsed).decode("utf-8")))
            except Exception as exc:  # noqa: BLE001
                raise ConnectionError(
                    f"frame signing failed: {exc}",
                ) from exc

        if not signed:
            return accepted

        async with self._send_lock:
            try:
                r = await self._client.post(
                    "/api/v1/agent/events",
                    json={"frames": [s for _, s in signed]},
                    timeout=15.0,
                )
            except httpx.HTTPError as exc:
                raise ConnectionError(
                    f"long-poll send failed: {exc}",
                ) from exc

        if r.status_code == 401:
            raise AuthenticationError(
                "brain rejected agent token mid-session",
                details={"status": 401},
            )
        if r.status_code >= 500:
            raise ConnectionError(
                f"long-poll send returned HTTP {r.status_code}",
            )
        if r.status_code != 200:
            # 4xx other than 401 = malformed payload / over quota.
            # We treat these as accepted so the buffer purges them
            # rather than retrying forever.
            logger.warning(
                "z4j longpoll: send returned HTTP %s; dropping batch",
                r.status_code,
            )
            for idx, _ in signed:
                accepted.append(idx)
            return accepted

        body = r.json()
        accepted_count = int(body.get("accepted", 0)) + int(body.get("rejected", 0))
        # The brain treats per-frame verify failures as silent
        # drops; we mark them accepted on our side too so the
        # buffer doesn't loop on a permanently-bad entry.
        for i, (idx, _) in enumerate(signed):
            if i < accepted_count:
                accepted.append(idx)
        return accepted

    async def receive_frames(
        self,
        on_frame: Callable[[Frame], Awaitable[None]],
    ) -> None:
        """Long-poll for command frames until the transport is closed.

        Each successful poll ranges over up to ``max_frames=50``
        already-pending commands. An empty response means the
        brain held the request open for ``poll_wait_seconds`` and
        no command arrived; we re-poll immediately.

        On a verification failure we raise ``SignatureError`` so
        the supervisor closes the transport and reconnects with a
        fresh ``session_nonce`` - the brain will then build new
        signer/verifier state on the next request, recovering from
        a brain restart that desynced the seq cursor or from a
        forged frame that poisoned the previous session's state.
        """
        if self._client is None or self._verifier is None:
            raise ConnectionError("long-poll transport not connected")

        while not self._closed:
            try:
                r = await self._client.get(
                    "/api/v1/agent/commands",
                    params={
                        "wait": self._poll_wait_seconds,
                        "max_frames": 50,
                    },
                    timeout=self._poll_wait_seconds + 10.0,
                )
            except httpx.HTTPError as exc:
                # Network blip - exit so the supervisor reconnects
                # with backoff. Same contract as WebSocket recv.
                raise ConnectionError(
                    f"long-poll recv failed: {exc}",
                ) from exc

            if r.status_code == 401:
                raise AuthenticationError(
                    "brain rejected agent token mid-session",
                    details={"status": 401},
                )
            if r.status_code != 200:
                raise ConnectionError(
                    f"long-poll recv returned HTTP {r.status_code}",
                )

            payload = r.json()
            for raw in payload.get("frames", []):
                try:
                    frame = self._verifier.parse_and_verify(raw)
                except SignatureError as exc:
                    # Session is desynced (brain restart, forged
                    # frame, replay attempt). Don't keep polling
                    # with a poisoned _last_seq cursor - surface
                    # to the supervisor so it reconnects with a
                    # fresh session_nonce. This matches the WS
                    # path's 4403 close behaviour.
                    logger.error(
                        "z4j longpoll: command frame failed verification, reconnecting: %s",
                        exc,
                    )
                    raise
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "z4j longpoll: command frame parse failed",
                    )
                    raise InvalidFrameError(
                        f"could not parse command frame: {exc}",
                    ) from exc
                try:
                    await on_frame(frame)
                except Z4JError:
                    logger.exception(
                        "z4j longpoll: command handler raised z4j error",
                    )
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "z4j longpoll: command handler raised",
                    )


def _safe_uuid(value: str | UUID) -> UUID:
    """Best-effort UUID coercion; FrameSigner accepts str or UUID anyway."""
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, AttributeError):
        return uuid4()


__all__ = ["LongPollTransport"]
