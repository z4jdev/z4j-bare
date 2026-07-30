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
from uuid import UUID, uuid4

import httpx
from z4j_core.errors import (
    AuthenticationError,
    InvalidFrameError,
    ProtocolError,
    SignatureError,
    Z4JError,
)
from z4j_core.transport.frames import (
    RETRY_BY_REFERENCE_CAPABILITY,
    Frame,
    _SignedFrameBase,
    parse_frame,
)
from z4j_core.transport.framing import FrameSigner, FrameVerifier
from z4j_core.version import (
    __version__ as CORE_VERSION,  # noqa: N812  conventional version constant alias
)

from z4j_bare.transport.websocket import AGENT_RUNTIME_FEATURES

logger = logging.getLogger("z4j.transport.longpoll")

#: HTTP statuses where the brain looked at the FRAME content and rejected
#: it as malformed/invalid: 415 (wrong media type) and 422 (body-shape
#: validation on the frame envelope). Re-sending the identical body loops,
#: so a SINGLE frame that persistently returns one of these is genuinely
#: undeliverable and (after batch-size reduction has isolated it) is dropped
#: after a bounded retry budget; a MULTI-frame batch is reduced/split first
#: so valid siblings are never co-dropped.
#:
#: 400 is deliberately NOT here: the ``/agent/events`` route never
#: emits a content-based 400 -- body-shape violations are FastAPI/Pydantic
#: 422, media-type is 415 -- so every 400 reaching a POST is REQUEST-level
#: (the host-validation middleware rejects a bad Host/Origin BEFORE the body
#: is read, or an upstream proxy/WAF). Treating it as a per-frame content
#: reject destroyed deliverable events during an HA allowed-hosts skew or a
#: mid-session config/WAF change, so 400 now falls through to the retryable
#: reconnect path with 403/404/405 (routing / WAF / mixed-deploy) and
#: 3xx/408/425/429/5xx. A TRANSIENT partial store (a 200 storing fewer than
#: sent) is separate again: retried with backoff, never a drop-budget hit.
#:
#: Accepted residual (r11 review): if an INTERMEDIARY WAF/proxy returns 400
#: keyed to one frame's bytes, that frame reconnect-loops (head-of-line) and
#: never hits the content-drop budget; it self-clears only via buffer
#: overflow. Treating 400 as content-drop instead would re-introduce the far
#: more common host-skew mass-loss, so retryable is the least-bad choice.
_CONTENT_REJECT_STATUSES: frozenset[int] = frozenset({415, 422})


class UploadRetryableError(Exception):
    """The brain did not durably store every frame, for a TRANSIENT
    reason (a 200 response reporting fewer stored than sent -- a DB
    deadlock / pool timeout / transient skip on the brain).

    The whole batch is re-sent after a BACKOFF (the brain dedups
    already-stored frames by content-derived event_id, so replay is
    harmless). It must NOT count toward any drop budget: the frames are
    deliverable, the brain just could not store them this instant
    Deliberately NOT a:class:`ConnectionError` (a content
    round-trip succeeded; no need to tear down the session).
    """


class UploadContentRejectedError(Exception):
    """The brain rejected the FRAME content as malformed/invalid
    (HTTP 415 media-type or 422 envelope validation). Re-sending the
    identical body loops.

    A MULTI-frame batch is reduced/split so valid siblings still deliver;
    a SINGLE frame that keeps being content-rejected is genuinely
    undeliverable and is dropped after a bounded retry budget. A
    bare 400 is NOT a content reject on this route -- it is request-level
    (host validation / proxy / WAF) and retried, not dropped.
    """


class PayloadTooLargeError(Exception):
    """A long-poll POST returned HTTP 413 (body exceeded the brain cap).

    The runtime reduces its send batch size and retries; a single frame
    that still 413s is dropped after a bounded budget (it can never fit),
    NOT on the first failure (a transient/proxy 413 must not lose a
    deliverable frame). Not a:class:`ConnectionError`.
    """


class LongPollTransport:
    """HTTPS long-poll fallback transport.

    Public surface mirrors :class:`WebSocketTransport`:
    :meth:`connect`, :meth:`send_frames`, :meth:`receive_frames`,
    :meth:`close`. The runtime treats the two transports
    interchangeably.

    Per-agent ``FrameSigner`` / ``FrameVerifier`` are constructed
    on first :meth:`connect`, bound to the canonical agent/project
    UUIDs the brain advertises on the probe response
    (``X-Z4J-Agent-Id`` / ``X-Z4J-Project-Id``, the long-poll
    analogue of the WebSocket ``hello_ack``). The config-supplied
    ``project_id`` / ``agent_id`` are only a fallback for brains
    that predate the headers, and then only when they are real
    UUIDs (the config's project_id is normally a slug).
    """

    __slots__ = (
        "_client",
        "_closed",
        "_dev_mode",
        "_heartbeat_interval",
        "_hmac_secret",
        "_poll_wait_seconds",
        "_send_lock",
        "_session_id",
        "_session_nonce",
        "_signer",
        "_token",
        "_verifier",
        "agent_id",
        "agent_version",
        "brain_url",
        "capabilities",
        "engines",
        "framework_name",
        "max_frame_size",
        "project_id",
        "schedulers",
    )

    #: The brain has no ack channel over long-poll: it never signs an
    #: ``event_batch_ack`` into the ``GET /commands`` response (that
    #: route only carries command frames), and the events POST's HTTP
    #: 200 IS the acknowledgement (the brain dedups by event_id, so a
    #: replay is harmless). The runtime consults this flag to confirm
    #: buffered event_batch entries on a successful send instead of
    #: waiting for an ack frame that never arrives. Without it the send
    #: loop re-drains every unconfirmed batch each iteration and POSTs
    #: the same already-ingested events at line rate until the brain
    #: rate-limits (429), which was itself a loss path pre-fix.
    confirm_on_send: bool = True

    #: Header name shared with brain. Both sides agree on this value
    #: so the brain's per-session signer/verifier registry can key
    #: by ``(agent_id, session_nonce)`` instead of just ``agent_id``,
    #: which means a fresh ``connect()`` always gets fresh state on
    #: BOTH ends - a benign reconnect is no longer poisoned by the
    #: previous session's seq counter, and an attacker who lands a
    #: forged max-seq frame can only DoS their own (unknown) nonce.
    _SESSION_HEADER = "X-Z4J-Session-Nonce"
    #: Long-poll analogue of WebSocket runtime-feature observability.
    _RUNTIME_FEATURES_HEADER = "X-Z4J-Runtime-Features"
    _RETRY_CONTRACTS_HEADER = "X-Z4J-Retry-Contracts"

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

    def _retry_contracts_header(self) -> str:
        """Compact adapter-derived contracts sent on every command poll."""
        engines = sorted(
            name
            for name, advertised in self.capabilities.items()
            if RETRY_BY_REFERENCE_CAPABILITY in advertised
        )
        return ",".join(f"{name}=1" for name in engines)

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
                # Runtime-wide feature observability. This is deliberately
                # distinct from the adapter-derived contract header below.
                self._RUNTIME_FEATURES_HEADER: ",".join(AGENT_RUNTIME_FEATURES),
                # Boundary A: unlike the advisory runtime feature above, this
                # is derived from the loaded adapter objects and checked only
                # for the exact long-poll request that claims a retry.
                self._RETRY_CONTRACTS_HEADER: self._retry_contracts_header(),
            },
            timeout=httpx.Timeout(15.0, read=None),
            http2=False,
            follow_redirects=False,
        )

        # Probe the bearer token + reachability.: use max_frames=0, the
        # brain's NON-CLAIMING liveness/identity mode -- 401 tells us the token is
        # bad; 200 confirms the agent row exists, the brain is up, our auth works,
        # and the X-Z4J-Agent/Project-Id headers carry the canonical UUIDs. The old
        # max_frames=1 probe CLAIMED (marked DISPATCHED) the oldest pending command
        # and discarded the body, permanently stranding a queued DESTRUCTIVE
        # command (non-redeliverable, so never re-sent) until it timed out.
        #
        # (N-1): a pre-1.7.1 brain declares ``max_frames`` as ``ge=1``, so
        # it 422s max_frames=0 in FastAPI's query validation BEFORE the handler
        # runs -- returning no identity headers and, without a fallback, making a
        # 1.7.1 agent unable to connect to a 1.7.0 brain at all. On a 422 we fall
        # back to max_frames=1, which a 1.7.0 brain accepts; that merely reproduces
        # that older brain's OWN connect-probe behaviour (it claims the oldest
        # pending command), so it is no regression versus a 1.7.0 agent talking to
        # the same brain. A 1.7.1 brain never 422s max_frames=0, so the fallback
        # only ever runs against a genuinely older brain.
        async def _probe(max_frames_val: int) -> httpx.Response:
            return await self._client.get(
                "/api/v1/agent/commands",
                params={"wait": 0, "max_frames": max_frames_val},
                timeout=10.0,
            )

        try:
            r = await _probe(0)
            if r.status_code == 422:
                r = await _probe(1)
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

        # The probe response advertises the canonical agent/project
        # UUIDs (the long-poll analogue of the WebSocket hello_ack).
        # Bind THOSE into the signer/verifier: the config's
        # project_id is a SLUG, and the frame HMAC envelope binds
        # the project UUID on the brain side, so signing under
        # anything else fails verification on every frame. Config
        # values are the fallback for brains that predate the
        # identity headers (only helps when the operator configured
        # real UUIDs; a slug there was never able to work).
        agent_uuid = _uuid_or_none(
            r.headers.get("x-z4j-agent-id"),
        ) or _uuid_or_none(self.agent_id)
        project_uuid = _uuid_or_none(
            r.headers.get("x-z4j-project-id"),
        ) or _uuid_or_none(self.project_id)
        if agent_uuid is None or project_uuid is None:
            logger.warning(
                "z4j longpoll: brain sent no identity headers and the "
                "configured agent_id/project_id are not UUIDs "
                "(agent_id=%r project_id=%r); frame signatures will "
                "not verify. Upgrade the brain, or set Z4J_AGENT_ID / "
                "Z4J_PROJECT_ID to the UUIDs shown on the brain's "
                "agents page.",
                self.agent_id,
                self.project_id,
            )
            agent_uuid = agent_uuid or uuid4()
            project_uuid = project_uuid or uuid4()

        # Bind the per-connection session_nonce into the signer/verifier so
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
        # Shield the aclose so an agent SIGTERM mid-shutdown doesn't
        # leak the httpx pool.
        try:
            import asyncio as _asyncio

            await _asyncio.shield(client.aclose())
        except Exception:  # noqa: S110  best-effort httpx pool close on shutdown
            pass

    # ------------------------------------------------------------------
    # Send / receive
    # ------------------------------------------------------------------

    async def send_frames(  # noqa: PLR0912, PLR0915  per-frame sign/post branching
        self,
        frames: list[bytes],
    ) -> list[int]:
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
            except Exception:
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
            except Exception as exc:
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
        if r.status_code == 200:
            try:
                body = r.json()
            except Exception:
                body = {}
            if isinstance(body, dict) and body.get("error_code") == "scheduler_upgrade_required":
                raise ProtocolError(
                    "brain requires a current Boundary-D scheduler adapter",
                )
            # Parse the stored-count DEFENSIVELY: confirm (delete) the
            # POSTed frames ONLY on EXACT equality with the number sent.
            # A malformed/over-count response (a non-int, a bool, a
            # negative, or a value greater than what we sent) must NEVER
            # confirm a frame the brain did not store -- treat anything
            # but the exact match as "not all stored" and retry.
            raw = body.get("accepted") if isinstance(body, dict) else None
            stored = raw if isinstance(raw, int) and not isinstance(raw, bool) else -1
            if stored == len(signed):
                accepted.extend(idx for idx, _ in signed)
                return accepted
            # Stored fewer than sent (or a malformed count): the brain
            # had a TRANSIENT problem storing some frames (a DB deadlock /
            # pool timeout / transient skip). Confirm NOTHING and re-send
            # the whole batch after a backoff. The brain dedups the
            # already-stored frames by content-derived event_id, so
            # replay is harmless. This is a transient outcome, NOT a
            # content rejection: it must not consume a drop budget
            raise UploadRetryableError(
                f"long-poll: brain stored {stored}/{len(signed)} frames "
                "(transient); re-sending the whole batch after backoff",
            )
        if r.status_code == 413:
            raise PayloadTooLargeError(
                "long-poll: POST body too large (HTTP 413)",
            )
        if r.status_code in _CONTENT_REJECT_STATUSES:
            # The brain rejected the FRAME content as malformed/invalid
            # (415 media-type / 422 envelope validation). Re-sending the
            # identical body loops. The runtime reduces/splits a multi-frame
            # batch (valid siblings still deliver) and drops only a SINGLE
            # persistently-rejected frame after a bounded budget.
            raise UploadContentRejectedError(
                f"long-poll: brain rejected the frame content (HTTP {r.status_code})",
            )
        # Everything else -- 3xx redirects (follow disabled, so the body
        # never reached the handler), a bare 400 (request-level host
        # validation / proxy / WAF, NOT per-frame content),
        # 403/404/405 (routing/WAF/mixed deploy), transient 408/425/429/5xx,
        # and any unexpected status -- is treated as transient: keep the
        # batch unconfirmed and retry after backoff, NEVER counting
        # toward the quarantine. Honor Retry-After when present.
        retry_after = r.headers.get("retry-after")
        if retry_after:
            try:
                delay = min(float(retry_after), float(self._poll_wait_seconds))
                if delay > 0:
                    await asyncio.sleep(delay)
            except (ValueError, TypeError):
                pass
        raise ConnectionError(
            f"long-poll send returned HTTP {r.status_code} (retryable)",
        )

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
                except SignatureError:
                    # Session is desynced (brain restart, forged
                    # frame, replay attempt). Don't keep polling
                    # with a poisoned _last_seq cursor - surface
                    # to the supervisor so it reconnects with a
                    # fresh session_nonce. This matches the WS
                    # path's 4403 close behaviour.
                    logger.exception(
                        "z4j longpoll: command frame failed verification, reconnecting",
                    )
                    raise
                except Exception as exc:
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
                except Exception:
                    logger.exception(
                        "z4j longpoll: command handler raised",
                    )


def _uuid_or_none(value: str | UUID | None) -> UUID | None:
    """Parse a UUID, or return None for anything that is not one.

    The pre-1.7 ``_safe_uuid`` this replaces minted a RANDOM uuid4
    for non-UUID input, which silently bound a garbage project id
    into the frame HMAC whenever the config carried the documented
    project SLUG, making every frame fail verification.
    """
    if isinstance(value, UUID):
        return value
    if not value:
        return None
    try:
        return UUID(str(value))
    except (ValueError, AttributeError):
        return None


__all__ = [
    "LongPollTransport",
    "PayloadTooLargeError",
    "UploadContentRejectedError",
    "UploadRetryableError",
]
