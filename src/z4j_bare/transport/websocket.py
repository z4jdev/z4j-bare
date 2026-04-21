"""WebSocket transport for the agent.

The primary transport. Maintains a long-lived connection to
``wss://<brain>/ws/agent`` using the ``websockets`` library. On
disconnect, the runtime reconnects with exponential backoff + jitter.

This module only handles the TRANSPORT layer - parsing frames,
signing, dispatch, and buffer management are all upstream of here.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from z4j_core.errors import (
    AuthenticationError,
    InvalidFrameError,
    SignatureError,
    Z4JError,
)
from z4j_core.transport.frames import (
    Frame,
    HelloAckFrame,
    HelloFrame,
    HelloPayload,
    _SignedFrameBase,
    parse_frame,
    serialize_frame,
)
from z4j_core.transport.framing import FrameSigner, FrameVerifier
from z4j_core.transport.versioning import CURRENT_PROTOCOL, check_compatibility
from z4j_core.version import __version__ as CORE_VERSION

logger = logging.getLogger("z4j.agent.transport.websocket")


class WebSocketTransport:
    """WebSocket-based transport to the brain.

    Constructed once per agent runtime. :meth:`connect` negotiates the
    ``hello`` handshake and opens the persistent connection;
    :meth:`send_frames` pushes outbound frames; :meth:`receive_frames`
    runs a loop that dispatches inbound frames to a callback; and
    :meth:`close` tears everything down.

    Attributes:
        brain_url: Base URL of the brain (``https://z4j.example.com``).
                   The ``/ws/agent`` path is appended automatically.
        project_id: Project the agent belongs to. Used in logs only.
        framework_name: Framework adapter name (``"django"``,
                        ``"bare"``, ...). Sent in the ``hello`` frame.
        engines: List of engine adapter names.
        schedulers: List of scheduler adapter names.
        capabilities: Per-adapter capability sets.
        agent_version: Agent version string. Sent in ``hello``.
        max_frame_size: Maximum inbound WebSocket frame size in bytes.

    The bearer token is stored in a private slot and never exposed via
    public attributes - :meth:`__repr__` masks it so accidental
    ``logger.info(transport)`` cannot leak it.
    """

    __slots__ = (
        "brain_url",
        "project_id",
        "framework_name",
        "engines",
        "schedulers",
        "capabilities",
        "agent_version",
        "max_frame_size",
        "_token",
        "_hmac_secret",
        "_ws",
        "_session_id",
        "_heartbeat_interval",
        "_send_lock",
        "_closed",
        "_dev_mode",
        "_signer",
        "_verifier",
    )

    def __init__(
        self,
        *,
        brain_url: str,
        token: str,
        project_id: str,
        framework_name: str,
        engines: list[str],
        schedulers: list[str],
        capabilities: dict[str, list[str]],
        hmac_secret: bytes,
        agent_version: str = CORE_VERSION,
        max_frame_size: int = 1_048_576,
        dev_mode: bool = False,
    ) -> None:
        if len(hmac_secret) < 32:
            raise ValueError(
                "WebSocketTransport hmac_secret must be at least 32 bytes",
            )
        self.brain_url = brain_url
        self._token = token
        self._hmac_secret = hmac_secret
        self.project_id = project_id
        self.framework_name = framework_name
        self.engines = list(engines)
        self.schedulers = list(schedulers)
        self.capabilities = dict(capabilities)
        self.agent_version = agent_version
        self.max_frame_size = max_frame_size
        self._dev_mode = dev_mode

        self._ws: ClientConnection | None = None
        self._session_id: str | None = None
        self._heartbeat_interval: int = 10
        self._send_lock = asyncio.Lock()
        self._closed = False
        self._signer: FrameSigner | None = None
        self._verifier: FrameVerifier | None = None

    def __repr__(self) -> str:
        return (
            f"<WebSocketTransport project={self.project_id!r} "
            f"brain={self.brain_url!r} token=[REDACTED]>"
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @property
    def ws_url(self) -> str:
        """WebSocket URL derived from the brain base URL."""
        base = self.brain_url.rstrip("/")
        if base.startswith("https://"):
            base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            base = "ws://" + base[len("http://"):]
        return base + "/ws/agent"

    @property
    def heartbeat_interval(self) -> int:
        """Seconds between agent heartbeats, as advertised by the brain."""
        return self._heartbeat_interval

    @property
    def session_id(self) -> str | None:
        """Current session id, or None if not connected."""
        return self._session_id

    async def connect(self) -> None:
        """Establish the WebSocket + negotiate the ``hello`` handshake.

        Raises:
            AuthenticationError: The brain rejected the token (HTTP 401).
            ProtocolError: The brain refused the protocol version.
            ConnectionError: Any transport-level failure.
            ValueError: Plain-``ws://`` (non-TLS) was requested in
                non-dev mode. The bearer token would travel in
                cleartext; we refuse.
        """
        if self._closed:
            raise RuntimeError("WebSocketTransport is closed")

        url = self.ws_url
        # Security (audit H6): refuse plain ``ws://`` unless the
        # operator explicitly opted into ``dev_mode``. The bearer
        # token goes in the ``Authorization`` header of the initial
        # HTTP upgrade; a MITM on a non-TLS link harvests it in
        # cleartext, and combined with C2 (today's agent→brain
        # traffic is unsigned) that is total agent impersonation.
        if url.startswith("ws://") and not self._dev_mode:
            raise ValueError(
                f"refusing plain ws:// connection to {url}. Set "
                f"brain_url to https:// or pass dev_mode=True "
                f"explicitly if this is a local test.",
            )
        headers = [("Authorization", f"Bearer {self._token}")]

        logger.info("z4j agent connecting to %s (project=%s)", url, self.project_id)

        try:
            self._ws = await websockets.connect(
                url,
                additional_headers=headers,
                max_size=self.max_frame_size,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            )
        except InvalidStatus as exc:
            status = exc.response.status_code
            if status == 401:
                raise AuthenticationError(
                    "brain rejected agent token",
                    details={"status": status},
                ) from exc
            raise ConnectionError(f"brain returned HTTP {status}") from exc
        except WebSocketException as exc:
            raise ConnectionError(f"websocket connect failed: {exc}") from exc
        except OSError as exc:
            raise ConnectionError(f"network error: {exc}") from exc

        # Send the hello frame.
        hello = HelloFrame(
            id=self._new_frame_id("hello"),
            payload=HelloPayload(
                protocol_version=CURRENT_PROTOCOL,
                agent_version=self.agent_version,
                framework=self.framework_name,
                engines=self.engines,
                schedulers=self.schedulers,
                capabilities=self.capabilities,
                host={},
            ),
        )

        try:
            await self._ws.send(serialize_frame(hello))
        except WebSocketException as exc:
            await self._close_ws()
            raise ConnectionError(f"failed to send hello frame: {exc}") from exc

        # Wait for hello_ack.
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
        except TimeoutError as exc:
            await self._close_ws()
            raise ConnectionError("timed out waiting for hello_ack") from exc
        except WebSocketException as exc:
            await self._close_ws()
            raise ConnectionError(f"failed to receive hello_ack: {exc}") from exc

        ack = parse_frame(raw)
        if not isinstance(ack, HelloAckFrame):
            await self._close_ws()
            raise InvalidFrameError(
                f"expected hello_ack, got {type(ack).__name__}",
            )

        # Verify the brain's protocol version is one we can speak.
        # Raises ProtocolError on mismatch - caller surfaces this and
        # refuses to retry, since it cannot be fixed by reconnecting.
        try:
            check_compatibility(ack.payload.protocol_version)
        except Exception:
            await self._close_ws()
            raise

        self._session_id = ack.payload.session_id
        self._heartbeat_interval = ack.payload.heartbeat_interval_seconds

        # Protocol v2: every stateful frame carries an envelope HMAC
        # bound to (agent_id, project_id, ts, nonce, seq). The signer
        # and verifier are recreated on every reconnect so the seq
        # counter resets with the session - the brain resets its
        # mirror counter the same way.
        self._signer = FrameSigner(
            secret=self._hmac_secret,
            agent_id=ack.payload.agent_id,
            project_id=ack.payload.project_id,
        )
        self._verifier = FrameVerifier(
            secret=self._hmac_secret,
            agent_id=ack.payload.agent_id,
            project_id=ack.payload.project_id,
            direction="brain->agent",
        )

        # Version compatibility warning.
        brain_ver = getattr(ack.payload, "brain_version", "")
        if brain_ver:
            from z4j_core.version import __version__ as agent_ver

            agent_parts = agent_ver.split(".")[:2]
            brain_parts = brain_ver.split(".")[:2]
            if agent_parts != brain_parts and agent_ver != "0.0.0":
                logger.warning(
                    "z4j agent: version mismatch with brain "
                    "(agent=%s, brain=%s). Upgrade recommended.",
                    agent_ver,
                    brain_ver,
                )

        logger.info(
            "z4j agent connected: session=%s heartbeat_interval=%ss",
            self._session_id,
            self._heartbeat_interval,
        )

    async def close(self) -> None:
        """Close the WebSocket and mark the transport unusable."""
        self._closed = True
        await self._close_ws()

    async def _close_ws(self) -> None:
        ws = self._ws
        self._ws = None
        if ws is None:
            return
        try:
            await ws.close()
        except WebSocketException:  # pragma: no cover
            pass
        except OSError:  # pragma: no cover
            pass

    # ------------------------------------------------------------------
    # Send / receive
    # ------------------------------------------------------------------

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        """Sign + send each buffered frame. Returns accepted indices.

        Each ``frames[i]`` is the unsigned-but-serialised bytes the
        runtime wrote to the local buffer. We parse, hand the typed
        frame to :class:`FrameSigner` so it can stamp a fresh
        ``(ts, nonce, seq)`` and HMAC the envelope, then re-serialise
        and push to the socket. A frame that fails to parse (buffer
        corruption, schema drift) is logged and skipped - we cannot
        silently re-send bytes we never authenticated.

        On partial failure (some frames sent, then a ``ConnectionClosed``
        mid-batch) we raise :class:`PartialSendError` carrying the
        list of indices that *did* make it. The caller is responsible
        for confirming those before retrying the rest, otherwise the
        already-sent frames would be re-sent on reconnect and the
        brain would deduplicate them - wasteful but not incorrect.
        """
        if self._ws is None:
            raise ConnectionError("transport not connected")
        if self._signer is None:
            raise ConnectionError(
                "transport cannot send before hello_ack (signer not built)",
            )

        accepted: list[int] = []
        async with self._send_lock:
            for idx, raw in enumerate(frames):
                try:
                    parsed = parse_frame(raw)
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "z4j transport: dropping unparseable buffered frame",
                    )
                    # Treat as "accepted" so the buffer purges it - a
                    # frame we cannot parse is a frame we cannot ever
                    # deliver, retrying will not help.
                    accepted.append(idx)
                    continue
                if not isinstance(parsed, _SignedFrameBase):
                    logger.error(
                        "z4j transport: refusing to send unsigned %s frame "
                        "on post-handshake stream",
                        getattr(parsed, "type", None),
                    )
                    accepted.append(idx)
                    continue
                try:
                    signed = self._signer.sign_and_serialize(parsed)
                except Exception as exc:  # noqa: BLE001
                    raise ConnectionError(
                        f"frame signing failed: {exc}",
                    ) from exc
                try:
                    await self._ws.send(signed)
                except ConnectionClosed as exc:
                    raise PartialSendError(
                        f"connection closed during send: {exc}",
                        accepted=accepted,
                    ) from exc
                except WebSocketException as exc:
                    raise PartialSendError(
                        f"websocket send failed: {exc}",
                        accepted=accepted,
                    ) from exc
                accepted.append(idx)
        return accepted

    async def receive_frames(
        self,
        on_frame: Callable[[Frame], Awaitable[None]],
    ) -> None:
        """Verify + dispatch inbound frames until the connection closes.

        Every frame is run through :class:`FrameVerifier` first:
        signature, replay, and session-binding checks happen here so
        the :class:`CommandDispatcher` never sees an unauthenticated
        frame. Any :class:`SignatureError` is connection-fatal - a
        peer that sent one forged frame cannot be trusted for the
        rest of the session.
        """
        if self._ws is None:
            raise ConnectionError("transport not connected")
        if self._verifier is None:
            raise ConnectionError(
                "transport cannot receive before hello_ack "
                "(verifier not built)",
            )
        try:
            async for raw in self._ws:
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                try:
                    frame = self._verifier.parse_and_verify(raw)
                except SignatureError as exc:
                    logger.error(
                        "z4j transport: inbound frame failed verification: %s",
                        exc,
                    )
                    await self._close_ws()
                    raise ConnectionError(
                        f"inbound frame verification failed: {exc}",
                    ) from exc
                except Exception as exc:  # noqa: BLE001
                    logger.exception("unexpected error parsing inbound frame")
                    raise InvalidFrameError(
                        f"could not parse inbound frame: {exc}",
                    ) from exc
                try:
                    await on_frame(frame)
                except Z4JError:
                    logger.exception("z4j error handling inbound frame")
                except Exception:  # noqa: BLE001
                    logger.exception("unexpected error handling inbound frame")
        except ConnectionClosed as exc:
            logger.info("z4j agent websocket closed: %s", exc)
        except WebSocketException as exc:
            raise ConnectionError(f"websocket recv failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _new_frame_id(prefix: str) -> str:
        import secrets as _secrets
        return f"{prefix}_{_secrets.token_hex(8)}"


class PartialSendError(ConnectionError):
    """Raised when ``send_frames`` fails part-way through a batch.

    The ``accepted`` attribute lists the frame indices that the
    transport successfully handed to the socket before the failure.
    These frames may or may not have been delivered - the brain
    deduplicates by frame id, so the safe action is to confirm them
    in the local buffer and only retry the unsent tail. Subclasses
    :class:`ConnectionError` so existing callers continue to handle
    this as a transport failure.
    """

    def __init__(self, message: str, *, accepted: list[int]) -> None:
        super().__init__(message)
        self.accepted: list[int] = list(accepted)


__all__ = ["PartialSendError", "WebSocketTransport"]
