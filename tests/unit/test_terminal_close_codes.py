"""A brain close that reconnecting cannot fix must not be retried forever.

The classifier only special-cased 4401/4403 (auth). Everything else became a
generic ``ConnectionError``, which the runtime bins as ``error_class
= "connection"`` and retries on the normal reconnect backoff.

That default is right for a network blip and wrong for "this agent build is
unacceptable". An agent rejected for its version would reconnect forever against
a brain that refuses it every time.

Why this ships before the brain sends 4427: a brain that started enforcing
version skew today would be rejecting exactly the agents too old to contain this
table, and those agents would storm it. Shipping the client side first is what
makes enforcement safe in a later release. Celery sequenced the v1 -> v2 task
protocol the same way, releasing 3.1.25 to teach the old side to cope before
changing the new side.
"""

from __future__ import annotations

import asyncio
import secrets
from typing import TYPE_CHECKING

import pytest
from pydantic import SecretStr
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close
from z4j_bare import runtime as rt
from z4j_bare.runtime import AgentRuntime
from z4j_bare.transport import websocket as ws_transport
from z4j_bare.transport.websocket import (
    _TERMINAL_CLOSE_CODES,
    WebSocketTransport,
    _ws_close_code,
)
from z4j_core.errors import (
    AgentIncompatibleError,
    AuthenticationError,
    ProtocolError,
)
from z4j_core.models import Config
from z4j_core.transport.frames import (
    ErrorFrame,
    ErrorPayload,
    HelloAckFrame,
    HelloAckPayload,
    serialize_frame,
)
from z4j_core.transport.framing import FrameSigner, FrameVerifier

if TYPE_CHECKING:
    from pathlib import Path


#: The machine code every incompatibility verdict below has to keep carrying.
#:
#: Splitting these failures onto their own exception type changed which backoff
#: schedule the supervisor picks, which was the whole point. It must not also
#: have changed what the failure is CALLED. ``Z4JError.code`` is documented as
#: stable and is published in ``docs/API.md §1.Errors``, so a consumer is
#: entitled to branch on the string, and a rename is the one break that reports
#: itself nowhere: ``except ProtocolError`` keeps matching while
#: ``err.code == "protocol_incompatible"`` silently stops.
#:
#: So every assertion of this constant below is really the same assertion: the
#: transport reclassified a schedule, not a condition. ``z4j_core``'s own suite
#: pins the string against the class; these pin it against the code paths that
#: raise, which is where a swapped exception class would show up.
_PUBLISHED_CODE = "protocol_incompatible"


class _Rcvd:
    def __init__(self, code: int) -> None:
        self.code = code


class _ClosedError(Exception):
    """Stands in for websockets' ConnectionClosed, which carries ``.rcvd``."""

    def __init__(self, code: int) -> None:
        super().__init__(f"closed with {code}")
        self.rcvd = _Rcvd(code)


class _LegacyClosedError(Exception):
    """Older ``websockets`` exposed ``.code`` directly rather than ``.rcvd``."""

    def __init__(self, code: int) -> None:
        super().__init__(f"closed with {code}")
        self.code = code


@pytest.mark.parametrize("code", [4426, 4427])
def test_terminal_codes_are_registered(code: int) -> None:
    """Both unfixable-by-retry codes must be present with a usable message."""
    assert code in _TERMINAL_CLOSE_CODES
    message = _TERMINAL_CLOSE_CODES[code]
    assert message and message == message.strip()


def test_version_skew_message_tells_the_operator_what_to_do() -> None:
    """ "Rejected" is not actionable; "upgrade to within one minor" is."""
    assert "upgrade the agent" in _TERMINAL_CLOSE_CODES[4427]
    assert "one minor" in _TERMINAL_CLOSE_CODES[4427]


@pytest.mark.parametrize(
    "code",
    [
        4401,  # auth: handled by its own branch, must NOT be terminal-protocol
        4403,  # auth
        4429,  # connect rate limit: retrying later is exactly right
        4002,  # replaced by a newer connection: reconnecting is correct
        1000,  # clean shutdown
        1011,  # brain-side internal error: transient
        1006,  # abnormal closure, i.e. a real network blip
    ],
)
def test_retryable_codes_are_not_terminal(code: int) -> None:
    """Over-classifying is the dangerous direction.

    Marking 4429 or 4002 terminal would stop an agent permanently over a rate
    limit or a routine reconnect, which is a worse failure than the storm this
    table exists to prevent.
    """
    assert code not in _TERMINAL_CLOSE_CODES


@pytest.mark.parametrize("exc_cls", [_ClosedError, _LegacyClosedError])
@pytest.mark.parametrize("code", [4426, 4427, 4401, 1006])
def test_close_code_is_extracted_from_both_exception_shapes(
    exc_cls: type[Exception],
    code: int,
) -> None:
    """The lookup is useless if the code cannot be read off the exception.

    ``websockets`` moved the close frame from ``.code`` to ``.rcvd.code``; both
    shapes are in the wild depending on the pinned version.
    """
    assert _ws_close_code(exc_cls(code)) == code


def test_missing_close_code_is_none_not_a_crash() -> None:
    """A bare exception must degrade to "unknown", not raise."""
    assert _ws_close_code(Exception("no close frame")) is None


def test_the_supervisor_gives_incompatibility_a_non_transient_schedule() -> None:
    """The two schedules must not overlap, or "terminal" buys nothing."""
    assert rt._INCOMPATIBLE_RECONNECT_INITIAL > rt._PROTOCOL_RECONNECT_MAX, (
        "an incompatible agent must not start below where transient errors end"
    )
    assert rt._INCOMPATIBLE_RECONNECT_MAX >= 3600.0


def test_the_incompatible_error_is_still_a_protocol_error() -> None:
    """Catchable as ProtocolError for existing handlers, separable for the
    supervisor. Both halves matter, and neither is a claim about the
    transport: what the transport does with it is settled further down, by
    running it."""
    assert issubclass(AgentIncompatibleError, ProtocolError)
    assert AgentIncompatibleError is not ProtocolError


# ---------------------------------------------------------------------------
# Everything below drives the shipped transport and the shipped supervisor.
#
# Constructing an AgentIncompatibleError in the test and asserting things
# about it tests the exception class. It passes unchanged with every raise
# site in the transport reverted to the plain ProtocolError that storms,
# which is the failure this file exists to prevent, so the only assertions
# worth making here are ones that reach the code that decides.
# ---------------------------------------------------------------------------

_AGENT_ID = "22222222-2222-2222-2222-222222222222"
_PROJECT_ID = "11111111-1111-1111-1111-111111111111"
_SESSION_ID = "33333333-3333-3333-3333-333333333333"


class _FakeBrainSocket:
    """The websockets ClientConnection with the network taken out.

    ``recv`` produces what a given brain would have put on the wire -- which
    for a brain that refuses this agent means raising the close frame it sent
    instead of a reply.
    """

    def __init__(self, *replies: bytes | BaseException) -> None:
        self._replies = list(replies)
        self.sent: list[bytes] = []
        self.closed = False

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def recv(self) -> bytes:
        reply = self._replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply

    def __aiter__(self) -> _FakeBrainSocket:
        return self

    async def __anext__(self) -> bytes:
        if not self._replies:
            raise StopAsyncIteration
        return await self.recv()

    async def close(self) -> None:
        self.closed = True


def _brain(monkeypatch: pytest.MonkeyPatch, *replies: bytes | BaseException) -> _FakeBrainSocket:
    """Point the transport's own ``websockets.connect`` at a scripted brain."""
    socket = _FakeBrainSocket(*replies)

    async def _connect(*_args: object, **_kwargs: object) -> _FakeBrainSocket:
        return socket

    monkeypatch.setattr(ws_transport.websockets, "connect", _connect)
    return socket


def _closed_with(code: int) -> ConnectionClosedError:
    """What ``websockets`` raises when the peer closes during the handshake."""
    return ConnectionClosedError(Close(code, "refused"), None)


def _hello_ack(protocol_version: str) -> bytes:
    return serialize_frame(
        HelloAckFrame(
            id="ack_handshake",
            payload=HelloAckPayload(
                protocol_version=protocol_version,
                brain_version="99.0.0",
                agent_id=_AGENT_ID,
                project_id=_PROJECT_ID,
                session_id=_SESSION_ID,
            ),
        ),
    )


def _transport(secret: bytes | None = None) -> WebSocketTransport:
    return WebSocketTransport(
        brain_url="https://brain.example.com",
        token="tok",
        project_id=_PROJECT_ID,
        framework_name="bare",
        engines=[],
        schedulers=[],
        capabilities={},
        hmac_secret=secret or secrets.token_bytes(32),
    )


# --- the handshake -----------------------------------------------------


@pytest.mark.parametrize("code", [4426, 4427])
async def test_a_terminal_close_during_the_handshake_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
    code: int,
) -> None:
    _brain(monkeypatch, _closed_with(code))

    with pytest.raises(AgentIncompatibleError) as caught:
        await _transport().connect()

    assert caught.value.code == _PUBLISHED_CODE


async def test_an_ack_for_a_protocol_this_build_cannot_speak_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The close-code table is not the only way an agent learns it is wrong.

    A brain that completes the upgrade and acks a protocol version this build
    does not implement has said the same thing, and it will say it again on
    every reconnect: which versions each side speaks is a property of the two
    builds, not of the moment.
    """
    _brain(monkeypatch, _hello_ack("99"))

    with pytest.raises(AgentIncompatibleError) as caught:
        await _transport().connect()

    assert "99" in str(caught.value)
    assert caught.value.code == _PUBLISHED_CODE


async def test_a_retryable_close_during_the_handshake_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Over-classifying is the dangerous direction: a connect rate limit that
    became terminal would strand an agent for an hour over a wait."""
    _brain(monkeypatch, _closed_with(4429))

    with pytest.raises(ConnectionError) as caught:
        await _transport().connect()

    assert not isinstance(caught.value, ProtocolError)


async def test_a_supported_ack_completes_the_handshake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control for the three above.

    Without it, a fake socket that broke the handshake for some unrelated
    reason would make every rejection test pass for the wrong reason.
    """
    from z4j_core.transport.versioning import CURRENT_PROTOCOL

    socket = _brain(monkeypatch, _hello_ack(CURRENT_PROTOCOL))
    transport = _transport()

    await transport.connect()

    assert transport.session_id == _SESSION_ID
    assert socket.closed is False
    assert len(socket.sent) == 1  # the hello frame


# --- an established session --------------------------------------------


async def test_a_fatal_upgrade_error_frame_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The brain can also deliver the verdict after the handshake.

    The frame that provoked it is still at the head of the agent's buffer, so
    a reconnect replays it and earns the same answer. Retrying that on the
    transient schedule is the same permanent storm.
    """
    secret = secrets.token_bytes(32)
    signer = FrameSigner(
        secret=secret,
        agent_id=_AGENT_ID,
        project_id=_PROJECT_ID,
        session_id=_SESSION_ID,
    )
    fatal = signer.sign_and_serialize(
        ErrorFrame(
            id="err_upgrade",
            payload=ErrorPayload(
                code="scheduler_upgrade_required",
                message="schedule projection requires a current scheduler adapter",
                fatal=True,
            ),
        ),
    )
    transport = _transport(secret)
    transport._ws = _FakeBrainSocket(fatal)  # type: ignore[assignment]
    transport._verifier = FrameVerifier(
        secret=secret,
        agent_id=_AGENT_ID,
        project_id=_PROJECT_ID,
        session_id=_SESSION_ID,
        direction="brain->agent",
    )

    async def _unused(_frame: object) -> None:  # pragma: no cover - never reached
        raise AssertionError("a fatal frame must not reach the dispatcher")

    with pytest.raises(AgentIncompatibleError) as caught:
        await transport.receive_frames(_unused)

    assert caught.value.code == _PUBLISHED_CODE


async def test_a_fatal_error_frame_a_retry_could_clear_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only codes that name a BUILD are terminal; the rest keep the default."""
    secret = secrets.token_bytes(32)
    signer = FrameSigner(
        secret=secret,
        agent_id=_AGENT_ID,
        project_id=_PROJECT_ID,
        session_id=_SESSION_ID,
    )
    fatal = signer.sign_and_serialize(
        ErrorFrame(
            id="err_internal",
            payload=ErrorPayload(
                code="internal_error",
                message="the brain fell over",
                fatal=True,
            ),
        ),
    )
    transport = _transport(secret)
    transport._ws = _FakeBrainSocket(fatal)  # type: ignore[assignment]
    transport._verifier = FrameVerifier(
        secret=secret,
        agent_id=_AGENT_ID,
        project_id=_PROJECT_ID,
        session_id=_SESSION_ID,
        direction="brain->agent",
    )

    async def _unused(_frame: object) -> None:  # pragma: no cover - never reached
        raise AssertionError("a fatal frame must not reach the dispatcher")

    with pytest.raises(ProtocolError) as caught:
        await transport.receive_frames(_unused)

    assert not isinstance(caught.value, AgentIncompatibleError)
    # Same published code as the terminal case above, deliberately. The
    # exception TYPE is what the supervisor sorts on; the code names the
    # condition for consumers and does not encode the schedule. Reading the two
    # together is the point -- a reader who sees only one could reasonably
    # assume the code is the discriminator and start branching on it.
    assert caught.value.code == _PUBLISHED_CODE


async def test_an_established_revoked_session_enters_auth_backoff() -> None:
    """A 4003 revoke must unwind the session, not end only its receive task."""
    secret = secrets.token_bytes(32)
    socket = _FakeBrainSocket(_closed_with(4003))
    transport = _transport(secret)
    transport._ws = socket  # type: ignore[assignment]
    transport._verifier = FrameVerifier(
        secret=secret,
        agent_id=_AGENT_ID,
        project_id=_PROJECT_ID,
        session_id=_SESSION_ID,
        direction="brain->agent",
    )

    async def _unused(_frame: object) -> None:  # pragma: no cover - never reached
        raise AssertionError("a close frame must not reach the dispatcher")

    with pytest.raises(AuthenticationError) as caught:
        await transport.receive_frames(_unused)

    assert caught.value.details == {"close_code": 4003}
    assert socket.closed is True
    assert transport._ws is None


async def test_clean_receive_exhaustion_unwinds_the_session() -> None:
    """Clean iterator exhaustion must not leave TaskGroup siblings running."""
    secret = secrets.token_bytes(32)
    socket = _FakeBrainSocket()
    transport = _transport(secret)
    transport._ws = socket  # type: ignore[assignment]
    transport._verifier = FrameVerifier(
        secret=secret,
        agent_id=_AGENT_ID,
        project_id=_PROJECT_ID,
        session_id=_SESSION_ID,
        direction="brain->agent",
    )

    async def _unused(_frame: object) -> None:  # pragma: no cover - never reached
        raise AssertionError("an empty stream must not reach the dispatcher")

    with pytest.raises(ConnectionError, match="ended without a close frame"):
        await transport.receive_frames(_unused)

    assert socket.closed is True
    assert transport._ws is None


# --- the supervisor, end to end ----------------------------------------


class _FakeFramework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover - never reached
        pass


def _runtime(tmp_path: Path, transport: WebSocketTransport) -> AgentRuntime:
    config = Config(
        brain_url="https://brain.example.com",
        token=SecretStr("test-token-12345678901234567890"),
        project_id=_PROJECT_ID,
        buffer_path=tmp_path / "unused.sqlite",
        dev_mode=True,
        autostart=False,
        hmac_secret=SecretStr(secrets.token_hex(32)),
    )
    runtime = AgentRuntime(
        config=config,
        framework=_FakeFramework(),  # type: ignore[arg-type]
        engines=[],
    )
    runtime._stop_event = asyncio.Event()
    runtime._reconnect_now = asyncio.Event()
    runtime._transport = transport  # type: ignore[assignment]
    runtime._dispatcher = object()  # type: ignore[assignment]
    return runtime


async def _first_backoff(runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch) -> float:
    """Seconds the supervisor scheduled after its first failed connect.

    Read off the real ``_supervise`` rather than recomputed here, because the
    number this file cares about is the one an agent in the field would
    actually sleep. The loop is stopped at the wait instead of sleeping it
    out; the schedule under test starts at two minutes.
    """
    scheduled: list[float] = []
    real_wait = asyncio.wait

    async def _capture(
        fs: object,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  mirrors the asyncio.wait signature it stands in for
        return_when: str = asyncio.ALL_COMPLETED,
    ) -> object:
        scheduled.append(timeout)  # type: ignore[arg-type]
        assert runtime._stop_event is not None
        runtime._stop_event.set()
        return await real_wait(fs, return_when=return_when)  # type: ignore[arg-type]

    monkeypatch.setattr(asyncio, "wait", _capture)
    await runtime._supervise()

    assert len(scheduled) == 1, "expected exactly one reconnect to be scheduled"
    return scheduled[0]


async def test_an_unspeakable_ack_backs_off_past_the_transient_ceiling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The whole path: scripted brain, real transport, real supervisor.

    The storm is a number, so this asserts the number. A protocol-classed
    failure sleeps at most a minute; this must sleep longer than any transient
    class ever will.
    """
    _brain(monkeypatch, _hello_ack("99"))
    runtime = _runtime(tmp_path, _transport())

    delay = await _first_backoff(runtime, monkeypatch)

    assert delay >= rt._INCOMPATIBLE_RECONNECT_INITIAL
    assert delay > rt._PROTOCOL_RECONNECT_MAX
    assert runtime._failure_streaks["incompatible"] == 1
    assert runtime._failure_streaks["protocol"] == 0
    assert runtime._failure_streaks["connection"] == 0


@pytest.mark.parametrize("code", [4426, 4427])
async def test_a_terminal_close_backs_off_past_the_transient_ceiling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    code: int,
) -> None:
    _brain(monkeypatch, _closed_with(code))
    runtime = _runtime(tmp_path, _transport())

    delay = await _first_backoff(runtime, monkeypatch)

    assert delay > rt._PROTOCOL_RECONNECT_MAX
    assert runtime._failure_streaks["incompatible"] == 1


async def test_a_transient_close_still_reconnects_promptly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The negative control for the two above.

    A harness that reported a long backoff for everything would pass them
    both while proving nothing.
    """
    _brain(monkeypatch, _closed_with(4429))
    runtime = _runtime(tmp_path, _transport())

    delay = await _first_backoff(runtime, monkeypatch)

    assert delay < rt._INCOMPATIBLE_RECONNECT_INITIAL
    assert runtime._failure_streaks["connection"] == 1
    assert runtime._failure_streaks["incompatible"] == 0
