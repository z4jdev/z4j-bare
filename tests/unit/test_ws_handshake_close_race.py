"""The handshake verdict must not depend on which call saw the close frame.

The brain accepts the HTTP upgrade and then closes: 4401 for a bad or
revoked token, 4403 for a source address outside the agent allowlist, 4426
and 4427 for a build it refuses. If the close frame is processed before the
client's hello ``send()``, the send raises ``ConnectionClosed``; otherwise
the ``recv()`` of the hello_ack does. Only the recv branch mapped the code,
so a brain that closes before reading the hello, which the allowlist path
does every time, surfaced as a bare ConnectionError on the fast reconnect
schedule, reproduced 20 of 20 on loopback with an immediate close.

These tests run the shipped transport against a real ``websockets`` server
on loopback so the race is the real one. Twenty rounds per code: one run
proves a timing, twenty prove the table runs on both branches.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from collections.abc import Awaitable, Callable

import pytest
from websockets.asyncio.server import Server, ServerConnection, serve
from z4j_bare.transport.websocket import WebSocketTransport
from z4j_core.errors import AgentIncompatibleError, AuthenticationError, ProtocolError

_PROJECT_ID = "11111111-1111-1111-1111-111111111111"
ROUNDS = 20

Handler = Callable[[ServerConnection], Awaitable[None]]


class _Brain:
    """A loopback brain that closes each socket the way ``handler`` says."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self._server: Server | None = None
        self.port = 0

    async def __aenter__(self) -> _Brain:
        self._server = await serve(self._handler, "127.0.0.1", 0, close_timeout=1)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()


def _close_at_once(code: int, reason: str) -> Handler:
    """Close before reading anything: the allowlist refusal's shape."""

    async def handler(ws: ServerConnection) -> None:
        await ws.close(code, reason)

    return handler


def _close_after_hello(code: int, reason: str) -> Handler:
    """Read the hello first, then close: the bearer refusal's usual shape."""

    async def handler(ws: ServerConnection) -> None:
        # Whatever the read does, the close that follows is the point.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(ws.recv(), timeout=2)
        await ws.close(code, reason)

    return handler


async def _connect_outcome(port: int) -> BaseException:
    transport = WebSocketTransport(
        brain_url=f"http://127.0.0.1:{port}",
        token="tok",
        project_id=_PROJECT_ID,
        framework_name="bare",
        engines=[],
        schedulers=[],
        capabilities={},
        dev_mode=True,
        hmac_secret=b"x" * 32,
    )
    try:
        await transport.connect()
    except Exception as exc:  # the class IS the assertion
        return exc
    finally:
        await transport.close()
    raise AssertionError("the stub brain closed the socket, so connect() cannot succeed")


async def _outcomes(handler: Handler, rounds: int = ROUNDS) -> list[BaseException]:
    async with _Brain(handler) as brain:
        return [await _connect_outcome(brain.port) for _ in range(rounds)]


def _tally(outcomes: list[BaseException]) -> dict[str, int]:
    return dict(Counter(type(exc).__name__ for exc in outcomes))


@pytest.mark.parametrize(("code", "reason"), [(4403, "ip denied"), (4401, "bad bearer")])
async def test_a_close_before_the_hello_is_read_is_auth_every_time(
    code: int,
    reason: str,
) -> None:
    outcomes = await _outcomes(_close_at_once(code, reason))

    assert all(isinstance(exc, AuthenticationError) for exc in outcomes), _tally(outcomes)
    for exc in outcomes:
        assert isinstance(exc, AuthenticationError)
        assert exc.details["close_code"] == code
        assert exc.details["reason"] == reason


async def test_a_close_after_the_hello_is_auth() -> None:
    """Positive control: the branch that always classified still does."""
    outcomes = await _outcomes(_close_after_hello(4401, "bad bearer"), rounds=3)

    assert all(isinstance(exc, AuthenticationError) for exc in outcomes), _tally(outcomes)
    assert all(exc.details["close_code"] == 4401 for exc in outcomes)  # type: ignore[attr-defined]


async def test_a_terminal_close_before_the_hello_is_terminal() -> None:
    """The same table carries the build verdicts, on the same branch."""
    outcomes = await _outcomes(_close_at_once(4427, "agent too old"), rounds=5)

    assert all(isinstance(exc, AgentIncompatibleError) for exc in outcomes), _tally(outcomes)


@pytest.mark.parametrize(("code", "reason"), [(1011, "brain fell over"), (4429, "rate limited")])
async def test_a_retryable_close_before_the_hello_stays_transient(
    code: int,
    reason: str,
) -> None:
    """Negative control: over-classifying is the dangerous direction.

    A brain-side error or a connect rate limit parked on the ten-minute auth
    schedule would be a worse failure than the storm the table prevents.
    """
    outcomes = await _outcomes(_close_at_once(code, reason), rounds=5)

    for exc in outcomes:
        assert isinstance(exc, ConnectionError), _tally(outcomes)
        assert not isinstance(exc, AuthenticationError)
        assert not isinstance(exc, ProtocolError)


def test_the_4403_verdict_does_not_blame_the_token() -> None:
    """An operator who reads "rejected agent token" rotates the token, and
    an address denial survives the rotation."""
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close
    from z4j_bare.transport.websocket import _handshake_rejection

    denied = _handshake_rejection(ConnectionClosedError(Close(4403, "ip denied"), None))
    bearer = _handshake_rejection(ConnectionClosedError(Close(4401, "bad bearer"), None))

    assert isinstance(denied, AuthenticationError)
    assert "token" not in str(denied)
    assert isinstance(bearer, AuthenticationError)
    assert "token" in str(bearer)
