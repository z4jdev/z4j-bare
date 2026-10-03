"""A long-poll 403 that names the agent's standing is an auth failure.

The brain answers 403 with ``error: project_inactive`` for an agent whose
project is archived and ``error: ip_denied`` for a source address outside
the agent allowlist. The bearer is valid in both cases, and nothing the
agent does on its own changes the answer. The transport classed every
non-401 status as transient, so such an agent reconnected every 1 to 30 s
forever; the WebSocket path gives the same two verdicts (the 4401 and 4403
closes) the auth backoff of 10 s to 600 s.

Every test here drives the shipped transport against a stub HTTP brain on
loopback, not an httpx stand-in, on all three request paths: the connect
probe, the events POST and the command poll. A 403 with any other body and
a 500 are the negative controls: they must stay transient, because
over-classifying would park an agent for ten minutes over a WAF hiccup.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from z4j_bare.transport.longpoll import LongPollTransport
from z4j_core.errors import AuthenticationError
from z4j_core.transport.frames import (
    EventBatchFrame,
    EventBatchPayload,
    serialize_frame,
)

AGENT_UUID = uuid.uuid4()
PROJECT_UUID = uuid.uuid4()
SECRET = b"s" * 32
TRANSPORT_LOGGER = "z4j.transport.longpoll"

_ARCHIVED = {
    "error": "project_inactive",
    "message": "the agent's project is archived",
    "details": {"project_id": str(PROJECT_UUID), "agent_id": str(AGENT_UUID)},
}
_DENIED = {
    "error": "ip_denied",
    "message": "source address is not admitted",
    "details": {"surface": "agent"},
}

#: (status, body, verdict). ``None`` is an empty, non-JSON body.
CASES = [
    pytest.param(401, {"error": "unauthenticated", "message": "bad bearer"}, "auth", id="401"),
    pytest.param(403, _ARCHIVED, "auth", id="403-project_inactive"),
    pytest.param(403, _DENIED, "auth", id="403-ip_denied"),
    pytest.param(403, {"detail": "Forbidden"}, "transient", id="403-proxy-body"),
    pytest.param(403, {"error": "forbidden", "message": "no"}, "transient", id="403-other-code"),
    pytest.param(403, None, "transient", id="403-no-body"),
    pytest.param(500, {"error": "internal_error"}, "transient", id="500"),
]

_Reply = tuple[int, dict[str, Any] | None]


class _StubBrain:
    """A loopback HTTP brain answering each route from a script.

    The probe (``max_frames=0``) always succeeds with the identity headers so
    a session can be built; the poll and the events POST answer with whatever
    the test scripted. Every response closes the connection, so each request
    is a fresh TCP connection and no earlier reply can be mistaken for a
    later one.
    """

    def __init__(self) -> None:
        self.probe: _Reply = (200, {"frames": []})
        self.poll: _Reply = (200, {"frames": []})
        self.post: _Reply = (200, {"accepted": 1})
        self.requests: list[tuple[str, str]] = []
        self.port = 0
        self._server: asyncio.AbstractServer | None = None

    async def __aenter__(self) -> _StubBrain:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            method, target, _ = request_line.decode().split(" ", 2)
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode().partition(":")
                headers[name.strip().lower()] = value.strip()
            length = int(headers.get("content-length") or 0)
            if length:
                await reader.readexactly(length)
            self.requests.append((method, target))

            extra = ""
            if method == "POST":
                status, body = self.post
            elif "max_frames=50" in target:
                status, body = self.poll
            else:
                status, body = self.probe
                extra = f"X-Z4J-Agent-Id: {AGENT_UUID}\r\nX-Z4J-Project-Id: {PROJECT_UUID}\r\n"
            payload = b"" if body is None else json.dumps(body).encode()
            writer.write(
                (
                    f"HTTP/1.1 {status} Stub\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(payload)}\r\n"
                    f"Connection: close\r\n{extra}\r\n"
                ).encode()
                + payload
            )
            await writer.drain()
        finally:
            writer.close()


def _transport(url: str) -> LongPollTransport:
    return LongPollTransport(
        brain_url=url,
        token="z4j_agent_test",
        project_id=str(PROJECT_UUID),
        agent_id=str(AGENT_UUID),
        framework_name="bare",
        engines=[],
        schedulers=[],
        capabilities={},
        hmac_secret=SECRET,
        dev_mode=True,
        poll_wait_seconds=1,
    )


def _event_batch_bytes() -> bytes:
    frame = EventBatchFrame(
        id="evb_403_1",
        payload=EventBatchPayload(
            events=[{"engine": "celery", "kind": "task.succeeded", "task_id": "t-1"}],
        ),
    )
    return serialize_frame(frame)


async def _outcome(action: Callable[[], Awaitable[object]]) -> BaseException:
    try:
        await action()
    except Exception as exc:  # the class IS the assertion
        return exc
    raise AssertionError("the stub brain refused the request, so it cannot succeed")


def _assert_verdict(exc: BaseException, verdict: str, status: int, body: dict | None) -> None:
    if verdict == "auth":
        assert isinstance(exc, AuthenticationError), repr(exc)
        assert exc.details["status"] == status
        if status == 403:
            assert body is not None
            assert exc.details["error"] == body["error"]
            assert exc.details["reason"] == body["message"]
        return
    # Transient: the supervisor's 1 s to 60 s schedule, never the auth one.
    assert isinstance(exc, ConnectionError), repr(exc)
    assert not isinstance(exc, AuthenticationError)


@pytest.mark.parametrize(("status", "body", "verdict"), CASES)
async def test_connect_probe_classifies(status: int, body: dict | None, verdict: str) -> None:
    async with _StubBrain() as brain:
        brain.probe = (status, body)
        transport = _transport(brain.url)
        try:
            exc = await _outcome(transport.connect)
        finally:
            await transport.close()
    _assert_verdict(exc, verdict, status, body)
    assert transport._client is None, "a refused probe must release the HTTP client"


@pytest.mark.parametrize(("status", "body", "verdict"), CASES)
async def test_events_post_classifies(status: int, body: dict | None, verdict: str) -> None:
    async with _StubBrain() as brain:
        transport = _transport(brain.url)
        await transport.connect()
        brain.post = (status, body)
        try:
            exc = await _outcome(lambda: transport.send_frames([_event_batch_bytes()]))
        finally:
            await transport.close()
    _assert_verdict(exc, verdict, status, body)
    assert any(method == "POST" for method, _ in brain.requests)


@pytest.mark.parametrize(("status", "body", "verdict"), CASES)
async def test_command_poll_classifies(status: int, body: dict | None, verdict: str) -> None:
    async def _never(_frame: object) -> None:  # pragma: no cover  the poll fails first
        raise AssertionError("no frame can arrive from a refused poll")

    async with _StubBrain() as brain:
        transport = _transport(brain.url)
        await transport.connect()
        brain.poll = (status, body)
        try:
            exc = await _outcome(lambda: transport.receive_frames(_never))
        finally:
            await transport.close()
    _assert_verdict(exc, verdict, status, body)
    assert any("max_frames=50" in target for _, target in brain.requests)


async def test_a_refusal_logs_the_code_and_the_brains_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The transport's own line carries what the brain said, so a DEBUG
    trace of every attempt is enough to tell an archived project from a
    rotated token without the supervisor's summary."""
    async with _StubBrain() as brain:
        brain.probe = (403, _ARCHIVED)
        transport = _transport(brain.url)
        with caplog.at_level(logging.INFO, logger=TRANSPORT_LOGGER):
            try:
                exc = await _outcome(transport.connect)
            finally:
                await transport.close()
    assert isinstance(exc, AuthenticationError)
    lines = [r.getMessage() for r in caplog.records if r.name == TRANSPORT_LOGGER]
    assert any("project_inactive" in line and _ARCHIVED["message"] in line for line in lines), lines


async def test_a_healthy_brain_still_connects() -> None:
    """The control: the stub is a working brain when it is not scripted to
    refuse, so the refusals above are the transport's verdicts, not an
    artefact of the stub."""
    async with _StubBrain() as brain:
        transport = _transport(brain.url)
        try:
            await transport.connect()
            assert transport.session_id is not None
            accepted = await transport.send_frames([_event_batch_bytes()])
        finally:
            await transport.close()
    assert accepted == [0]
