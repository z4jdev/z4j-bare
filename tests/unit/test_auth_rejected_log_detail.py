"""The one auth WARNING an operator sees must name the refusal.

"z4j agent auth rejected: brain rejected agent token" reads as "rotate the
token". For an address outside the allowlist (4403, ``ip_denied``) or an
archived project (``project_inactive``) the token is not at fault, and the
rotation costs an outage of its own. The transports carry the close code or
the HTTP status, the brain's error code and its reason text in the error
details; the supervisor's first-in-streak WARNING renders them.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from pathlib import Path

import pytest
from pydantic import SecretStr
from websockets.asyncio.server import ServerConnection, serve
from z4j_bare import runtime as rt
from z4j_bare.runtime import AgentRuntime, _log_disconnect
from z4j_bare.transport.websocket import WebSocketTransport
from z4j_core.errors import AuthenticationError
from z4j_core.models import Config

SUPERVISOR_LOGGER = "z4j.runtime.supervisor"
_PROJECT_ID = "11111111-1111-1111-1111-111111111111"


def _warning(caplog: pytest.LogCaptureFixture, err: BaseException) -> str:
    with caplog.at_level(logging.WARNING, logger=SUPERVISOR_LOGGER):
        _log_disconnect("auth", err, 1)
    records = [r for r in caplog.records if r.name == SUPERVISOR_LOGGER]
    assert len(records) == 1 and records[0].levelno == logging.WARNING
    return records[0].getMessage()


def test_a_websocket_refusal_names_the_close_code_and_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _warning(
        caplog,
        AuthenticationError(
            "brain refused the agent",
            details={"close_code": 4403, "reason": "ip denied"},
        ),
    )
    assert "close code 4403" in message
    assert "ip denied" in message
    assert "Will retry with backoff" in message


def test_a_longpoll_refusal_names_the_status_code_and_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _warning(
        caplog,
        AuthenticationError(
            "brain refused the agent",
            details={
                "status": 403,
                "error": "project_inactive",
                "reason": "the agent's project is archived",
            },
        ),
    )
    assert "HTTP 403" in message
    assert "project_inactive" in message
    assert "the agent's project is archived" in message


def test_a_bare_rejection_reads_as_before(caplog: pytest.LogCaptureFixture) -> None:
    """No details, no suffix: the message an existing log parser knows."""
    message = _warning(caplog, AuthenticationError("brain rejected agent token"))
    assert message.startswith("z4j agent auth rejected: brain rejected agent token. Will retry")


@pytest.mark.parametrize(
    ("details", "expected"),
    [
        ({"close_code": 4401}, " (close code 4401)"),
        ({"status": 401}, " (HTTP 401)"),
        ({"close_code": True, "status": "403", "error": "", "reason": 5}, ""),
        ({}, ""),
    ],
)
def test_the_detail_suffix_renders_only_what_is_there(details: dict, expected: str) -> None:
    from z4j_bare.runtime import _auth_rejection_detail

    assert _auth_rejection_detail(AuthenticationError("x", details=details)) == expected


def test_a_non_z4j_error_has_no_suffix() -> None:
    from z4j_bare.runtime import _auth_rejection_detail

    assert _auth_rejection_detail(RuntimeError("no details attribute")) == ""


# --- the whole path: loopback brain, real transport, real supervisor ------


class _Framework:
    name = "bare"

    def fire_startup(self) -> None:  # pragma: no cover  never reached
        pass


async def _first_backoff(runtime: AgentRuntime, monkeypatch: pytest.MonkeyPatch) -> float:
    """Seconds the real supervisor scheduled after its first failed connect."""
    scheduled: list[float] = []
    real_wait = asyncio.wait

    async def _capture(
        fs: object,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  mirrors the asyncio.wait signature
        return_when: str = asyncio.ALL_COMPLETED,
    ) -> object:
        if timeout is None:
            # The real websockets client waits on its own internals without a
            # timeout; only the supervisor's backoff wait carries one.
            return await real_wait(fs, return_when=return_when)  # type: ignore[arg-type]
        scheduled.append(timeout)
        assert runtime._stop_event is not None
        runtime._stop_event.set()
        return await real_wait(fs, return_when=return_when)  # type: ignore[arg-type]

    monkeypatch.setattr(asyncio, "wait", _capture)
    await runtime._supervise()
    assert len(scheduled) == 1, "expected exactly one reconnect to be scheduled"
    return scheduled[0]


async def test_an_address_denial_at_connect_is_logged_with_its_code_and_backs_off(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A brain that closes 4403 before reading the hello, end to end.

    The close lands on the hello send, the transport classifies it as auth,
    the supervisor sleeps the auth schedule (not the 1 s transient start) and
    its WARNING carries the code and the brain's reason.
    """

    async def deny(ws: ServerConnection) -> None:
        await ws.close(4403, "ip denied")

    server = await serve(deny, "127.0.0.1", 0, close_timeout=1)
    port = server.sockets[0].getsockname()[1]
    try:
        url = f"http://127.0.0.1:{port}"
        config = Config(
            brain_url=url,
            token=SecretStr("test-token-12345678901234567890"),
            project_id=_PROJECT_ID,
            buffer_path=tmp_path / "unused.sqlite",
            dev_mode=True,
            autostart=False,
            hmac_secret=SecretStr(secrets.token_hex(32)),
        )
        runtime = AgentRuntime(config=config, framework=_Framework(), engines=[])  # type: ignore[arg-type]
        runtime._stop_event = asyncio.Event()
        runtime._reconnect_now = asyncio.Event()
        runtime._transport = WebSocketTransport(  # type: ignore[assignment]
            brain_url=url,
            token="tok",
            project_id=_PROJECT_ID,
            framework_name="bare",
            engines=[],
            schedulers=[],
            capabilities={},
            dev_mode=True,
            hmac_secret=secrets.token_bytes(32),
        )
        runtime._dispatcher = object()  # type: ignore[assignment]

        with caplog.at_level(logging.DEBUG, logger=SUPERVISOR_LOGGER):
            delay = await _first_backoff(runtime, monkeypatch)
    finally:
        server.close()
        await server.wait_closed()

    assert delay >= rt._AUTH_RECONNECT_INITIAL
    assert runtime._failure_streaks["auth"] == 1
    assert runtime._failure_streaks["connection"] == 0
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name == SUPERVISOR_LOGGER and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1, warnings
    assert "close code 4403" in warnings[0]
    assert "ip denied" in warnings[0]
