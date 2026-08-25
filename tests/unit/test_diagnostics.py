"""Behavioral contracts for agent diagnostics."""

from __future__ import annotations

import asyncio
import base64
import secrets
import threading
from pathlib import Path

from pydantic import SecretStr
from z4j_bare import control, diagnostics
from z4j_bare.runtime import AgentRuntime
from z4j_core.errors import AuthenticationError
from z4j_core.models import Config


def _config_with_bad_remote_credentials(tmp_path: Path) -> Config:
    hmac_secret = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")
    return Config(
        brain_url="https://brain.example.com",
        token=SecretStr("definitely-invalid-token-000000000000000"),
        project_id="wrong-project",
        buffer_path=tmp_path / "diagnostic-buffer.sqlite",
        dev_mode=False,
        autostart=False,
        hmac_secret=SecretStr(hmac_secret),
    )


def test_websocket_probe_success_does_not_claim_remote_authentication(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A supervisor auth rejection is not part of start()'s success result."""
    rejected = threading.Event()

    async def _reject_bad_credentials(self: AgentRuntime) -> None:
        rejected.set()
        raise AuthenticationError("test brain rejected token")

    original_start = AgentRuntime.start

    def _start_and_observe_rejection(self: AgentRuntime) -> None:
        original_start(self)
        assert rejected.wait(timeout=1.0), "supervisor did not exercise the auth rejection"

    monkeypatch.setenv("Z4J_HEARTBEAT", "0")
    monkeypatch.setattr(AgentRuntime, "_connect_and_run", _reject_bad_credentials)
    monkeypatch.setattr(AgentRuntime, "start", _start_and_observe_rejection)
    monkeypatch.setattr(control, "write_pidfile", lambda _adapter: tmp_path / "agent.pid")
    monkeypatch.setattr(control, "install_sighup_handler", lambda _runtime: None)

    result = asyncio.run(
        diagnostics._probe_websocket_async(
            _config_with_bad_remote_credentials(tmp_path),
            timeout=2.0,
        )
    )

    assert result.ok is True
    assert result.details == {
        "remote_connection_verified": False,
        "remote_authentication_verified": False,
    }
    assert "remote connection and authentication were not verified" in result.message
    assert "ws upgrade" not in result.message.lower()
