"""Long-poll transport signing-identity regression tests (B13).

The fleet-readiness B13 finding: ``Config.project_id`` is a SLUG, and
the pre-fix transport coerced any non-UUID value to a RANDOM ``uuid4``
before binding it into the frame-HMAC envelope, while the brain binds
the real project UUID from the agent's DB row. Every frame therefore
failed signature verification in both directions on the documented
configuration path.

The fix under test: the brain advertises the canonical UUIDs on the
probe response (``X-Z4J-Agent-Id`` / ``X-Z4J-Project-Id``) and the
transport binds those, falling back to config values only when they
are themselves UUIDs.
"""

from __future__ import annotations

import logging
import uuid

import httpx
import pytest
from z4j_bare.transport import longpoll as lp_mod
from z4j_bare.transport.longpoll import LongPollTransport, _uuid_or_none

AGENT_UUID = uuid.uuid4()
PROJECT_UUID = uuid.uuid4()
SECRET = b"s" * 32


class _FakeResponse:
    def __init__(self, status_code: int = 200, headers: dict | None = None):
        self.status_code = status_code
        # httpx.Headers gives the case-insensitive lookup the
        # transport relies on; a plain dict would mask a
        # header-casing bug.
        self.headers = httpx.Headers(headers or {})


class _FakeAsyncClient:
    """Stands in for httpx.AsyncClient inside connect()."""

    next_response: _FakeResponse = _FakeResponse()

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    async def get(self, *args, **kwargs) -> _FakeResponse:
        return type(self).next_response

    async def aclose(self) -> None:
        return None


def _transport(*, project_id: str, agent_id: str = "") -> LongPollTransport:
    return LongPollTransport(
        brain_url="https://brain.example.com",
        token="z4j_agent_test",
        project_id=project_id,
        agent_id=agent_id,
        framework_name="bare",
        engines=[],
        schedulers=[],
        capabilities={},
        hmac_secret=SECRET,
    )


@pytest.fixture
def fake_httpx(monkeypatch):
    monkeypatch.setattr(lp_mod.httpx, "AsyncClient", _FakeAsyncClient)
    yield _FakeAsyncClient
    _FakeAsyncClient.next_response = _FakeResponse()


def test_uuid_or_none_parses_and_rejects() -> None:
    u = uuid.uuid4()
    assert _uuid_or_none(u) is u
    assert _uuid_or_none(str(u)) == u
    assert _uuid_or_none("my-project") is None  # the documented slug shape
    assert _uuid_or_none("") is None
    assert _uuid_or_none(None) is None


async def test_probe_headers_bind_canonical_ids_with_slug_config(
    fake_httpx,
) -> None:
    """THE B13 regression: slug config + identity headers -> real UUIDs.

    Pre-fix this bound a random uuid4 as the project id, so the signer
    could never produce a frame the brain's verifier (bound to the DB
    row's UUID) would accept.
    """
    fake_httpx.next_response = _FakeResponse(
        headers={
            "X-Z4J-Agent-Id": str(AGENT_UUID),
            "X-Z4J-Project-Id": str(PROJECT_UUID),
        },
    )
    t = _transport(project_id="my-project")
    await t.connect()
    assert t._signer is not None
    assert t._signer._agent_id == str(AGENT_UUID)
    assert t._signer._project_id == str(PROJECT_UUID)
    assert t._verifier._agent_id == str(AGENT_UUID)
    assert t._verifier._project_id == str(PROJECT_UUID)
    await t.close()


async def test_uuid_config_binds_when_brain_sends_no_headers(
    fake_httpx,
) -> None:
    """Old brain (no identity headers) + UUID-configured agent works."""
    fake_httpx.next_response = _FakeResponse(headers={})
    t = _transport(project_id=str(PROJECT_UUID), agent_id=str(AGENT_UUID))
    await t.connect()
    assert t._signer._agent_id == str(AGENT_UUID)
    assert t._signer._project_id == str(PROJECT_UUID)
    await t.close()


async def test_slug_config_without_headers_warns_loudly(
    fake_httpx,
    caplog,
) -> None:
    """Old brain + slug config cannot work; it must SAY so, not die
    silently in a SignatureError loop."""
    fake_httpx.next_response = _FakeResponse(headers={})
    t = _transport(project_id="my-project")
    with caplog.at_level(logging.WARNING, logger="z4j.transport.longpoll"):
        await t.connect()
    assert any("frame signatures will not verify" in rec.message for rec in caplog.records)
    # Still connects (legacy behavior) so close() etc. keep working.
    assert t._signer is not None
    await t.close()


async def test_headers_win_over_uuid_config(fake_httpx) -> None:
    """The brain's DB row is canonical even when the config carries
    plausible (but different) UUIDs."""
    fake_httpx.next_response = _FakeResponse(
        headers={
            "X-Z4J-Agent-Id": str(AGENT_UUID),
            "X-Z4J-Project-Id": str(PROJECT_UUID),
        },
    )
    t = _transport(
        project_id=str(uuid.uuid4()),
        agent_id=str(uuid.uuid4()),
    )
    await t.connect()
    assert t._signer._agent_id == str(AGENT_UUID)
    assert t._signer._project_id == str(PROJECT_UUID)
    await t.close()
