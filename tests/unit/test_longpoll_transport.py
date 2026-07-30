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
from z4j_core.transport import RETRY_BY_REFERENCE_CAPABILITY

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


async def test_adapter_retry_contract_rides_every_longpoll_request(
    fake_httpx,
) -> None:
    fake_httpx.next_response = _FakeResponse(
        headers={
            "X-Z4J-Agent-Id": str(AGENT_UUID),
            "X-Z4J-Project-Id": str(PROJECT_UUID),
        }
    )
    transport = LongPollTransport(
        brain_url="https://brain.example.com",
        token="z4j_agent_test",
        project_id=str(PROJECT_UUID),
        agent_id=str(AGENT_UUID),
        framework_name="bare",
        engines=["celery", "rq"],
        schedulers=[],
        capabilities={
            "celery": ["retry_task", RETRY_BY_REFERENCE_CAPABILITY],
            "rq": ["retry_task"],
        },
        hmac_secret=SECRET,
    )

    await transport.connect()
    assert transport._client is not None
    assert transport._client.kwargs["headers"]["X-Z4J-Retry-Contracts"] == "celery=1"
    await transport.close()


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


async def test_probe_falls_back_to_max_frames_1_on_422_n1_r10_h1(monkeypatch) -> None:
    # (N-1): a pre-1.7.1 brain declares max_frames ge=1 and 422s the
    # non-claiming max_frames=0 probe in FastAPI query validation. The agent must
    # fall back to max_frames=1 (accepted by that brain) and still learn its
    # identity, instead of failing to connect to a 1.7.0 brain entirely.
    calls: list[int] = []

    class _FallbackClient:
        def __init__(self, *a, **k) -> None: ...

        async def get(self, *a, **k) -> _FakeResponse:
            mf = k["params"]["max_frames"]
            calls.append(mf)
            if mf == 0:
                return _FakeResponse(status_code=422)
            return _FakeResponse(
                headers={
                    "X-Z4J-Agent-Id": str(AGENT_UUID),
                    "X-Z4J-Project-Id": str(PROJECT_UUID),
                },
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(lp_mod.httpx, "AsyncClient", _FallbackClient)
    t = _transport(project_id="my-project")
    await t.connect()
    assert calls == [0, 1]  # tried the non-claiming probe, fell back on 422
    assert t._signer._project_id == str(PROJECT_UUID)
    await t.close()


async def test_probe_does_not_fall_back_on_a_1_7_1_brain_r10_h1(monkeypatch) -> None:
    # A 1.7.1 brain answers max_frames=0 with 200 (non-claiming), so the agent
    # never sends the claiming max_frames=1 fallback.
    calls: list[int] = []

    class _OkClient:
        def __init__(self, *a, **k) -> None: ...

        async def get(self, *a, **k) -> _FakeResponse:
            calls.append(k["params"]["max_frames"])
            return _FakeResponse(
                headers={
                    "X-Z4J-Agent-Id": str(AGENT_UUID),
                    "X-Z4J-Project-Id": str(PROJECT_UUID),
                },
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(lp_mod.httpx, "AsyncClient", _OkClient)
    t = _transport(project_id="my-project")
    await t.connect()
    assert calls == [0]  # non-claiming probe only, no fallback
    await t.close()
