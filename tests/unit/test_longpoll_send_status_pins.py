"""Pinning tests for ``LongPollTransport.send_frames`` status handling.

Defect (1.7 longpoll live-matrix, finding LP-2): ``send_frames``
treats HTTP 429 (and 408 / 425) like any other non-401 4xx and
returns the whole batch as "accepted"
(``transport/longpoll.py:383-393``), which the runtime's send loop
translates into a buffer purge (legacy mode / non-event_batch kinds
confirm immediately, ``runtime.py:1426-1439``). A rate-limited
first-attempt batch is therefore silently LOST even though the brain
never ingested a single event.

The module docstring of ``transport/longpoll.py`` (lines 17-22)
records the original design intent: "the long-poll ack is the HTTP
200 itself". Anything that is NOT a 200 was never an ack and must
never purge.

Tests here (post-fix regression guards):

- Transient statuses (429/408/425) raise the retryable
  ``ConnectionError`` the supervisor backs off on, never purge, and
  honor Retry-After.
- The already-correct behaviors the fix must not regress:
  401 raises AuthenticationError, 5xx raises ConnectionError, and a
  200 consumes the response body's accepted/rejected counts.
"""

from __future__ import annotations

import uuid
from typing import ClassVar

import httpx
import pytest
from z4j_bare.transport import longpoll as lp_mod
from z4j_bare.transport.longpoll import (
    LongPollTransport,
    PayloadTooLargeError,
    UploadContentRejectedError,
    UploadRetryableError,
)
from z4j_core.errors import AuthenticationError
from z4j_core.transport.frames import (
    EventBatchFrame,
    EventBatchPayload,
    serialize_frame,
)

AGENT_UUID = uuid.uuid4()
PROJECT_UUID = uuid.uuid4()
SECRET = b"s" * 32

_IDENTITY_HEADERS = {
    "X-Z4J-Agent-Id": str(AGENT_UUID),
    "X-Z4J-Project-Id": str(PROJECT_UUID),
}

#: Transient statuses that mean "try again later", never "discard".
#: 408 Request Timeout, 425 Too Early, 429 Too Many Requests.
TRANSIENT_4XX = (408, 425, 429)


class _FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        headers: dict | None = None,
        body: dict | None = None,
    ):
        self.status_code = status_code
        self.headers = httpx.Headers(headers or {})
        self._body = body if body is not None else {}

    def json(self) -> dict:
        return self._body


class _FakeAsyncClient:
    """Stands in for httpx.AsyncClient across connect() + send_frames()."""

    get_response: _FakeResponse = _FakeResponse(headers=_IDENTITY_HEADERS)
    post_response: _FakeResponse = _FakeResponse()
    posts: ClassVar[list[dict]] = []

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    async def get(self, *args, **kwargs) -> _FakeResponse:
        return type(self).get_response

    async def post(self, url, *, json=None, **kwargs) -> _FakeResponse:
        type(self).posts.append({"url": url, "json": json})
        return type(self).post_response

    async def aclose(self) -> None:
        return None


@pytest.fixture
def fake_httpx(monkeypatch):
    monkeypatch.setattr(lp_mod.httpx, "AsyncClient", _FakeAsyncClient)
    _FakeAsyncClient.posts = []
    yield _FakeAsyncClient
    _FakeAsyncClient.get_response = _FakeResponse(headers=_IDENTITY_HEADERS)
    _FakeAsyncClient.post_response = _FakeResponse()
    _FakeAsyncClient.posts = []


async def _connected_transport() -> LongPollTransport:
    t = LongPollTransport(
        brain_url="https://brain.example.com",
        token="z4j_agent_test",
        project_id=str(PROJECT_UUID),
        agent_id=str(AGENT_UUID),
        framework_name="bare",
        engines=[],
        schedulers=[],
        capabilities={},
        hmac_secret=SECRET,
    )
    await t.connect()
    return t


def _event_batch_bytes(frame_id: str = "evb_pin_1") -> bytes:
    """A parseable, signable event_batch frame as the buffer stores it."""
    frame = EventBatchFrame(
        id=frame_id,
        payload=EventBatchPayload(
            events=[
                {
                    "engine": "celery",
                    "kind": "task.succeeded",
                    "task_id": "t-1",
                },
            ],
        ),
    )
    return serialize_frame(frame)


# ---------------------------------------------------------------------------
# Fixed behavior: transient statuses keep the batch unconfirmed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", TRANSIENT_4XX)
async def test_transient_4xx_is_retryable_not_purged(
    fake_httpx,
    status: int,
) -> None:
    """LP-2 fix: 429/408/425 raise the retryable ConnectionError.

    Pre-fix these lumped into the "malformed / over quota" purge path
    and returned the whole batch as accepted, so a rate-limited
    first-attempt batch was silently lost. Now they raise
    ConnectionError, which the supervisor turns into
    increment_attempts + reconnect-with-backoff, leaving every entry
    in the buffer for re-send.
    """
    fake_httpx.post_response = _FakeResponse(status_code=status)
    t = await _connected_transport()
    with pytest.raises(ConnectionError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


async def test_retry_after_header_is_honored(fake_httpx, monkeypatch) -> None:
    """A Retry-After on the 429 is slept (capped) before raising."""
    slept: list[float] = []

    async def _fake_sleep(d: float) -> None:
        slept.append(d)

    monkeypatch.setattr(lp_mod.asyncio, "sleep", _fake_sleep)
    fake_httpx.post_response = _FakeResponse(
        status_code=429,
        headers={"Retry-After": "2"},
    )
    t = await _connected_transport()
    with pytest.raises(ConnectionError):
        await t.send_frames([_event_batch_bytes()])
    assert slept and slept[-1] == 2.0
    await t.close()


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_redirects_are_retryable_not_purged(fake_httpx, status: int) -> None:
    """R5-M3: 3xx must not purge.

    follow_redirects is disabled on the client, so a redirect means the
    POST body never reached the events handler. Pre-fix every non-200
    that was not in the small transient set was treated as permanent and
    purged, so 302/307/308 silently dropped the batch. Redirects are now
    retryable (raise ConnectionError, entries stay unconfirmed).
    """
    fake_httpx.post_response = _FakeResponse(status_code=status)
    t = await _connected_transport()
    with pytest.raises(ConnectionError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


@pytest.mark.parametrize("status", [418, 451, 456, 499])
async def test_unexpected_status_is_retryable_not_purged(
    fake_httpx,
    status: int,
) -> None:
    """R5-M3: an unexpected/ambiguous status defaults to retry, not purge.

    Only an explicit allowlist of genuinely-permanent validation
    failures may purge; anything else keeps the batch for retry.
    """
    fake_httpx.post_response = _FakeResponse(status_code=status)
    t = await _connected_transport()
    with pytest.raises(ConnectionError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


@pytest.mark.parametrize("status", [415, 422])
async def test_content_reject_statuses_raise_content_rejected(
    fake_httpx,
    status: int,
) -> None:
    """R6-F6 / R7-MED: a per-FRAME content-validation status is a CONTENT
    reject.

    415 (media type) / 422 (envelope validation) mean the brain looked at
    the frame and rejected it as malformed. Re-sending the identical body
    loops, so these raise ``UploadContentRejectedError`` -- the runtime
    reduces/splits a multi-frame batch so valid siblings still deliver, and
    drops only a SINGLE persistently-rejected frame after a bounded budget.
    They do NOT confirm (delete) the batch; nothing is dropped on the first
    failure. 400 is NOT here (R8-M2): see the routing test below.
    """
    fake_httpx.post_response = _FakeResponse(status_code=status)
    t = await _connected_transport()
    with pytest.raises(UploadContentRejectedError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


@pytest.mark.parametrize("status", [400, 403, 404, 405])
async def test_routing_statuses_are_retryable_not_permanent(
    fake_httpx,
    status: int,
) -> None:
    """R6-F6 / R8-M2: 400/403/404/405 are request-level routing / host-
    validation / WAF / deploy outcomes, NOT proof of bad frame content, so
    they retry (a transient 404 during a deploy, or a host-validation 400
    from an HA allowed-hosts skew, must not lose data). The /agent/events
    route never emits a content-based 400 -- body-shape violations are 422,
    media-type 415 -- so a POST 400 is always request-level and retryable.
    Pre-fix a 400 purged the batch."""
    fake_httpx.post_response = _FakeResponse(status_code=status)
    t = await _connected_transport()
    with pytest.raises(ConnectionError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


async def test_413_raises_payload_too_large(fake_httpx) -> None:
    """R6-F6: 413 must not purge; it signals the batch is too big so the
    runtime can reduce batch size (or drop a single oversized frame)."""
    fake_httpx.post_response = _FakeResponse(status_code=413)
    t = await _connected_transport()
    with pytest.raises(PayloadTooLargeError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


# ---------------------------------------------------------------------------
# PIN: already-correct behaviors the fix must not regress.
# ---------------------------------------------------------------------------


async def test_401_raises_authentication_error(fake_httpx) -> None:
    fake_httpx.post_response = _FakeResponse(status_code=401)
    t = await _connected_transport()
    with pytest.raises(AuthenticationError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


@pytest.mark.parametrize("status", [500, 502, 503])
async def test_5xx_raises_connection_error(fake_httpx, status: int) -> None:
    """5xx already takes the retryable path (longpoll.py:379-382)."""
    fake_httpx.post_response = _FakeResponse(status_code=status)
    t = await _connected_transport()
    with pytest.raises(ConnectionError):
        await t.send_frames([_event_batch_bytes()])
    await t.close()


async def test_200_confirms_only_when_all_frames_stored(fake_httpx) -> None:
    """R6-F1 / R7-HIGH2: a 200 confirms the batch ONLY when the brain
    durably stored EVERY frame it was sent.

    Pre-fix the client confirmed ``accepted + rejected`` indices, so a
    frame the brain never stored (rejected) was deleted from the buffer
    -- the real long-poll loss bug. Now: stored == sent confirms all;
    stored < sent is a TRANSIENT partial store (the brain could not
    persist some frames this instant), so it raises
    ``UploadRetryableError`` and the whole batch re-sends after a backoff
    -- NEVER counting toward any drop budget (the brain dedups the
    already-stored frames on replay).
    """
    # All stored -> confirm all.
    fake_httpx.post_response = _FakeResponse(
        status_code=200,
        body={"accepted": 1, "rejected": 0, "errors": []},
    )
    t = await _connected_transport()
    accepted = await t.send_frames([_event_batch_bytes()])
    assert accepted == [0]

    # Partial store (1 of 2 not stored) -> NEVER confirm; retry whole.
    fake_httpx.post_response = _FakeResponse(
        status_code=200,
        body={"accepted": 1, "rejected": 1, "errors": ["dispatch not durably committed"]},
    )
    with pytest.raises(UploadRetryableError):
        await t.send_frames([_event_batch_bytes("evb_a"), _event_batch_bytes("evb_b")])

    # Nothing stored -> retry whole (the reproduced R6-F1 case:
    # {accepted:0, rejected:1} previously returned index [0] = deleted).
    fake_httpx.post_response = _FakeResponse(
        status_code=200,
        body={"accepted": 0, "rejected": 1, "errors": []},
    )
    with pytest.raises(UploadRetryableError):
        await t.send_frames([_event_batch_bytes("evb_c")])
    await t.close()
