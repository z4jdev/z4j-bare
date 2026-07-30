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

import pytest
from z4j_bare.transport.websocket import _TERMINAL_CLOSE_CODES, _ws_close_code


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
