"""Transport layer - how the agent talks to the brain.

Two implementations, selected by configuration:

- :class:`z4j_bare.transport.websocket.WebSocketTransport` - the
  primary transport. A long-lived bi-directional WebSocket to the
  brain at ``/ws/agent``. Preferred in production.
- :class:`z4j_bare.transport.longpoll.LongPollTransport` - an
  explicitly selected alternative. HTTPS POST for uploads + long-poll
  GET for commands. Use it when WebSocket is blocked by corporate
  proxies. The runtime does not switch to it automatically.

Both transports implement the same :class:`Transport` Protocol so the
rest of the agent runtime does not care which one is in use.

See ``docs/API.md §5`` for the wire protocol reference.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from z4j_core.transport.frames import Frame

from z4j_bare.transport.websocket import WebSocketTransport


@runtime_checkable
class Transport(Protocol):
    """Bi-directional wire transport between agent and brain.

    All methods are async because they perform network I/O. The
    contract is:

    1. :meth:`connect` - establish the session, send ``hello``,
       receive ``hello_ack``. Raises on hard failure.
    2. :meth:`send_frames` - push a batch of outbound frames. Returns
       input indices handed off successfully by that transport. A
       WebSocket ``event_batch`` is durably confirmed only by a later
       ``event_batch_ack``; a long-poll HTTP response is its acknowledgement.
    3. :meth:`receive_frames` - read any inbound frames (heartbeat
       tells and command pushes). Blocks until at least one frame
       arrives or the connection is lost.
    4. :meth:`close` - shut down cleanly.

    The runtime drives a simple loop: connect → send pending → read
    available → handle command → back to send. A disconnect triggers
    a reconnect with exponential-backoff jitter.
    """

    async def connect(self) -> None:
        """Open the session to the brain.

        Raises:
            :class:`z4j_core.errors.ProtocolError`: The brain
                refused the ``hello`` because of a version mismatch.
            :class:`z4j_core.errors.AuthenticationError`: The
                bearer token was rejected.
            :class:`ConnectionError`: Transport-level failure.
        """
        ...

    async def send_frames(self, frames: list[bytes]) -> list[int]:
        """Push serialized frames and return transport-accepted indices.

        The returned list contains the indices (into the input list)
        that the implementation handed off successfully. Whether that
        also confirms durable brain acceptance is transport-specific;
        the runtime applies each implementation's acknowledgement policy.
        """
        ...

    async def receive_frames(
        self,
        on_frame: Callable[[Frame], Awaitable[None]],
    ) -> None:
        """Read inbound frames, invoking ``on_frame`` for each.

        Returns when the peer closes the connection cleanly or when
        :meth:`close` is invoked. Raises on unexpected disconnect.
        """
        ...

    async def close(self) -> None:
        """Close the transport. Idempotent."""
        ...


__all__ = ["Transport", "WebSocketTransport"]
