"""Heartbeat loop.

Every ``heartbeat_interval`` seconds (value negotiated in the
``hello_ack`` frame - default 10s) the agent enqueues a
:class:`HeartbeatFrame` with its current buffer stats and adapter
health. The brain uses this to:

- Mark the agent online/offline
- Surface "agent is flushing slowly" warnings in the dashboard
- Alert on dropped events

The heartbeat task does no network I/O of its own - it just appends
a frame to the outbound buffer and lets the transport loop drain it.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from z4j_core.transport.frames import (
    HeartbeatFrame,
    HeartbeatPayload,
    serialize_frame,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from z4j_bare.buffer import BufferStore

logger = logging.getLogger("z4j.agent.heartbeat")


class Heartbeat:
    """Periodically enqueue heartbeat frames.

    Run as an ``asyncio.Task`` alongside the transport loop. The
    task exits when ``stop_event`` is set.

    Attributes:
        buffer: Outbound buffer.
        interval: Seconds between heartbeats. Initially 10; updated
                  to whatever the brain told us in ``hello_ack``.
        stop_event: Cooperative cancellation signal.
    """

    def __init__(
        self,
        *,
        buffer: BufferStore,
        stop_event: asyncio.Event,
        interval: float = 10.0,
        health_provider: "Callable[[], dict[str, str]] | None" = None,
    ) -> None:
        self.buffer = buffer
        self.stop_event = stop_event
        self.interval = interval
        self._last_flush_at: datetime | None = None
        self._dropped_events = 0
        self._health_provider = health_provider

    def set_interval(self, interval: float) -> None:
        """Update the heartbeat interval.

        Called once after the ``hello_ack`` frame is received so the
        brain can dictate the cadence.
        """
        self.interval = interval

    def record_flush(self, when: datetime) -> None:
        """Record the last time the transport successfully drained the buffer."""
        self._last_flush_at = when

    def record_dropped(self, count: int) -> None:
        """Increment the lifetime dropped-event counter.

        Called when the buffer's eviction logic drops an event. Used
        by metrics and surfaced to the brain via heartbeat.
        """
        self._dropped_events += count

    async def run(self) -> None:
        """Run the heartbeat loop until ``stop_event`` is set.

        Uses ``asyncio.wait_for`` so ``stop_event.wait`` cancels the
        sleep immediately on shutdown.
        """
        logger.debug("z4j agent heartbeat loop starting (interval=%ss)", self.interval)
        try:
            while not self.stop_event.is_set():
                self._enqueue_heartbeat()
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=self.interval,
                    )
                    return  # stop_event set
                except TimeoutError:
                    continue  # timer fired, loop again
        except asyncio.CancelledError:
            logger.debug("z4j agent heartbeat cancelled")
            raise
        finally:
            logger.debug("z4j agent heartbeat loop exited")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _enqueue_heartbeat(self) -> None:
        # Skip if shutdown is already underway - appending after the
        # buffer has been closed would raise RuntimeError and break
        # the loop's clean exit.
        if self.stop_event.is_set():
            return
        adapter_health: dict[str, str] = {}
        if self._health_provider is not None:
            try:
                adapter_health = self._health_provider()
            except Exception:  # noqa: BLE001
                adapter_health = {"error": "health provider failed"}
        frame = HeartbeatFrame(
            id=self._new_id(),
            ts=datetime.now(UTC),
            payload=HeartbeatPayload(
                buffer_size=self.buffer.size(),
                last_flush_at=self._last_flush_at,
                dropped_events=self._dropped_events,
                adapter_health=adapter_health,
            ),
        )
        try:
            self.buffer.append("heartbeat", serialize_frame(frame))
        except RuntimeError:
            # Lost the race with close() - buffer was shut down
            # between our is_set() check and the append. Swallow the
            # error: we are exiting anyway.
            logger.debug("z4j agent heartbeat dropped: buffer closed")

    @staticmethod
    def _new_id() -> str:
        import secrets as _secrets
        return f"hb_{_secrets.token_hex(6)}"


__all__ = ["Heartbeat"]
