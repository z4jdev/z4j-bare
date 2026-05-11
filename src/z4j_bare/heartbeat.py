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
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from z4j_core.transport.frames import (
    AgentStatusFrame,
    AgentStatusPayload,
    HeartbeatFrame,
    HeartbeatPayload,
    serialize_frame,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from z4j_bare.buffer import BufferStore

logger = logging.getLogger("z4j.runtime.heartbeat")

#: Hard wall-clock cap on synchronous health /
#: status provider calls. Adapter providers (notably the celery
#: ``inspector.stats()`` path) call into broker code that does
#: blocking I/O - if the broker is wedged the call can block
#: indefinitely. With this cap the worst case is one heartbeat tick
#: ships a synthetic ``{"error": "provider timed out"}`` health blob
#: while the loop stays responsive.
_PROVIDER_TIMEOUT_SECONDS: float = 5.0


class Heartbeat:
    """Periodically enqueue heartbeat + agent_status frames.

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
        status_provider: "Callable[[], dict[str, Any]] | None" = None,
    ) -> None:
        self.buffer = buffer
        self.stop_event = stop_event
        self.interval = interval
        self._last_flush_at: datetime | None = None
        self._dropped_events = 0
        self._health_provider = health_provider
        self._status_provider = status_provider

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
                # Health/status provider callbacks are SYNCHRONOUS
                # and may call into adapter code that does blocking
                # I/O (e.g. the celery adapter's ``inspector.stats()``
                # does a blocking Redis BRPOP via kombu's pidbox
                # broadcast). Calling those providers directly in this
                # coroutine would wedge the entire asyncio event loop
                # until BRPOP returned - which means heartbeats stop,
                # the WS recv loop freezes, the websockets-library
                # PING/PONG starves, and the brain ages the agent to
                # ``offline`` while the loop is completely silent.
                #
                # The enqueue helpers are async and run the provider
                # via ``asyncio.to_thread`` with a hard ``wait_for``
                # cap. A wedged provider can no longer block the loop;
                # on cap-timeout the heartbeat ships with a synthetic
                # ``{"error": ...}`` health blob.
                await self._enqueue_heartbeat()
                await self._enqueue_agent_status()
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

    async def _enqueue_heartbeat(self) -> None:
        # Skip if shutdown is already underway - appending after the
        # buffer has been closed would raise RuntimeError and break
        # the loop's clean exit.
        if self.stop_event.is_set():
            return
        adapter_health: dict[str, str] = {}
        if self._health_provider is not None:
            adapter_health = await self._safe_provider_call(
                self._health_provider,
                provider_name="health",
            )
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

    async def _enqueue_agent_status(self) -> None:
        """Emit an :class:`AgentStatusFrame` alongside the heartbeat.

        Phase H: brain-side persisted to ``agent_status_history`` so
        the dashboard can render a per-agent flap timeline. Opt out
        via ``Z4J_AGENT_STATUS_DISABLED=1`` in environments that don't
        want the frame on the wire (typically resource-constrained
        agents that ship hundreds per host).
        """
        if os.environ.get("Z4J_AGENT_STATUS_DISABLED", "").lower() in (
            "1", "true", "yes", "on",
        ):
            return
        if self.stop_event.is_set():
            return
        if self._status_provider is None:
            # No supervisor wired the provider in. Don't emit a
            # half-empty frame; the brain treats absence as "agent
            # doesn't speak this protocol version yet" rather than
            # "agent in a permanent zero-failure state."
            return
        status = await self._safe_provider_call(
            self._status_provider,
            provider_name="status",
        )
        if status.get("error") == "provider timed out":
            # Don't ship a placeholder ``agent_status`` frame; the brain
            # would persist it as a real reading. Skip and let the next
            # tick try again.
            return
        if not status:
            return
        try:
            payload = AgentStatusPayload(**status)
        except Exception:  # noqa: BLE001
            logger.exception("z4j agent: status provider returned invalid shape")
            return
        frame = AgentStatusFrame(
            id=self._new_status_id(),
            ts=datetime.now(UTC),
            payload=payload,
        )
        try:
            self.buffer.append("agent_status", serialize_frame(frame))
        except RuntimeError:
            logger.debug("z4j agent: agent_status dropped: buffer closed")

    async def _safe_provider_call(
        self,
        provider: "Callable[[], dict[str, Any]]",
        *,
        provider_name: str,
    ) -> dict[str, Any]:
        """Run a synchronous provider off the event loop with a timeout.

        Three failure modes the caller is protected from:

        1. Provider raises - returns ``{"error": "provider raised"}``.
        2. Provider blocks > ``_PROVIDER_TIMEOUT_SECONDS`` (the
           ``inspector.stats()`` BRPOP wedge case) - returns
           ``{"error": "provider timed out"}``. The loop continues;
           the next tick tries again.
        3. ``asyncio.to_thread`` itself raises (extremely rare) -
           ``{"error": "to_thread failed"}``.

        Provider runs in a worker thread so even an unbounded blocking
        call doesn't pin the event loop. The thread is leaked on
        timeout (Python has no safe way to cancel a sync call); the
        leak is bounded in practice because the provider that
        triggered it eventually returns or the agent restarts.
        """
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(provider),
                timeout=_PROVIDER_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "z4j agent: %s provider timed out after %.1fs; "
                "shipping synthetic error blob",
                provider_name,
                _PROVIDER_TIMEOUT_SECONDS,
            )
            return {"error": "provider timed out"}
        except Exception:  # noqa: BLE001
            logger.exception(
                "z4j agent: %s provider raised; shipping synthetic error blob",
                provider_name,
            )
            return {"error": "provider raised"}

    @staticmethod
    def _new_id() -> str:
        import secrets as _secrets
        return f"hb_{_secrets.token_hex(6)}"

    @staticmethod
    def _new_status_id() -> str:
        import secrets as _secrets
        return f"as_{_secrets.token_hex(6)}"


__all__ = ["Heartbeat"]
