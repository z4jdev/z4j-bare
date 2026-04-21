"""The ``bare`` framework adapter.

For Python projects that have no web framework at all - e.g. a
standalone worker script (Celery, RQ, Dramatiq, or any other engine)
or an ML pipeline repository that just runs tasks. The bare adapter:

- Builds its :class:`Config` from environment variables and explicit
  kwargs to :func:`z4j_bare.install_agent`.
- Returns empty :class:`DiscoveryHints` - engine adapters fall back
  to their own discovery strategy.
- Has no request context to supply.
- Has no lifecycle hooks beyond what the caller passes directly.

Framework adapters for Django, Flask, and FastAPI live in their own
packages and implement the same :class:`FrameworkAdapter` Protocol
on top of their respective frameworks.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from z4j_core.models import Config, DiscoveryHints, RequestContext, User


class BareFrameworkAdapter:
    """Framework-free adapter.

    Does not integrate with any web framework. Configuration comes
    either from the explicit ``config`` passed to the constructor or
    from environment variables read by :func:`z4j_bare.install.build_config`.

    Attributes:
        name: Always ``"bare"``.
        _config: The resolved :class:`Config`.
    """

    name: str = "bare"

    def __init__(self, config: Config) -> None:
        self._config = config
        self._startup_hooks: list[Callable[[], None]] = []
        self._shutdown_hooks: list[Callable[[], None]] = []

    # --- FrameworkAdapter Protocol ---------------------------------

    def discover_config(self) -> Config:
        return self._config

    def discovery_hints(self) -> DiscoveryHints:
        return DiscoveryHints(framework_name="bare")

    def current_context(self) -> RequestContext | None:
        return None

    def current_user(self) -> User | None:
        return None

    def on_startup(self, hook: Callable[[], None]) -> None:
        self._startup_hooks.append(hook)

    def on_shutdown(self, hook: Callable[[], None]) -> None:
        self._shutdown_hooks.append(hook)

    def register_admin_view(self, view: Any) -> None:  # noqa: ARG002
        # No admin surface for bare Python. Intentional no-op.
        return None

    # --- Bare-specific helpers --------------------------------------

    def fire_startup(self) -> None:
        """Invoke every registered startup hook.

        Called by :class:`z4j_bare.runtime.AgentRuntime` after the
        transport connects. Exceptions from individual hooks are
        allowed to propagate to the caller - the runtime wraps this
        call in a safety boundary.
        """
        for hook in self._startup_hooks:
            hook()

    def fire_shutdown(self) -> None:
        """Invoke every registered shutdown hook.

        Called during :meth:`AgentRuntime.stop` after the transport
        is closed. Exceptions are allowed to propagate; the caller
        handles them.
        """
        for hook in self._shutdown_hooks:
            hook()


__all__ = ["BareFrameworkAdapter"]
