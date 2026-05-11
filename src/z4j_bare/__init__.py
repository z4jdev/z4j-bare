"""z4j-bare - framework-free agent runtime.

Public API:

- :func:`install_agent` - the function bare Python projects call to
  wire up the agent. Returns a started :class:`AgentRuntime`.
- :class:`AgentRuntime` - the runtime object. Hold a reference and
  call :meth:`AgentRuntime.stop` during process shutdown.
- :class:`RuntimeState` - the runtime state enum.
- :class:`BareFrameworkAdapter` - default framework adapter for
  projects with no web framework.
- :class:`BufferStore` - local SQLite buffer (exposed for tests and
  for advanced users who want to inspect the queue).

Licensed under Apache License 2.0. See the repository
``LICENSE-APACHE``.
"""

from __future__ import annotations

from z4j_bare.buffer import BufferEntry, BufferStore
from z4j_bare.framework import BareFrameworkAdapter
from z4j_bare.install import install_agent
from z4j_bare.runtime import AgentRuntime, RuntimeState
from z4j_bare.safety import safe_boundary, safe_call

__version__ = "1.5.0"

__all__ = [
    "AgentRuntime",
    "BareFrameworkAdapter",
    "BufferEntry",
    "BufferStore",
    "RuntimeState",
    "__version__",
    "install_agent",
    "safe_boundary",
    "safe_call",
]
