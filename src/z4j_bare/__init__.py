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

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from z4j_bare.buffer import BufferEntry, BufferStore
from z4j_bare.framework import BareFrameworkAdapter
from z4j_bare.install import install_agent
from z4j_bare.runtime import AgentRuntime, RuntimeState
from z4j_bare.safety import safe_boundary, safe_call

# Report the installed wheel version (drift-proof - tracks the
# pyproject version automatically). Falls back to the z4j-core
# protocol version for source checkouts with no installed metadata.
try:
    __version__ = _pkg_version("z4j-bare")
except PackageNotFoundError:
    from z4j_core.version import __version__  # type: ignore[no-redef]

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
