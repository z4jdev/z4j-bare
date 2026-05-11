"""Public ``install_agent()`` entry point for bare Python projects.

This is the function bare-Python projects call from their startup
module (e.g. a Celery app's ``celery.py``, an RQ worker bootstrap,
a Dramatiq broker init module, or any other engine entry point) to
wire up z4j. Framework adapters (Django, Flask, FastAPI) use their
own installation paths that eventually call through to the same
:class:`AgentRuntime`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from z4j_core.errors import ConfigError
from z4j_core.models import Config
from z4j_core.paths import reject_deprecated_path_env
from z4j_core.protocols import FrameworkAdapter, QueueEngineAdapter, SchedulerAdapter

from z4j_bare._process_singleton import try_register
from z4j_bare.framework import BareFrameworkAdapter
from z4j_bare.runtime import AgentRuntime

if TYPE_CHECKING:
    pass


def install_agent(
    *,
    engines: list[QueueEngineAdapter],
    brain_url: str | None = None,
    token: str | None = None,
    project_id: str | None = None,
    hmac_secret: str | None = None,
    agent_name: str | None = None,
    agent_id: str | None = None,
    transport: str | None = None,
    framework: type[FrameworkAdapter] | FrameworkAdapter | None = None,
    schedulers: list[SchedulerAdapter] | None = None,
    environment: str | None = None,
    tags: dict[str, str] | None = None,
    dev_mode: bool | None = None,
    strict_mode: bool | None = None,
    autostart: bool | None = None,
    buffer_path: Path | None = None,
    max_payload_bytes: int | None = None,
    redaction_extra_key_patterns: list[str] | None = None,
    redaction_extra_value_patterns: list[str] | None = None,
) -> AgentRuntime:
    """Install and start the z4j agent inside a bare Python process.

    Every keyword argument has the same meaning as the corresponding
    field on :class:`z4j_core.models.Config`. Arguments are resolved
    in this priority order:

    1. Explicit ``install_agent(...)`` keyword arguments (highest)
    2. ``Z4J_*`` environment variables
    3. Defaults from the :class:`Config` model

    The three arguments marked "required" must be supplied by exactly
    one of these sources - otherwise :class:`ConfigError` is raised.

    Required:
        engines: At least one :class:`QueueEngineAdapter` instance
                 from any installed engine package - e.g.
                 :class:`z4j_celery.CeleryEngineAdapter`,
                 :class:`z4j_rq.RqEngineAdapter`,
                 :class:`z4j_dramatiq.DramatiqEngineAdapter`. The
                 runtime supports multiple engines installed
                 simultaneously.
        brain_url (or ``Z4J_BRAIN_URL``): Base URL of the brain.
        token (or ``Z4J_TOKEN``): Agent bearer token.
        project_id (or ``Z4J_PROJECT_ID``): Project slug.
        hmac_secret (or ``Z4J_HMAC_SECRET``): Per-project HMAC secret
                    the brain minted when the agent was created.
                    Required - the runtime refuses to start without
                    it (protocol v2 envelope HMAC is mandatory).

    Returns:
        A started :class:`AgentRuntime`. The runtime runs in a
        background thread; the caller should hold a reference to it
        and call :meth:`AgentRuntime.stop` during process shutdown.

    Raises:
        ConfigError: Required configuration is missing or invalid.
        RuntimeError: Any of the dropped 1.4 path-override env vars
            (``Z4J_RUNTIME_DIR``, ``Z4J_BUFFER_DIR``, ``Z4J_BUFFER_PATH``)
            is set. Set ``Z4J_HOME`` instead.
    """
    # Hard-fail early on dropped path-override env vars so operators
    # who upgraded from 1.4 see the migration message instead of a
    # silently-relocated state directory.
    reject_deprecated_path_env()

    if not engines:
        raise ConfigError("install_agent: at least one engine adapter is required")

    from z4j_core.config import resolve_agent_config

    config = resolve_agent_config(
        framework_name="bare",
        explicit_kwargs={
            "brain_url": brain_url,
            "token": token,
            "project_id": project_id,
            "hmac_secret": hmac_secret,
            "agent_name": agent_name,
            "agent_id": agent_id,
            "transport": transport,
            "environment": environment,
            "tags": tags,
            "dev_mode": dev_mode,
            "strict_mode": strict_mode,
            "autostart": autostart,
            "buffer_path": buffer_path,
            "max_payload_bytes": max_payload_bytes,
            "redaction_extra_key_patterns": redaction_extra_key_patterns,
            "redaction_extra_value_patterns": redaction_extra_value_patterns,
        },
    )

    # Resolve the framework adapter:
    #   - explicit instance: use as-is
    #   - explicit class: instantiate with the resolved Config
    #   - None: fall back to BareFrameworkAdapter (the historical default)
    # The class-not-instance shape is the typical caller pattern: e.g.
    # z4j-celery's worker_bootstrap probes for an installed framework
    # adapter (Django/Flask/FastAPI/bare) and passes the class so this
    # function can construct it with the same Config the runtime uses.
    # Without this kwarg, install_agent always reported framework="bare"
    # in the hello frame even when running inside a Django+Celery worker
    # process - the dashboard's Framework column would show "bare"
    # instead of the operator's actual stack.
    if framework is None:
        framework_instance: FrameworkAdapter = BareFrameworkAdapter(config)
    elif isinstance(framework, type):
        framework_instance = framework(config)
    else:
        framework_instance = framework

    runtime = AgentRuntime(
        config=config,
        framework=framework_instance,
        engines=engines,
        schedulers=schedulers,
    )
    # Cooperate with the process-wide singleton so a Django+Celery
    # worker (where both ``Z4JDjangoConfig.ready()`` and
    # ``celery.signals.worker_init`` install the agent) ends up
    # with exactly one running runtime, not two fighting over the
    # same agent token. Whoever registered first wins; we drop the
    # newly-built ``runtime`` and return the active one.
    active = try_register(runtime, owner="install_agent")
    if active is not runtime:
        return active
    if config.autostart:
        runtime.start()
    return runtime


__all__ = ["install_agent"]
