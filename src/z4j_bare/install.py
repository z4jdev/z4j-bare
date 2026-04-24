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
from z4j_core.protocols import FrameworkAdapter, QueueEngineAdapter, SchedulerAdapter

from z4j_bare._process_singleton import try_register
from z4j_bare.framework import BareFrameworkAdapter
from z4j_bare.runtime import AgentRuntime

if TYPE_CHECKING:
    pass


#: Directory under which every buffer-path variant must live. Fixed
#: per-user root so an attacker who flips ``Z4J_BUFFER_PATH`` can't
#: make the agent write SQLite into ``/etc/...`` or a Windows
#: system directory. Operators who need a custom location should
#: change ``HOME`` itself (container convention).
_BUFFER_ROOT: Path = (Path.home() / ".z4j").resolve()


def _clamp_buffer_path(candidate: Path) -> Path:
    """Resolve + validate a buffer-path candidate against :data:`_BUFFER_ROOT`.

    Rejects (via :class:`ConfigError`):

    - Symlinks whose target escapes the root.
    - Parent-traversal (``..``) that escapes the root once resolved.
    - Any path that, after ``resolve(strict=False)``, is not a
      descendant of :data:`_BUFFER_ROOT`.

    Relative paths are interpreted under the root. Absolute paths
    outside the root are rejected outright. The file itself does
    not have to exist yet - :class:`BufferStore` creates it.
    """
    _BUFFER_ROOT.mkdir(parents=True, exist_ok=True)
    candidate = Path(candidate)
    if not candidate.is_absolute():
        candidate = _BUFFER_ROOT / candidate
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(_BUFFER_ROOT)
    except ValueError as exc:
        raise ConfigError(
            f"Z4J_BUFFER_PATH must be inside {_BUFFER_ROOT} "
            f"(got {candidate}, resolved to {resolved}). Refusing "
            f"to open a SQLite file outside the allowed root.",
        ) from exc
    return resolved


def install_agent(
    *,
    engines: list[QueueEngineAdapter],
    brain_url: str | None = None,
    token: str | None = None,
    project_id: str | None = None,
    hmac_secret: str | None = None,
    agent_name: str | None = None,
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
    """
    if not engines:
        raise ConfigError("install_agent: at least one engine adapter is required")

    resolved = _resolve(
        brain_url=brain_url,
        token=token,
        project_id=project_id,
        hmac_secret=hmac_secret,
        agent_name=agent_name,
        environment=environment,
        tags=tags,
        dev_mode=dev_mode,
        strict_mode=strict_mode,
        autostart=autostart,
        buffer_path=buffer_path,
        max_payload_bytes=max_payload_bytes,
        redaction_extra_key_patterns=redaction_extra_key_patterns,
        redaction_extra_value_patterns=redaction_extra_value_patterns,
    )

    try:
        config = Config(**resolved)
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"invalid z4j agent configuration: {exc}") from exc

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


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def _resolve(  # noqa: PLR0912  (a flat field-by-field resolver is clearer)
    *,
    brain_url: str | None,
    token: str | None,
    project_id: str | None,
    hmac_secret: str | None,
    environment: str | None,
    tags: dict[str, str] | None,
    dev_mode: bool | None,
    strict_mode: bool | None,
    autostart: bool | None,
    buffer_path: Path | None,
    max_payload_bytes: int | None,
    redaction_extra_key_patterns: list[str] | None,
    redaction_extra_value_patterns: list[str] | None,
    agent_name: str | None = None,
) -> dict[str, Any]:
    """Merge explicit kwargs with ``Z4J_*`` env vars."""
    env = os.environ

    # Explicit kwarg > env var. Use ``is not None`` rather than a
    # truthy ``or`` so an operator passing an empty string as a
    # required field fails the non-empty check below rather than
    # silently sliding onto an env fallback value. Surfaced by
    # audit pass 8 2026-04-21 - matched the same bug in
    # ``z4j-fastapi`` and fixed consistently here.
    resolved_brain = brain_url if brain_url is not None else env.get("Z4J_BRAIN_URL")
    resolved_token = token if token is not None else env.get("Z4J_TOKEN")
    resolved_project = project_id if project_id is not None else env.get("Z4J_PROJECT_ID")
    # Protocol v2 envelope HMAC is mandatory - runtime.start() refuses
    # to come up without a secret. Framework adapters (Django, Flask,
    # FastAPI) had their own env-read code for this field; bare-Python
    # agents (RQ, Dramatiq, future Go/Java bootstrappers) were missing
    # it entirely, which surfaced as "hmac_secret is required" when
    # booting the dramatiq-sandbox. Centralizing the read here fixes
    # every bare caller at once.
    resolved_hmac = (
        hmac_secret if hmac_secret is not None else env.get("Z4J_HMAC_SECRET")
    )

    missing: list[str] = []
    if not resolved_brain:
        missing.append("Z4J_BRAIN_URL (or brain_url kwarg)")
    if not resolved_token:
        missing.append("Z4J_TOKEN (or token kwarg)")
    if not resolved_project:
        missing.append("Z4J_PROJECT_ID (or project_id kwarg)")
    if missing:
        raise ConfigError("missing required configuration: " + ", ".join(missing))

    # At this point resolved_brain etc are non-None - assert for the type checker.
    assert resolved_brain is not None
    assert resolved_token is not None
    assert resolved_project is not None

    out: dict[str, Any] = {
        "brain_url": resolved_brain,
        "token": resolved_token,
        "project_id": resolved_project,
    }
    if resolved_hmac:
        out["hmac_secret"] = resolved_hmac

    resolved_agent_name = (
        agent_name if agent_name is not None else env.get("Z4J_AGENT_NAME")
    )
    if resolved_agent_name:
        out["agent_name"] = resolved_agent_name

    if environment is not None:
        out["environment"] = environment
    elif "Z4J_ENVIRONMENT" in env:
        out["environment"] = env["Z4J_ENVIRONMENT"]

    if tags is not None:
        out["tags"] = tags

    # Security (audit C3): ``dev_mode`` is explicitly NOT read from
    # the environment. It disables the HMAC-required invariant, so
    # letting an env var flip it turns one compromised .env / K8s
    # configmap into silent signature bypass. The kwarg is the only
    # accepted source, and the caller must be code (so the switch
    # is reviewable in git).
    #
    # If ``Z4J_DEV_MODE`` is set in the environment, we log a loud
    # warning and ignore it - a legacy sandbox might still have it;
    # we don't want to crash, but we also don't want to honor it.
    if dev_mode is not None:
        out["dev_mode"] = dev_mode
    elif "Z4J_DEV_MODE" in env:
        import warnings

        warnings.warn(
            "Z4J_DEV_MODE env var is IGNORED for security reasons. "
            "Pass ``dev_mode=True`` as a kwarg to ``install_agent`` "
            "if you really want to disable HMAC verification. See "
            "docs/SECURITY.md for why.",
            stacklevel=3,
        )

    if strict_mode is not None:
        out["strict_mode"] = strict_mode
    elif "Z4J_STRICT_MODE" in env:
        out["strict_mode"] = env["Z4J_STRICT_MODE"].lower() in ("1", "true", "yes", "on")

    if autostart is not None:
        out["autostart"] = autostart
    elif "Z4J_AUTOSTART" in env:
        out["autostart"] = env["Z4J_AUTOSTART"].lower() in ("1", "true", "yes", "on")

    # Security (audit H8): the buffer path is resolved inside a
    # fixed allowed root (``~/.z4j/`` by default) so a compromised
    # env var cannot make the agent ``sqlite3.connect`` to
    # ``/etc/cron.d/...`` or ``C:\Windows\...``. An absolute path
    # outside the root, a symlink target outside the root, or any
    # ``..`` traversal attempt is rejected.
    if buffer_path is not None:
        out["buffer_path"] = _clamp_buffer_path(buffer_path)
    elif "Z4J_BUFFER_PATH" in env:
        out["buffer_path"] = _clamp_buffer_path(Path(env["Z4J_BUFFER_PATH"]))

    if max_payload_bytes is not None:
        out["max_payload_bytes"] = max_payload_bytes
    elif "Z4J_MAX_PAYLOAD_BYTES" in env:
        out["max_payload_bytes"] = int(env["Z4J_MAX_PAYLOAD_BYTES"])

    if redaction_extra_key_patterns is not None:
        out["redaction_extra_key_patterns"] = redaction_extra_key_patterns
    if redaction_extra_value_patterns is not None:
        out["redaction_extra_value_patterns"] = redaction_extra_value_patterns

    return out


__all__ = ["install_agent"]
