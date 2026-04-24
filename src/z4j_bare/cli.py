"""Command-line entry point: ``python -m z4j_bare``.

Used by projects that cannot modify their engine's worker startup
code - they run the agent as a separate process that connects to the
same broker.

Note: running the agent as a separate process means some context
enrichment (current request id, current user) is not available. For
everything else it works identically to the in-process installation
path.

Usage::

    python -m z4j_bare run \\
        --brain-url https://z4j.internal \\
        --token $Z4J_TOKEN \\
        --project-id ml-pipeline-prod \\
        --engine celery \\
        --app myproject.celery:app

The ``--engine`` argument selects which engine adapter to load. Any
engine name registered in :data:`_ENGINE_LOADERS` is accepted; each
loader does its own lazy import so installing only ``z4j-rq`` (no
``z4j-celery``) and passing ``--engine rq --app myproject:queue`` is
a supported path.

The ``--app`` argument is an importable dotted path to the engine's
"app" object (whatever shape that engine uses - e.g. a Celery ``app``,
an RQ ``Queue`` or ``Redis`` connection, a Dramatiq broker). The
deprecated ``--celery-app`` flag is kept as an alias for ``--app``
when ``--engine=celery``.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import signal
import sys
from typing import Any, Callable

from z4j_bare.install import install_agent
from z4j_bare.runtime import AgentRuntime

logger = logging.getLogger("z4j.agent.cli")


# ---------------------------------------------------------------------------
# Engine loader registry
# ---------------------------------------------------------------------------
#
# Each entry maps an engine name (the value users pass to ``--engine``)
# to a loader callable. The loader receives the resolved ``app`` path
# string (or ``None``) and returns an instantiated adapter that
# satisfies :class:`z4j_core.protocols.QueueEngineAdapter`.
#
# Loaders MUST do their own lazy import of the engine package so the
# CLI can list available engines (or accept ``--engine X``) without
# requiring every engine package to be installed. A loader that fails
# to import its package raises ``ImportError`` with a helpful pip
# install hint; the CLI converts that into a non-zero exit code.
#
# Adding a new engine = add one row here. No other CLI changes needed.
# A future v1.1 polish (tracked in docs/BARE_AUDIT_2026Q2.md) replaces
# this hand-rolled registry with ``importlib.metadata`` entry-point
# discovery once we have 5+ engines.

EngineLoader = Callable[[str | None], Any]


def _load_celery(app_path: str | None) -> Any:
    if not app_path:
        raise ImportError(
            "--app is required when --engine=celery "
            "(e.g. --app myproject.celery:app)",
        )
    celery_app = _import_object(app_path)
    try:
        from z4j_celery.engine import CeleryEngineAdapter  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "z4j-celery is not installed. pip install z4j-celery",
        ) from exc
    return CeleryEngineAdapter(celery_app=celery_app)


def _load_rq(app_path: str | None) -> Any:
    if not app_path:
        raise ImportError(
            "--app is required when --engine=rq "
            "(e.g. --app myproject:redis_conn for a redis.Redis instance, "
            "or --app myproject:queue for an rq.Queue instance)",
        )
    rq_app = _import_object(app_path)
    try:
        from z4j_rq.engine import RqEngineAdapter  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "z4j-rq is not installed. pip install z4j-rq",
        ) from exc
    return RqEngineAdapter(rq_app=rq_app)


def _load_dramatiq(app_path: str | None) -> Any:
    if not app_path:
        raise ImportError(
            "--app is required when --engine=dramatiq "
            "(e.g. --app myproject:broker for a dramatiq.Broker instance)",
        )
    broker = _import_object(app_path)
    try:
        from z4j_dramatiq.engine import (  # type: ignore[import-not-found]
            DramatiqEngineAdapter,
        )
    except ImportError as exc:
        raise ImportError(
            "z4j-dramatiq is not installed. pip install z4j-dramatiq",
        ) from exc
    return DramatiqEngineAdapter(broker=broker)


_ENGINE_LOADERS: dict[str, EngineLoader] = {
    "celery": _load_celery,
    "rq": _load_rq,
    "dramatiq": _load_dramatiq,
}


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a shell exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.subcommand is None:
        parser.print_help()
        return 2

    if args.subcommand == "run":
        return _cmd_run(args)
    if args.subcommand == "version":
        return _cmd_version()
    if args.subcommand == "doctor":
        return _cmd_doctor(args)

    parser.print_help()
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m z4j_bare",
        description="z4j agent runtime for bare Python projects",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level for the z4j namespace",
    )

    sub = parser.add_subparsers(dest="subcommand")

    run_parser = sub.add_parser("run", help="Run the agent in the foreground")
    run_parser.add_argument("--brain-url", help="Brain URL (or set Z4J_BRAIN_URL)")
    run_parser.add_argument("--token", help="Agent bearer token (or set Z4J_TOKEN)")
    run_parser.add_argument("--project-id", help="Project slug (or set Z4J_PROJECT_ID)")
    run_parser.add_argument(
        "--engine",
        default="celery",
        help=(
            "Which engine adapter to load. Known engines: "
            + ", ".join(sorted(_ENGINE_LOADERS))
            + ". Pass an unknown name to see this list again."
        ),
    )
    run_parser.add_argument(
        "--app",
        help=(
            "Importable dotted path to the engine's app object "
            "(e.g. myproject.celery:app for Celery, myproject:queue "
            "for RQ, myproject:broker for Dramatiq)."
        ),
    )
    # Deprecated alias kept so v1 users do not break. Emits a warning
    # on use; promote everyone to ``--app`` over the v2026.5 release.
    run_parser.add_argument(
        "--celery-app",
        dest="celery_app_legacy",
        help=argparse.SUPPRESS,
    )

    sub.add_parser("version", help="Print the agent version and exit")

    doc_parser = sub.add_parser(
        "doctor",
        help="Check connectivity to the brain and report what's wrong",
    )
    doc_parser.add_argument(
        "--brain-url",
        help="Brain URL (or set Z4J_BRAIN_URL)",
    )
    doc_parser.add_argument(
        "--token",
        help="Agent bearer token (or set Z4J_TOKEN)",
    )
    doc_parser.add_argument(
        "--project-id",
        help="Project slug (or set Z4J_PROJECT_ID)",
    )
    doc_parser.add_argument(
        "--hmac-secret",
        help="Shared HMAC secret (or set Z4J_HMAC_SECRET)",
    )
    doc_parser.add_argument(
        "--no-websocket",
        action="store_true",
        help="Skip the WebSocket upgrade probe",
    )
    doc_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of text",
    )
    return parser


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _cmd_version() -> int:
    from z4j_bare import __version__ as bare_version
    from z4j_core.version import PROTOCOL_VERSION, __version__ as core_version
    print(f"z4j-bare {bare_version}")
    print(f"z4j-core {core_version}")
    print(f"protocol {PROTOCOL_VERSION}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    _configure_logging(args.log_level)

    app_path = args.app
    if app_path is None and args.celery_app_legacy is not None:
        logger.warning(
            "z4j: --celery-app is deprecated; use --app instead",
        )
        app_path = args.celery_app_legacy

    try:
        engine = _load_engine(args.engine, app_path)
    except ImportError as exc:
        print(f"z4j: failed to load engine adapter: {exc}", file=sys.stderr)
        return 2

    try:
        runtime = install_agent(
            engines=[engine],
            brain_url=args.brain_url,
            token=args.token,
            project_id=args.project_id,
            autostart=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"z4j: failed to install agent: {exc}", file=sys.stderr)
        return 1

    logger.info("z4j agent running (ctrl-c to stop)")
    _wait_for_shutdown(runtime)
    return 0


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("z4j").setLevel(level)


def _load_engine(engine_name: str, app_path: str | None) -> Any:
    """Look up + invoke the registered loader for ``engine_name``.

    Unknown engine names produce a helpful error listing the engines
    this build of the CLI knows about - the user typically meant a
    typo or forgot to install the engine package.
    """
    loader = _ENGINE_LOADERS.get(engine_name)
    if loader is None:
        known = ", ".join(sorted(_ENGINE_LOADERS)) or "(none registered)"
        raise ImportError(
            f"unknown engine {engine_name!r}. Known engines: {known}. "
            f"Each engine also requires its package to be installed "
            f"(pip install z4j-{engine_name}).",
        )
    return loader(app_path)


def _import_object(dotted: str) -> Any:
    """Resolve a ``module:attr`` or ``module.attr`` reference.

    Example: ``myproject.celery:app`` imports ``myproject.celery``
    and returns ``module.app``. Engine-agnostic - used for every
    ``--app`` value regardless of which engine loader receives it.
    """
    if ":" in dotted:
        module_path, attr = dotted.split(":", 1)
    else:
        module_path, _, attr = dotted.rpartition(".")
        if not module_path:
            raise ImportError(f"invalid import path {dotted!r}")
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise ImportError(f"{module_path} has no attribute {attr!r}") from exc


def _wait_for_shutdown(runtime: AgentRuntime) -> None:
    """Block until SIGINT/SIGTERM, then stop the runtime cleanly."""
    stop_requested = False

    def _handle(signum: int, frame: object) -> None:  # noqa: ARG001
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _handle)
    try:
        signal.signal(signal.SIGTERM, _handle)
    except (AttributeError, ValueError):  # pragma: no cover  (Windows)
        pass

    # Simple wait loop - pause() would block forever even after signal
    # handler runs, so we use a small sleep and check the flag.
    import time as _time

    while not stop_requested:
        _time.sleep(0.5)

    logger.info("z4j agent stopping...")
    runtime.stop(timeout=10.0)


def _cmd_doctor(args: argparse.Namespace) -> int:
    """Run connectivity probes and report what's wrong.

    Builds a minimal :class:`Config` from CLI args + ``Z4J_*`` env
    vars (no engine adapter, no framework adapter) and walks the
    probe ladder from :mod:`z4j_bare.diagnostics`. Output mirrors
    the framework adapters' doctor commands so operators see
    consistent results regardless of which entry point they use.
    """
    import json
    import os as _os

    from z4j_core.models import Config
    from z4j_core.errors import ConfigError

    from z4j_bare import diagnostics

    brain_url = args.brain_url or _os.environ.get("Z4J_BRAIN_URL")
    token = args.token or _os.environ.get("Z4J_TOKEN")
    project_id = args.project_id or _os.environ.get("Z4J_PROJECT_ID")
    hmac_secret = args.hmac_secret or _os.environ.get("Z4J_HMAC_SECRET")

    missing = [
        name for name, val in (
            ("brain_url", brain_url),
            ("token", token),
            ("project_id", project_id),
        ) if not val
    ]
    if missing:
        msg = (
            f"z4j-doctor: missing required config: {', '.join(missing)}. "
            f"Pass --{missing[0].replace('_', '-')} or set Z4J_{missing[0].upper()}."
        )
        if args.json:
            print(json.dumps({"ok": False, "stage": "config", "error": msg}, indent=2))
        else:
            print(msg, file=sys.stderr)
        return 1

    try:
        config_kwargs: dict[str, Any] = {
            "brain_url": brain_url,
            "token": token,
            "project_id": project_id,
        }
        if hmac_secret:
            config_kwargs["hmac_secret"] = hmac_secret
        config = Config(**config_kwargs)
    except (ConfigError, ValueError) as exc:
        if args.json:
            print(json.dumps({"ok": False, "stage": "config", "error": str(exc)}, indent=2))
        else:
            print(f"z4j-doctor: invalid config: {exc}", file=sys.stderr)
        return 1

    results = []
    results.append(diagnostics.probe_buffer_path(config.buffer_path))
    if results[-1].ok:
        for probe in (
            diagnostics.probe_dns,
            diagnostics.probe_tcp,
            diagnostics.probe_tls,
        ):
            r = probe(str(config.brain_url))
            results.append(r)
            if not r.ok:
                break
        else:
            if not args.no_websocket:
                results.append(diagnostics.probe_websocket(config))

    if args.json:
        payload = {
            "ok": all(r.ok for r in results),
            "config": {
                "brain_url": str(config.brain_url),
                "project_id": config.project_id,
                "buffer_path": str(config.buffer_path),
                "transport": config.transport,
            },
            "probes": [
                {"name": r.name, "ok": r.ok, "message": r.message, "details": r.details}
                for r in results
            ],
        }
        print(json.dumps(payload, indent=2))
    else:
        print("z4j-doctor (bare)")
        print("=================")
        print(f"  brain_url:   {config.brain_url}")
        print(f"  project_id:  {config.project_id}")
        print(f"  buffer_path: {config.buffer_path}")
        print(f"  transport:   {config.transport}")
        print()
        for r in results:
            tag = "[OK]  " if r.ok else "[FAIL]"
            print(f"  {tag} {r.name:12s} {r.message}")
    return 0 if all(r.ok for r in results) else 1


__all__ = ["main"]
