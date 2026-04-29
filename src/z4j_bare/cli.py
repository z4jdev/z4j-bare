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


def make_engine_main(
    engine_id: str,
    *,
    upstream_package: str,
    broker_env: str | None = None,
    upstream_attribute: str | None = None,
) -> Callable[[list[str] | None], int]:
    """Return a ``main(argv)`` for a z4j-<engine> CLI.

    Engines are libraries, not runtimes (they don't manage their
    own connection - they're plugged into a framework adapter or
    z4j-bare). They expose a slim CLI surface for doctor / check /
    status to confirm:

    1. the upstream engine library is importable (``upstream_package``)
    2. the z4j-<engine> adapter package is importable
    3. the broker URL env var is set (when applicable)

    Operators run engine doctors when isolating a problem: "is RQ
    installed in this venv? Is REDIS_URL set? Is z4j-rq importable?"
    Each is a one-line answer; running the engine's doctor as part
    of a framework's doctor (the common path) gives the full
    picture.

    Engine adapters call this from their own ``cli.py``::

        # z4j_celery/cli.py
        from z4j_bare.cli import make_engine_main
        main = make_engine_main(
            "celery", upstream_package="celery", broker_env="CELERY_BROKER_URL",
        )
    """

    def _engine_main(argv: list[str] | None = None) -> int:
        if argv is None:
            argv = sys.argv[1:]
        parser = argparse.ArgumentParser(
            prog=f"z4j-{engine_id}",
            description=(
                f"z4j-{engine_id}: engine adapter diagnostics. "
                "Run from a host process for engine-specific health "
                "checks; run framework doctors for full coverage."
            ),
        )
        sub = parser.add_subparsers(dest="subcommand")
        sub.add_parser(
            "doctor",
            help=f"Check that {engine_id} + z4j-{engine_id} are importable",
        )
        sub.add_parser("check", help="Alias for doctor")
        sub.add_parser("status", help="One-line: package presence + broker URL")
        sub.add_parser("version", help="Print z4j-<engine> version + exit")
        args = parser.parse_args(argv)

        if args.subcommand is None:
            parser.print_help()
            return 2

        if args.subcommand == "version":
            try:
                import importlib.metadata as _md
                print(_md.version(f"z4j-{engine_id}"))  # noqa: T201
                return 0
            except Exception as exc:  # noqa: BLE001
                print(f"z4j-{engine_id} version: {exc}", file=sys.stderr)
                return 1

        # doctor / check / status all share the import probes.
        results: list[tuple[str, bool, str]] = []

        # Probe 1: upstream engine library
        try:
            mod = __import__(upstream_package)
            ver = getattr(mod, "__version__", "<unknown>")
            results.append(
                (
                    f"upstream {upstream_package}",
                    True,
                    f"v{ver}",
                ),
            )
        except ImportError as exc:
            results.append(
                (
                    f"upstream {upstream_package}",
                    False,
                    (
                        f"not installed in this venv: {exc}. "
                        f"Install with: pip install {upstream_package}"
                    ),
                ),
            )

        # Probe 2: z4j-<engine> adapter
        adapter_pkg = f"z4j_{engine_id}"
        try:
            adapter_mod = __import__(adapter_pkg)
            ver = getattr(adapter_mod, "__version__", "<unknown>")
            results.append(
                (
                    f"adapter z4j-{engine_id}",
                    True,
                    f"v{ver}",
                ),
            )
        except ImportError as exc:
            results.append(
                (
                    f"adapter z4j-{engine_id}",
                    False,
                    f"not installed: {exc}",
                ),
            )

        # Probe 3: broker URL (if applicable)
        if broker_env is not None:
            import os as _os
            url = _os.environ.get(broker_env)
            if url:
                results.append(
                    (f"broker {broker_env}", True, "set"),
                )
            else:
                results.append(
                    (
                        f"broker {broker_env}",
                        True,
                        (
                            "not set in env (this is OK if your "
                            f"{upstream_package} app loads it from "
                            "config files - the framework's doctor "
                            "checks the resolved URL via the app)"
                        ),
                    ),
                )

        if args.subcommand == "status":
            short = ", ".join(
                f"{n}={'OK' if ok else 'FAIL'}" for n, ok, _ in results
            )
            print(f"z4j-{engine_id} status: {short}")  # noqa: T201
            return 0 if all(ok for _, ok, _ in results) else 1

        # doctor / check
        print(f"z4j-{engine_id} doctor")  # noqa: T201
        print("=" * (len(f"z4j-{engine_id} doctor")))  # noqa: T201
        for name, ok, msg in results:
            tag = "[OK]  " if ok else "[FAIL]"
            print(f"  {tag} {name:24s}  {msg}")  # noqa: T201
        return 0 if all(ok for _, ok, _ in results) else 1

    return _engine_main


def make_main_for_adapter(adapter_id: str) -> Callable[[list[str] | None], int]:
    """Return a ``main(argv)`` pre-configured for a specific adapter.

    Used by framework wrapper packages (z4j-django, z4j-flask,
    z4j-fastapi) to inherit the full check/status/restart surface
    without re-declaring every subcommand. The wrapper just needs
    a one-liner::

        # z4j_flask/cli.py
        from z4j_bare.cli import make_main_for_adapter
        main = make_main_for_adapter("flask")

    What this does:
    - For ``restart`` / ``reload`` subcommands, pre-fills
      ``--adapter <adapter_id>`` so ``z4j-flask restart`` signals
      the flask agent's pidfile, not the generic bare one.
    - Sets ``argparse`` ``prog`` to the adapter's pip name so the
      help text reads correctly.
    - All other subcommands flow through unchanged.
    """

    def _adapter_main(argv: list[str] | None = None) -> int:
        if argv is None:
            argv = sys.argv[1:]
        # Pre-fill --adapter for restart/reload so "z4j-django
        # restart" routes to the django pidfile, not the generic
        # bare one. Operator can still override with explicit
        # --adapter X if they really want to signal a sibling.
        if argv and argv[0] in ("restart", "reload"):
            if "--adapter" not in argv and "-a" not in argv:
                argv = [argv[0], "--adapter", adapter_id, *argv[1:]]
        return main(argv, prog=f"z4j-{adapter_id}")

    return _adapter_main


def main(argv: list[str] | None = None, prog: str | None = None) -> int:
    """CLI entry point. Returns a shell exit code."""
    parser = _build_parser(prog=prog)
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
    if args.subcommand == "check":
        return _cmd_check(args)
    if args.subcommand == "status":
        return _cmd_status(args)
    if args.subcommand == "restart" or args.subcommand == "reload":
        return _cmd_restart(args)

    parser.print_help()
    return 2


def _build_parser(prog: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog or "python -m z4j_bare",
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

    # check / status / restart - the resilience triad. Mirror the
    # brain's z4j-brain check/status verbs so operators get a
    # consistent surface across every package.

    check_parser = sub.add_parser(
        "check",
        help=(
            "Short pass/fail health check (exit 0 OK, exit 1 fail). "
            "Same probes as ``doctor`` but compact output."
        ),
    )
    check_parser.add_argument(
        "--brain-url",
        help="Brain URL (or set Z4J_BRAIN_URL)",
    )
    check_parser.add_argument(
        "--token",
        help="Agent bearer token (or set Z4J_TOKEN)",
    )
    check_parser.add_argument(
        "--project-id",
        help="Project slug (or set Z4J_PROJECT_ID)",
    )

    sub.add_parser(
        "status",
        help=(
            "One-line current state: pidfile, buffer dir, last "
            "session id, consecutive error counts."
        ),
    )

    restart_parser = sub.add_parser(
        "restart",
        help=(
            "Force the running agent to drop its current connection "
            "and reconnect immediately, skipping the supervisor's "
            "exponential backoff. Sends SIGHUP via the pidfile."
        ),
    )
    restart_parser.add_argument(
        "--adapter",
        default="bare",
        help=(
            "Adapter id whose pidfile to signal "
            "(default: bare). Override when running multiple "
            "adapters in the same process tree."
        ),
    )
    sub.add_parser(
        "reload",
        help="Alias for ``restart`` (matches Unix service convention).",
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


def _load_doctor_config(args: argparse.Namespace) -> "Config":
    """Build a probe-ready Config from CLI args + Z4J_* env vars.

    Shared by the doctor and check commands. Raises ``ValueError``
    or ``ConfigError`` on missing / malformed config so the caller
    can format an error message appropriate to the command.
    """
    import os as _os

    from z4j_core.errors import ConfigError
    from z4j_core.models import Config

    brain_url = (
        getattr(args, "brain_url", None) or _os.environ.get("Z4J_BRAIN_URL")
    )
    token = getattr(args, "token", None) or _os.environ.get("Z4J_TOKEN")
    project_id = (
        getattr(args, "project_id", None) or _os.environ.get("Z4J_PROJECT_ID")
    )
    hmac_secret = (
        getattr(args, "hmac_secret", None)
        or _os.environ.get("Z4J_HMAC_SECRET")
    )

    missing = [
        name
        for name, val in (
            ("brain_url", brain_url),
            ("token", token),
            ("project_id", project_id),
        )
        if not val
    ]
    if missing:
        raise ValueError(
            f"missing required config: {', '.join(missing)}. "
            f"Pass --{missing[0].replace('_', '-')} or set Z4J_{missing[0].upper()}."
        )

    config_kwargs: dict[str, Any] = {
        "brain_url": brain_url,
        "token": token,
        "project_id": project_id,
    }
    if hmac_secret:
        config_kwargs["hmac_secret"] = hmac_secret
    return Config(**config_kwargs)


def _cmd_doctor(args: argparse.Namespace) -> int:
    """Run connectivity probes and report what's wrong.

    Builds a minimal :class:`Config` from CLI args + ``Z4J_*`` env
    vars (no engine adapter, no framework adapter) and walks the
    probe ladder from :mod:`z4j_bare.diagnostics`. Output mirrors
    the framework adapters' doctor commands so operators see
    consistent results regardless of which entry point they use.
    """
    import json

    from z4j_core.errors import ConfigError

    from z4j_bare import diagnostics

    try:
        config = _load_doctor_config(args)
    except (ConfigError, ValueError) as exc:
        if args.json:
            print(json.dumps({"ok": False, "stage": "config", "error": str(exc)}, indent=2))
        else:
            print(f"z4j-doctor: {exc}", file=sys.stderr)
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


def _cmd_check(args: argparse.Namespace) -> int:
    """Short pass/fail using the doctor probes, compact output.

    Exit 0 = every probe passes. Exit 1 = any probe failed. Output
    is one line per probe, suitable for shell scripts and Nagios-
    style monitors. ``-v`` flag (TODO) would re-emit doctor's full
    output on failure.
    """
    from z4j_bare import diagnostics
    from z4j_core.models import Config

    try:
        config = _load_doctor_config(args)
    except Exception as exc:  # noqa: BLE001
        print(f"z4j-bare check: config: FAIL ({exc})", file=sys.stderr)
        return 1

    results = [diagnostics.probe_buffer_path(config.buffer_path)]
    for probe in (
        diagnostics.probe_dns,
        diagnostics.probe_tcp,
        diagnostics.probe_tls,
    ):
        r = probe(str(config.brain_url))
        results.append(r)
        if not r.ok:
            break

    fails = [r for r in results if not r.ok]
    if fails:
        for r in fails:
            print(f"z4j-bare check: {r.name}: FAIL ({r.message})")
        return 1
    print(f"z4j-bare check: all green ({len(results)} probes)")
    return 0


def _cmd_status(args: argparse.Namespace) -> int:  # noqa: ARG001
    """One-line status: is an agent running on this host? Where?

    Reads the pidfile registry under ``$Z4J_RUNTIME_DIR`` (default
    ``~/.z4j/``). Lists every adapter with a live PID. Doesn't
    require a working brain - it's a host-local introspection.

    Exit 0 even if no agents are running (status is informational,
    not pass/fail; use ``check`` for that).
    """
    from z4j_bare.control import _runtime_dir, pidfile_path  # noqa: PLC0415

    rd = _runtime_dir()
    pidfiles = sorted(rd.glob("agent-*.pid"))
    if not pidfiles:
        print(f"z4j-bare status: no running agents under {rd}")
        return 0

    print(f"z4j-bare status: agents under {rd}")
    import os as _os
    for pf in pidfiles:
        adapter = pf.stem.removeprefix("agent-")
        try:
            pid = int(pf.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            print(f"  z4j-{adapter:12s}  pidfile unreadable: {pf}")
            continue
        try:
            _os.kill(pid, 0)  # signal 0 = liveness probe, no signal sent
            alive = "running"
        except ProcessLookupError:
            alive = "stale (process not running)"
        except PermissionError:
            alive = "running (different user)"
        except OSError as exc:
            alive = f"unknown ({exc})"
        print(f"  z4j-{adapter:12s}  pid={pid}  {alive}")
    return 0


def _cmd_restart(args: argparse.Namespace) -> int:
    """Send SIGHUP to the agent so it reconnects immediately.

    Useful after a brain restart or token rotation when an operator
    doesn't want to wait for the supervisor's backoff timer to
    elapse. Returns the exit code from
    :func:`z4j_bare.control.send_restart`.
    """
    from z4j_bare.control import send_restart

    adapter = getattr(args, "adapter", "bare")
    rc, msg = send_restart(adapter)
    if rc == 0:
        print(msg)
    else:
        print(f"z4j-bare restart: {msg}", file=sys.stderr)
    return rc


__all__ = ["main"]
