"""Exactly one place in z4j may swallow a ``BaseException``.

``safe_boundary`` is the firewall between z4j and the host application: a bug in
our signal handler or middleware must not crash the user's worker. That is worth
having, and it is worth having exactly once. Every other handler has to re-raise
or forward, because a second silent swallow is how a real failure disappears.

The docstring on ``safe_boundary`` asserted this and nothing checked it, which is
the shape of claim this project keeps getting wrong. It also asserted, wrongly,
that ``KeyboardInterrupt`` and ``SystemExit`` were swallowed too; they are
re-raised so the host can shut down, and that is checked below as well.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGES = Path(__file__).resolve().parents[3]

#: The one deliberate firewall, as ``path:function``.
_ALLOWED = {"z4j-bare/src/z4j_bare/safety.py:safe_call"}


def _require_repo_layout() -> None:
    """Skip when the sibling package sources are not on disk.

    This walks ``packages/*/src`` to check a property of the whole workspace.
    Run from an installed distribution, which is how a downstream packager
    verifies a build, that tree does not exist: the scan finds nothing, the
    allowlist matches nothing, and the vacuity companion below fails with a
    message about the firewall having moved. Wrong diagnosis, and a hard
    failure where the sibling scanners in z4j-core skip.

    Skipping is right here because the property is about the repository, not
    about the installed package. Nothing is lost: the suite that gates the
    release runs from the repo, where this does not skip.
    """
    if not (PACKAGES / "z4j-bare" / "src").is_dir():
        pytest.skip("package sources are not in this checkout layout")


def _handlers_that_drop_base_exception() -> list[str]:
    """Every ``except BaseException`` that neither re-raises nor forwards."""
    offenders: list[str] = []
    for path in PACKAGES.rglob("src/**/*.py"):
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue

        enclosing: dict[int, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                for child in ast.walk(node):
                    enclosing.setdefault(getattr(child, "lineno", -1), node.name)

        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler) or node.type is None:
                continue
            names = {n.id for n in ast.walk(node.type) if isinstance(n, ast.Name)}
            if "BaseException" not in names:
                continue

            # A bare ``raise`` anywhere in the handler used to count, so
            # ``if shutting_down: raise`` followed by ``return None`` read as a
            # re-raise. Only an unconditional one at the handler's top level
            # guarantees the exception leaves.
            reraises = any(isinstance(stmt, ast.Raise) for stmt in node.body)

            # Forwarding means the exception object reaches something that will
            # surface it: a future, a queue, a callback. Three things that are
            # NOT forwarding and were being counted as it:
            #
            #   logging it at any level, which the first version caught only
            #   for warning/error/exception and missed for info/debug/critical;
            #
            #   stringifying it, so ``self._last = str(exc)`` laundered a
            #   silent swallow past the check;
            #
            #   wrapping it, so ``logger.exception("failed", repr(exc))``
            #   slipped through the log-name test on the inner call.
            bound = node.name
            forwards = False
            if bound:
                for call in (c for c in ast.walk(node) if isinstance(c, ast.Call)):
                    callee = ""
                    if isinstance(call.func, ast.Name):
                        callee = call.func.id
                    elif isinstance(call.func, ast.Attribute):
                        callee = call.func.attr
                    lowered = callee.lower()
                    if "log" in lowered or lowered in {
                        "debug",
                        "info",
                        "warning",
                        "warn",
                        "error",
                        "exception",
                        "critical",
                        "print",
                        "str",
                        "repr",
                        "format",
                        "type",
                    }:
                        continue
                    carries = any(isinstance(a, ast.Name) and a.id == bound for a in ast.walk(call))
                    if carries:
                        forwards = True
                        break

            if reraises or forwards:
                continue

            rel = path.relative_to(PACKAGES).as_posix()
            offenders.append(f"{rel}:{enclosing.get(node.lineno, '<module>')}")
    return offenders


def test_only_one_place_swallows_base_exception() -> None:
    _require_repo_layout()
    unexpected = sorted(set(_handlers_that_drop_base_exception()) - _ALLOWED)
    assert unexpected == [], (
        "these swallow a BaseException without re-raising or forwarding it, so "
        "a real failure disappears silently. Only the safe_boundary firewall "
        "may do that:\n  " + "\n  ".join(unexpected)
    )


def test_the_firewall_is_still_there() -> None:
    """Guards the guard: if the allowlist entry goes stale the test above is empty."""
    _require_repo_layout()
    assert set(_handlers_that_drop_base_exception()) & _ALLOWED, (
        "the deliberate firewall no longer matches the allowlist, so the check "
        "above is passing vacuously"
    )


@pytest.mark.parametrize("signal_exc", [KeyboardInterrupt, SystemExit])
def test_shutdown_signals_are_not_swallowed(signal_exc: type[BaseException]) -> None:
    """Swallowing these blocks SIGTERM and Ctrl-C. The docstring once said we did."""
    from z4j_bare.safety import safe_call

    def raiser() -> None:
        raise signal_exc

    with pytest.raises(signal_exc):
        safe_call(raiser)


def test_an_ordinary_exception_is_swallowed() -> None:
    """The other half: a bug in z4j must not reach the host application."""
    from z4j_bare.safety import safe_call

    def raiser() -> None:
        message = "a bug in z4j"
        raise RuntimeError(message)

    assert safe_call(raiser) is None
