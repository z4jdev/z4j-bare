"""Regression tests for the env/kwarg resolver in ``install_agent``.

The ``_resolve`` helper inside :mod:`z4j_bare.install` merges
explicit kwargs with ``Z4J_*`` environment variables and
per-field defaults. Audit pass 8 on 2026-04-21 surfaced the same
truthy-fallback bug the ``z4j_fastapi`` resolver had: passing
``brain_url=""`` (empty string) silently slid onto
``env.get("Z4J_BRAIN_URL")`` because ``"" or env.get(...)`` is
truthy-falsy. Fix uses ``is not None`` so explicit empties
surface as ``ConfigError`` rather than silently honouring an
env value the operator may not have intended.

These tests pin the fixed semantics so the bare-agent install
path stays symmetric with the framework adapters.
"""

from __future__ import annotations

import os

import pytest
from z4j_bare._process_singleton import clear_runtime
from z4j_bare.install import install_agent
from z4j_core.errors import ConfigError


@pytest.fixture(autouse=True)
def _clear_z4j_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in [k for k in os.environ if k.startswith("Z4J_")]:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    """Every test starts with an empty singleton.

    Without this, an earlier test could leave a runtime
    registered and later tests would short-circuit through
    ``try_register``'s loser path instead of running the
    resolver. We only care about the resolver here.
    """
    clear_runtime()
    yield
    clear_runtime()


@pytest.fixture
def _fake_engine() -> object:
    """A stub engine that satisfies ``install_agent(engines=[...])``.

    ``install_agent`` only needs the engines list to be non-empty
    at the top of the function - the stub never gets exercised
    beyond the resolver.
    """

    class _StubEngine:
        name = "stub"

        def capabilities(self) -> set[str]:
            return set()

    return _StubEngine()


@pytest.fixture
def _fake_scheduler() -> object:
    """A scheduler adapter sufficient for a non-started runtime."""

    class _StubScheduler:
        name = "stub-scheduler"

        def capabilities(self) -> set[str]:
            return set()

    return _StubScheduler()


class TestAdapterPresence:
    def test_scheduler_only_install_builds_runtime_and_registers_singleton(
        self,
        _fake_scheduler: object,
    ) -> None:
        from z4j_bare import _process_singleton

        runtime = install_agent(
            engines=[],
            schedulers=[_fake_scheduler],  # type: ignore[list-item]
            brain_url="http://brain.invalid:7700",
            token="test-token",
            project_id="test-project",
            dev_mode=True,
            autostart=False,
        )

        assert runtime.engines == {}
        assert runtime.schedulers == {"stub-scheduler": _fake_scheduler}
        assert _process_singleton.current_runtime() is runtime

    def test_engine_and_scheduler_lists_cannot_both_be_empty(self) -> None:
        with pytest.raises(
            ConfigError,
            match="at least one engine or scheduler adapter is required",
        ):
            install_agent(engines=[], schedulers=[], autostart=False)


class TestRequiredFieldsFailFast:
    def test_empty_brain_url_does_not_fall_back_to_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
        _fake_engine: object,
    ) -> None:
        monkeypatch.setenv("Z4J_BRAIN_URL", "http://env-url:7700")
        monkeypatch.setenv("Z4J_TOKEN", "env-token")
        monkeypatch.setenv("Z4J_PROJECT_ID", "env-project")
        with pytest.raises(ConfigError, match="Z4J_BRAIN_URL"):
            install_agent(
                engines=[_fake_engine],
                brain_url="",
                token="t",
                project_id="p",
            )

    def test_empty_token_does_not_fall_back_to_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
        _fake_engine: object,
    ) -> None:
        monkeypatch.setenv("Z4J_BRAIN_URL", "http://env-url:7700")
        monkeypatch.setenv("Z4J_TOKEN", "env-token")
        monkeypatch.setenv("Z4J_PROJECT_ID", "env-project")
        with pytest.raises(ConfigError, match="Z4J_TOKEN"):
            install_agent(
                engines=[_fake_engine],
                brain_url="http://u",
                token="",
                project_id="p",
            )

    def test_empty_project_id_does_not_fall_back_to_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
        _fake_engine: object,
    ) -> None:
        monkeypatch.setenv("Z4J_BRAIN_URL", "http://env-url:7700")
        monkeypatch.setenv("Z4J_TOKEN", "env-token")
        monkeypatch.setenv("Z4J_PROJECT_ID", "env-project")
        with pytest.raises(ConfigError, match="Z4J_PROJECT_ID"):
            install_agent(
                engines=[_fake_engine],
                brain_url="http://u",
                token="t",
                project_id="",
            )

    def test_all_three_empty_lists_all_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        _fake_engine: object,
    ) -> None:
        # The headline of the audit: install_agent(brain_url='',
        # token='', project_id='') used to silently build a config
        # from env. Now the error message names every missing
        # field so the operator sees exactly what they forgot.
        monkeypatch.setenv("Z4J_BRAIN_URL", "http://env-url:7700")
        monkeypatch.setenv("Z4J_TOKEN", "env-token")
        monkeypatch.setenv("Z4J_PROJECT_ID", "env-project")
        with pytest.raises(ConfigError) as excinfo:
            install_agent(
                engines=[_fake_engine],
                brain_url="",
                token="",
                project_id="",
            )
        msg = str(excinfo.value)
        assert "Z4J_BRAIN_URL" in msg
        assert "Z4J_TOKEN" in msg
        assert "Z4J_PROJECT_ID" in msg


class TestAutostartFailureClearsSingletonR7P26:
    """install_agent(autostart=True) registers the runtime in the
    process singleton BEFORE start(). If start() then raises, the poisoned
    never-started runtime must be UNREGISTERED so a later install_agent can
    register + start a fresh one -- otherwise every subsequent install short-
    circuits through try_register's loser path and returns the dead runtime."""

    def test_start_failure_unregisters_the_poisoned_runtime(
        self,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
        _fake_engine: object,
    ) -> None:
        import secrets

        from z4j_bare import _process_singleton
        from z4j_bare.runtime import AgentRuntime

        def _boom(self: object) -> None:
            raise RuntimeError("start blew up (transient buffer-init error)")

        monkeypatch.setattr(AgentRuntime, "start", _boom)

        with pytest.raises(RuntimeError, match="start blew up"):
            install_agent(
                engines=[_fake_engine],
                brain_url="http://u:7700",
                token="test-token-12345678901234567890",
                project_id="p",
                hmac_secret=secrets.token_hex(32),
                autostart=True,
                dev_mode=True,
                buffer_path=tmp_path / "buf.sqlite",  # type: ignore[operator]
            )

        # The singleton is empty again: a fresh registration WINS (returns its
        # own object) instead of losing to the poisoned runtime.
        sentinel = object()
        assert _process_singleton.try_register(sentinel, owner="probe") is sentinel
