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

from z4j_core.errors import ConfigError

from z4j_bare._process_singleton import clear_runtime
from z4j_bare.install import install_agent


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
