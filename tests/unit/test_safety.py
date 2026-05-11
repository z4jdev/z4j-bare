"""Unit tests for ``z4j_bare.safety``.

The safety wrappers are the single most important firewall between
z4j code and the host application. Every test here must pass 100%
of the time.
"""

from __future__ import annotations

import logging

import pytest

from z4j_bare.safety import safe_boundary, safe_call


class TestSafeCall:
    def test_returns_function_value_on_success(self) -> None:
        result = safe_call(lambda: 42)
        assert result == 42

    def test_passes_args_and_kwargs(self) -> None:
        def add(a: int, b: int = 0) -> int:
            return a + b

        assert safe_call(add, 2, b=3) == 5

    def test_returns_none_on_exception(self) -> None:
        def boom() -> None:
            raise ValueError("nope")

        assert safe_call(boom) is None

    def test_propagates_keyboard_interrupt(self) -> None:
        # Process lifecycle signals must propagate - they are NOT
        # application errors and must not be swallowed. See audit
        # v2 finding #3.
        def boom() -> None:
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            safe_call(boom)

    def test_propagates_system_exit(self) -> None:
        def boom() -> None:
            raise SystemExit(2)

        with pytest.raises(SystemExit):
            safe_call(boom)

    def test_logs_exception_type_not_message(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.ERROR, logger="z4j.runtime.safety")

        def boom() -> None:
            raise RuntimeError("api_key=sk_live_secret_value")

        safe_call(boom)
        # The log line records the exception class name so an
        # operator can see *what* failed, but it must NOT echo the
        # message (which may contain credentials lifted from host
        # code). The traceback IS attached via exc_info, but the
        # message-line itself stays clean.
        assert any("RuntimeError" in rec.message for rec in caplog.records)
        assert not any(
            "sk_live_secret_value" in rec.message for rec in caplog.records
        )


class TestSafeBoundaryDecorator:
    def test_happy_path_returns_value(self) -> None:
        @safe_boundary
        def double(x: int) -> int:
            return x * 2

        assert double(5) == 10

    def test_raises_is_swallowed(self) -> None:
        @safe_boundary
        def bad() -> None:
            raise RuntimeError("fail")

        assert bad() is None

    def test_preserves_name(self) -> None:
        @safe_boundary
        def my_handler() -> None:
            pass

        assert my_handler.__name__ == "my_handler"
