"""Unit tests for the memory-optimized driver toggle's two sources.

The driver is enabled when EITHER soda-extensions' Soda Cloud override
(``memory_optimized_driver_settings.configure``, from the ``useMemoryOptimized``
feature flag) OR the ``MEMORY_OPTIMIZED_DRIVER_ENABLED`` env var is on — ORed, so
neither can force the other off.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from soda_core.common.data_source_connection import memory_optimized_driver_settings

ENV = "MEMORY_OPTIMIZED_DRIVER_ENABLED"


@pytest.fixture(autouse=True)
def _reset_settings():
    # The settings object is process-level state; reset around every test so order can't leak it.
    memory_optimized_driver_settings.reset()
    yield
    memory_optimized_driver_settings.reset()


def test_disabled_when_neither_env_nor_override(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert memory_optimized_driver_settings.is_enabled() is False


def test_override_enables_without_env(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    memory_optimized_driver_settings.configure(True)
    assert memory_optimized_driver_settings.is_enabled() is True


def test_env_enables_without_override(monkeypatch):
    monkeypatch.setenv(ENV, "true")
    assert memory_optimized_driver_settings.is_enabled() is True


def test_either_source_enables_or_semantics(monkeypatch):
    # Env explicitly off, override on → still enabled (override can't be forced off).
    monkeypatch.setenv(ENV, "false")
    memory_optimized_driver_settings.configure(True)
    assert memory_optimized_driver_settings.is_enabled() is True


def test_falsy_override_leaves_env_as_sole_control(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    memory_optimized_driver_settings.configure(None)
    assert memory_optimized_driver_settings.is_enabled() is False


def test_reset_clears_a_previously_set_override(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    memory_optimized_driver_settings.configure(True)
    assert memory_optimized_driver_settings.is_enabled() is True
    memory_optimized_driver_settings.reset()
    assert memory_optimized_driver_settings.is_enabled() is False


def test_log_active_once_logs_only_once_until_reset():
    # The "driver active" line must be emitted at most once per process so it does
    # not spam per query; reset() re-arms it.
    with patch("soda_core.common.data_source_connection.logger") as mock_logger:
        memory_optimized_driver_settings.log_active_once("postgres")
        memory_optimized_driver_settings.log_active_once("postgres")
        assert mock_logger.info.call_count == 1

        memory_optimized_driver_settings.reset()
        memory_optimized_driver_settings.log_active_once("postgres")
        assert mock_logger.info.call_count == 2
