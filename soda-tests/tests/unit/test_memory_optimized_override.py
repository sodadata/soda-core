"""Unit tests for the memory-optimized driver toggle's two sources.

The driver is enabled when EITHER soda-extensions' Soda Cloud override
(``set_memory_optimized_override``, from the ``useMemoryOptimized`` feature flag)
OR the ``MEMORY_OPTIMIZED_DRIVER_ENABLED`` env var is on — ORed, so neither can
force the other off.
"""

from __future__ import annotations

import pytest
from soda_core.common.data_source_connection import (
    is_memory_optimized_driver_enabled,
    set_memory_optimized_override,
)

ENV = "MEMORY_OPTIMIZED_DRIVER_ENABLED"


@pytest.fixture(autouse=True)
def _reset_override():
    # The override is process-level state; reset around every test so order can't leak it.
    set_memory_optimized_override(False)
    yield
    set_memory_optimized_override(False)


def test_disabled_when_neither_env_nor_override(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert is_memory_optimized_driver_enabled() is False


def test_override_enables_without_env(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    set_memory_optimized_override(True)
    assert is_memory_optimized_driver_enabled() is True


def test_env_enables_without_override(monkeypatch):
    monkeypatch.setenv(ENV, "true")
    assert is_memory_optimized_driver_enabled() is True


def test_either_source_enables_or_semantics(monkeypatch):
    # Env explicitly off, override on → still enabled (override can't be forced off).
    monkeypatch.setenv(ENV, "false")
    set_memory_optimized_override(True)
    assert is_memory_optimized_driver_enabled() is True


def test_falsy_override_leaves_env_as_sole_control(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    set_memory_optimized_override(None)
    assert is_memory_optimized_driver_enabled() is False
