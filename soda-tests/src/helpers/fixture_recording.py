"""The switch that re-records the pinned fixtures instead of asserting them."""

from __future__ import annotations

import os

import pytest

RECORD_FIXTURES_ENV_VAR = "SODA_TEST_RECORD_FIXTURES"

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


def _running_in_ci() -> bool:
    return os.environ.get("CI", "").lower() in _TRUTHY or os.environ.get("GITHUB_ACTIONS", "").lower() in _TRUTHY


def recording_fixtures() -> bool:
    """True when this run re-records the fixtures. A recording run skips every pin, so it fails on CI."""
    if os.environ.get(RECORD_FIXTURES_ENV_VAR) != "1":
        return False
    if _running_in_ci():
        pytest.fail(
            f"{RECORD_FIXTURES_ENV_VAR}=1 re-records the fixtures and skips every pin, so it never runs on CI",
            pytrace=False,
        )
    return True
