"""The switch that updates the pinned snapshots instead of asserting them.

Not to be confused with ``SODA_TEST_SNAPSHOT``, which records and replays data source queries.
"""

from __future__ import annotations

import os

import pytest

UPDATE_SNAPSHOTS_ENV_VAR = "SODA_TEST_UPDATE_SNAPSHOTS"

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


def _running_in_ci() -> bool:
    return os.environ.get("CI", "").lower() in _TRUTHY or os.environ.get("GITHUB_ACTIONS", "").lower() in _TRUTHY


def updating_snapshots() -> bool:
    """True when this run updates the snapshots. An updating run skips every pin, so it fails on CI."""
    if os.environ.get(UPDATE_SNAPSHOTS_ENV_VAR) != "1":
        return False
    if _running_in_ci():
        pytest.fail(
            f"{UPDATE_SNAPSHOTS_ENV_VAR}=1 updates the snapshots and skips every pin, so it never runs on CI",
            pytrace=False,
        )
    return True
