"""Configure the pytests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sodasql.telemetry.soda_telemetry import SodaTelemetry

# Re-initialize telemetry in test mode.
soda_telemetry = SodaTelemetry.get_instance(test_mode=True)


@pytest.fixture
def dbt_manifest() -> dict:
    """
    Get the manifest.

    Returns
    -------
    out : dict
        The manifest
    """
    manifest_file = Path(__file__).parent / "dbt/data/manifest.json"
    with manifest_file.open("r") as file:
        manifest = json.load(file)
    return manifest


@pytest.fixture
def dbt_run_results() -> dict:
    """
    Get the run_results.

    Returns
    -------
    out : dict
        The run_results
    """
    manifest_file = Path(__file__).parent / "dbt/data/run_results.json"
    with manifest_file.open("r") as file:
        manifest = json.load(file)
    return manifest
