"""Configure the pytests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sodasql.telemetry.soda_telemetry import SodaTelemetry

# Re-initialize telemetry in test mode.
soda_telemetry = SodaTelemetry.get_instance(test_mode=True)


@pytest.fixture
def dbt_manifest_file() -> Path:
    """
    Get the path to the dbt manifest.

    Returns
    -------
    out : Path
        The dbt manifest file.
    """
    manifest_file = Path(__file__).parent / "dbt/data/manifest.json"
    return manifest_file


@pytest.fixture
def dbt_manifest(dbt_manifest_file: Path) -> dict:
    """
    Get the dbt manifest.

    Parameters
    ----------
    manifest_file : Path
        The dbt manifest file.

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
def dbt_run_results_file() -> Path:
    """
    Get the path to the dbt run results.

    Returns
    -------
    out : Path
        The dbt run results file.
    """
    run_results_file = Path(__file__).parent / "dbt/data/run_results.json"
    return run_results_file


@pytest.fixture
def dbt_run_results(dbt_run_results_file: Path) -> dict:
    """
    Get the dbt run results.

    Parameters
    ----------
    dbt_run_results_file : Path
        The dbt run results file.

    Returns
    -------
    out : dict
        The run_results
    """
    manifest_file = Path(__file__).parent / "dbt/data/run_results.json"
    with manifest_file.open("r") as file:
        manifest = json.load(file)
    return manifest
