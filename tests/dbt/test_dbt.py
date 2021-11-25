from __future__ import annotations

import json
from pathlib import Path

import pytest
from dbt.contracts.results import TestStatus

from sodasql import dbt as soda_dbt


@pytest.fixture
def manifest() -> dict:
    """
    Get the manifest.

    Returns
    -------
    out : dict
        The manifest
    """
    manifest_file = Path(__file__).parent / "data/manifest.json"
    with manifest_file.open("r") as file:
        manifest = json.load(file)
    return manifest


@pytest.fixture
def run_results() -> dict:
    """
    Get the run_results.

    Returns
    -------
    out : dict
        The run_results
    """
    manifest_file = Path(__file__).parent / "data/run_results.json"
    with manifest_file.open("r") as file:
        manifest = json.load(file)
    return manifest


def test_parse_manifest_raises_not_implemented_error() -> None:
    """
    A NotImplementedError should be raised when manifest version is not v3.
    """
    with pytest.raises(NotImplementedError):
        soda_dbt.parse_manifest({"metadata": {"dbt_schema_version": "not v3"}})


@pytest.mark.parametrize("unique_id", ["model.soda.stg_soda__scan"])
def test_parse_manifest_contains_model_unique_ids(unique_id: str, manifest: dict) -> None:
    """Validate the model unique_id are present in the test_nodes."""
    model_nodes, _ = soda_dbt.parse_manifest(manifest)

    assert unique_id in model_nodes.keys()


@pytest.mark.parametrize(
    "unique_id",
    [
        "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f",
        "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e",
    ],
)
def test_parse_manifest_contains_test_unique_ids(unique_id: str, manifest: dict) -> None:
    """Validate the test unique_id are present in the test_nodes."""
    _, test_nodes = soda_dbt.parse_manifest(manifest)

    assert unique_id in test_nodes.keys()


def test_parse_run_results_raises_not_implemented_error() -> None:
    """
    A NotImplementedError should be raised when run results version is not v3.
    """
    with pytest.raises(NotImplementedError):
        soda_dbt.parse_run_results({"metadata": {"dbt_schema_version": "not v3"}})


@pytest.mark.parametrize(
    "result_index, status",
    [
        (0, TestStatus.Pass),
        (1, TestStatus.Fail),
    ],
)
def test_parse_run_results_status(
    result_index: int,
    status: TestStatus,
    run_results: dict,
) -> None:
    """Validate the status of the nth result."""
    parsed_run_results = soda_dbt.parse_run_results(run_results)

    assert parsed_run_results[result_index].status == status


@pytest.mark.parametrize(
    "result_index, failures",
    [
        (0, 0),
        (1, 3),
    ],
)
def test_parse_run_results_failures(
    result_index: int,
    failures: int,
    run_results: dict,
) -> None:
    """Validate the failures of the nth result."""
    parsed_run_results = soda_dbt.parse_run_results(run_results)

    assert parsed_run_results[result_index].failures == failures


@pytest.mark.parametrize(
    "result_index, unique_id",
    [
        (0, "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f"),
        (1, "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e"),
    ],
)
def test_parse_run_results_unique_id(
    result_index: int, unique_id: str, run_results: dict
) -> None:
    """Validate the unique_id of the nth result."""
    parsed_run_results = soda_dbt.parse_run_results(run_results)

    assert parsed_run_results[result_index].unique_id == unique_id


@pytest.mark.parametrize(
    "model_name, test_names",
    [
        (
            "model.soda.stg_soda__scan",
            {
                "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f",
                "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e",
            },
        ),
    ],
)
def test_create_models_to_test_mapping(
    model_name: str,
    test_names: set[str],
    manifest: dict,
    run_results: dict,
):
    """Check if the expected models are found."""

    model_nodes, test_nodes = soda_dbt.parse_manifest(manifest)
    parsed_run_results = soda_dbt.parse_run_results(run_results)

    models_with_tests = soda_dbt.create_models_to_tests_mapping(
        model_nodes, test_nodes, parsed_run_results
    )

    assert all(
        test_name in test_names
        for test_name in models_with_tests[model_name]
    )
