"""Test the ingest module."""

from __future__ import annotations

import requests
from pathlib import Path
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch

from sodasql.cli import ingest
from tests.common.mock_soda_server_client import MockSodaServerClient


@pytest.fixture
def mock_soda_server_client() -> MockSodaServerClient:
    """
    Mock the soda server client.

    Returns
    -------
    out : MockSodaServerClient
        The mocked soda server client.
    """
    return MockSodaServerClient()


@pytest.mark.parametrize(
    "command_type", ["sodaSqlScanStart", "sodaSqlScanTestResults", "sodaSqlScanEnd"]
)
def test_dbt_flush_test_results_soda_server_has_command_types(
    dbt_manifest: dict,
    dbt_run_results: dict,
    mock_soda_server_client: MockSodaServerClient,
    command_type: str,
) -> None:
    """Validate that the flush test results has the expected command types."""
    test_results_iterator = ingest.map_dbt_test_results_iterator(
        dbt_manifest, dbt_run_results
    )
    ingest.flush_test_results(
        test_results_iterator,
        mock_soda_server_client,
        warehouse_name="test",
        warehouse_type="test",
    )

    assert any(
        command_type == command["type"] for command in mock_soda_server_client.commands
    )


def test_dbt_flush_test_results_soda_server_scan_numbertest_result(
    dbt_manifest: dict,
    dbt_run_results: dict,
    mock_soda_server_client: MockSodaServerClient,
) -> None:
    """Validate if we have the expected number of test results."""
    test_results_iterator = ingest.map_dbt_test_results_iterator(
        dbt_manifest, dbt_run_results
    )
    ingest.flush_test_results(
        test_results_iterator,
        mock_soda_server_client,
        warehouse_name="test",
        warehouse_type="test",
    )

    # We expect three commands: scan start, test result, scan end
    test_results_commands = [
        command
        for command in mock_soda_server_client.commands
        if command["type"] == "sodaSqlScanTestResults"
    ]
    assert sum(len(command["testResults"]) for command in test_results_commands) == 3


@pytest.mark.parametrize(
    "column, value",
    [
        ("passed", True),
        ("skipped", False),
        ("values", {"failures": 0}),
        ("columnName", "result"),
        ("source", "dbt"),
    ],
)
def test_dbt_flush_test_results_soda_server_scan_test_result(
    dbt_manifest: dict,
    dbt_run_results: dict,
    mock_soda_server_client: MockSodaServerClient,
    column: str,
    value: Any,
) -> None:
    """Validate if the a scan test result is as expected."""
    id = "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f"

    test_results_iterator = ingest.map_dbt_test_results_iterator(
        dbt_manifest, dbt_run_results
    )
    ingest.flush_test_results(
        test_results_iterator,
        mock_soda_server_client,
        warehouse_name="test",
        warehouse_type="test",
    )

    # We expect three commands: scan start, test result, scan end
    test_results_command = mock_soda_server_client.commands[1]
    assert test_results_command["type"] == "sodaSqlScanTestResults"

    test_results = [
        test_result
        for test_result in test_results_command["testResults"]
        if test_result["id"] == id
    ]

    assert len(test_results) == 1, f"expected one test result: {test_results}"
    assert test_results[0][column] == value


def test_load_dbt_artifacts_given_artifacts(
    dbt_manifest_file: Path,
    dbt_manifest: dict,
    dbt_run_results_file: Path,
    dbt_run_results: dict,
) -> None:
    """Test load dbt artifacts given an artifacts directory."""
    manifest, run_results = ingest.load_dbt_artifacts(
        dbt_manifest_file, dbt_run_results_file
    )
    assert dbt_manifest == manifest
    assert dbt_run_results == run_results


@pytest.fixture
def mock_dbt_cloud_response(
    monkeypatch: MonkeyPatch,
    dbt_manifest_file: Path,
    dbt_run_results_file: Path,
) -> None:
    """
    Mock the dbt cloud response.

    Parameters
    ----------
    monkeypatch : MonkeyPatch
        The monkey patch fixture.
    dbt_manifest_file : Path
        The path to the manifest file.
    dbt_run_results_file : Path
        The path to the run results file.
    """

    def get(url: str, headers: dict | None = None):
        """Mock the requests.get method."""
        response = requests.Response()
        if "manifest.json" in url:
            file = dbt_manifest_file
        elif "run_results.json" in url:
            file = dbt_run_results_file
        else:
            raise ValueError(f"Unrecognized url: {url}")

        with file.open("rb") as f:
            response._content = f.read()

        response.status_code = requests.codes.ok
        return response

    monkeypatch.setattr(requests, "get", get)


def test_download_dbt_artifacts_from_cloud(
    mock_dbt_cloud_response: None,
    dbt_manifest: dict,
    dbt_run_results: dict,
):
    """Test if the dbt manifest and run results are returned."""
    manifest, run_results = ingest.download_dbt_artifacts_from_cloud(
        "api_token", "account_id", "run_id"
    )

    assert dbt_manifest == manifest
    assert dbt_run_results == run_results
