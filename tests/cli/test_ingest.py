"""Test the ingest module."""

from pathlib import Path
from typing import Any

import pytest

from sodasql.cli import ingest
from sodasql.cli.ingest import resolve_artifacts_paths
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
    dbt_manifest_file: Path,
    dbt_run_results_file: Path,
    mock_soda_server_client: MockSodaServerClient,
    command_type: str,
) -> None:
    """Validate that the flush test results has the expected command types."""
    test_results_iterator = ingest.map_dbt_test_results_iterator(
        dbt_manifest_file, dbt_run_results_file
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
    dbt_manifest_file: Path,
    dbt_run_results_file: Path,
    mock_soda_server_client: MockSodaServerClient,
) -> None:
    """Validate if we have the expected number of test results."""
    test_results_iterator = ingest.map_dbt_test_results_iterator(
        dbt_manifest_file, dbt_run_results_file
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
    assert sum(len(command["testResults"]) for command in test_results_commands) == 2


@pytest.mark.parametrize(
    "column, value",
    [
        ("passed", True),
        ("skipped", False),
        ("values", {"failures": 0}),
        ("columnName", "result"),
        ('source', 'dbt'),
    ],
)
def test_dbt_flush_test_results_soda_server_scan_test_result(
    dbt_manifest_file: Path,
    dbt_run_results_file: Path,
    mock_soda_server_client: MockSodaServerClient,
    column: str,
    value: Any,
) -> None:
    """Validate if the a scan test result is as expected."""
    id = "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f"

    test_results_iterator = ingest.map_dbt_test_results_iterator(
        dbt_manifest_file, dbt_run_results_file
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


@pytest.mark.parametrize(
    "dbt_artifacts, dbt_manifest, dbt_run_results, expectation",
    [
        pytest.param(
            Path('my_dbt_project/target/'),
            "",
            "",
            [
                Path('my_dbt_project/target/manifest.json'),
                Path('my_dbt_project/target/run_results.json'),
            ],
            id="dbt_artifacts path provided, others null"
        ),
        pytest.param(
            Path('my_dbt_project/target/'),
            Path('my_dbt_project/path_to_ignore'),
            Path('my_dbt_project/path_to_ignore'),
            [
                Path('my_dbt_project/target/manifest.json'),
                Path('my_dbt_project/target/run_results.json'),
            ],
            id="dbt_artifacts path provided, others provided, but should be ignored"
        ),
    ]
)
def test_resolve_artifacts_paths(dbt_artifacts, dbt_manifest, dbt_run_results, expectation):
    dbt_manifest, dbt_run_results = resolve_artifacts_paths(dbt_artifacts, dbt_manifest, dbt_run_results)
    assert dbt_manifest == expectation[0]
    assert dbt_run_results == expectation[1]


@pytest.mark.parametrize(
    "dbt_artifacts, dbt_manifest, dbt_run_results",
    [
        pytest.param(
            None,
            None,
            Path('my_dbt_project/target/run_results.json'),
            id="missing dbt_manifest and artifact",
        ),
        pytest.param(
            None,
            Path('my_dbt_project/target/run_results.json'),
            None,
            id="missing dbt_run_results and artifact",
        ),
    ]
)
def test_resolve_artifacts_paths_missing_paths(dbt_artifacts, dbt_manifest, dbt_run_results):
    with pytest.raises(ValueError):
        dbt_manifest, dbt_run_results = resolve_artifacts_paths(dbt_artifacts, dbt_manifest, dbt_run_results)
