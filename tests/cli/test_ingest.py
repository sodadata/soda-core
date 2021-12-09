"""Test the ingest module."""

from pathlib import Path

import pytest

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
    dbt_manifest_file: Path,
    dbt_run_results_file: Path,
    mock_soda_server_client: MockSodaServerClient,
    command_type: str,
) -> None:
    """Validate that the flush test results has the expected command types."""
    test_results_iterator = ingest.create_dbt_test_results_iterator(
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
