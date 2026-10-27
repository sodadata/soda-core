"""Contract verification with -d/--dataset when Soda Cloud cannot hand over the contract.

A failed fetch, or a fetch that returns no contract, fails the run before anything is verified. The Python API
raises ``ContractFetchFailedException``. The CLI turns it into a ``ScanExecutionFailedException``, which its
failure boundary reports: exit code 3, and a managed run marks its scan failed first.
"""

from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import ContractFetchFailedException, ContractNotFoundException, SodaCloudException
from soda_core.contracts.api.verify_api import verify_contract, verify_contracts_locally

DATASET = "my_data_source/my_db/my_schema/customers"
CONTRACT_YAML = f"dataset: {DATASET}\nchecks:\n  - row_count:\n"
SCAN_ID = "scan-under-test"


def test_contract_not_found_message_names_the_dataset_as_typed():
    message = str(ContractNotFoundException(DatasetIdentifier.parse(DATASET)))

    assert f"No data contract found for dataset '{DATASET}' in Soda Cloud." in message


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_through_the_deprecated_plural_api_raises(mock_from_config):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    with pytest.warns(DeprecationWarning), pytest.raises(ContractFetchFailedException):
        verify_contracts_locally(
            dataset_identifiers=[DATASET],
            data_source_file_paths=["ds.yaml"],
            soda_cloud_file_path="sc.yaml",
        )


@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_contract_file_wins_over_dataset_and_is_not_fetched(mock_from_config, mock_execute):
    mock_execute.return_value = MagicMock()

    verify_contract(
        contract_file_path="contract.yaml",
        dataset_identifier=DATASET,
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=False,
    )

    mock_from_config.return_value.fetch_contract_for_dataset.assert_not_called()
    [contract_yaml_source] = mock_execute.call_args.kwargs["contract_yaml_sources"]
    assert contract_yaml_source.file_path == "contract.yaml"


def _handle_verify(publish: bool = False):
    return handle_verify_contract(
        contract_file_path=None,
        dataset_identifier=DATASET,
        data_source_file_paths=["ds.yaml"],
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=publish,
        verbose=False,
    )


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_the_cli_boundary_on_a_managed_run_without_a_failure_channel_exits_4(mock_from_config, monkeypatch):
    # Without a channel to report through, Soda Cloud cannot have the failure, so exit 4 instead of claiming it
    # was delivered.
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    exit_code = run_scan(soda_cloud=None, command=lambda logs: _handle_verify(publish=True))

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()
