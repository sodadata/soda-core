"""Contract verification with -d/--dataset when Soda Cloud cannot hand over the contract.

A failed fetch, or a fetch that returns no contract, fails the run before anything is verified. The Python API
raises ``ContractFetchFailedException``. The CLI turns it into a ``ScanExecutionFailedException``, which its
failure boundary reports: exit code 3, and a managed run marks its scan failed first.
"""

import pickle
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import (
    ContractFetchFailedException,
    ContractNotFoundException,
    DatasetNotFoundException,
    DatasetQueryException,
    DataSourceNotFoundException,
    ScanExecutionFailedException,
    SodaCloudException,
)
from soda_core.contracts.api.verify_api import verify_contract, verify_contracts_locally
from soda_core.contracts.contract_verification import ContractVerificationSessionResult

DATASET = "my_data_source/my_db/my_schema/customers"
CONTRACT_YAML = f"dataset: {DATASET}\nchecks:\n  - row_count:\n"
SCAN_ID = "scan-under-test"


def _fetch_failures() -> list:
    """Each exception the fetch raises, with the reason the error line gives for it."""
    parsed = DatasetIdentifier.parse(DATASET)
    return [
        pytest.param(SodaCloudException("Soda Cloud is down"), "Soda Cloud is down", id="SodaCloudException"),
        pytest.param(
            ContractNotFoundException(parsed),
            "the dataset has no published contract in Soda Cloud",
            id="ContractNotFoundException",
        ),
        pytest.param(
            DatasetNotFoundException(parsed), "the dataset is unknown in Soda Cloud", id="DatasetNotFoundException"
        ),
        pytest.param(
            DataSourceNotFoundException(parsed),
            "data source 'my_data_source' is unknown in Soda Cloud",
            id="DataSourceNotFoundException",
        ),
    ]


# The transport tests cover every empty form Soda Cloud can send.
EMPTY_CONTRACTS = [
    pytest.param(None, id="none"),
    pytest.param("  \n\t \n", id="whitespace"),
]


def _verify(use_runner: bool = False) -> ContractVerificationSessionResult:
    return verify_contract(
        contract_file_path=None,
        dataset_identifier=DATASET,
        data_source_file_path=None if use_runner else "ds.yaml",
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=False,
        use_runner=use_runner,
    )


@pytest.mark.parametrize("use_runner", [False, True], ids=["local", "runner"])
@pytest.mark.parametrize("fetch_exception, reason", _fetch_failures())
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_raises_naming_the_dataset_once(
    mock_from_config, mock_execute, fetch_exception, reason, use_runner
):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_exception

    with pytest.raises(ContractFetchFailedException) as exc_info:
        _verify(use_runner=use_runner)

    assert str(exc_info.value) == f"Could not fetch the contract for dataset '{DATASET}': {reason}"
    assert exc_info.value.dataset_identifier == DATASET
    assert exc_info.value.__cause__ is fetch_exception
    mock_execute.assert_not_called()


@pytest.mark.parametrize("use_runner", [False, True], ids=["local", "runner"])
@pytest.mark.parametrize("fetched_contract", EMPTY_CONTRACTS)
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_fetch_without_a_contract_raises_naming_the_dataset(
    mock_from_config, mock_execute, fetched_contract: Optional[str], use_runner
):
    mock_from_config.return_value.fetch_contract_for_dataset.return_value = fetched_contract

    with pytest.raises(ContractFetchFailedException) as exc_info:
        _verify(use_runner=use_runner)

    assert (
        str(exc_info.value) == f"Could not fetch the contract for dataset '{DATASET}': Soda Cloud returned no contract"
    )
    mock_execute.assert_not_called()


def test_contract_not_found_message_names_the_dataset_as_typed():
    message = str(ContractNotFoundException(DatasetIdentifier.parse(DATASET)))

    assert f"No data contract found for dataset '{DATASET}' in Soda Cloud." in message


def _fetch_exceptions() -> list:
    """Each exception with a reason that a fetch raises, and the one verify_contract raises for it."""
    parsed = DatasetIdentifier.parse(DATASET)
    return [
        pytest.param(
            DatasetQueryException(
                f"Failed 'sodaCoreGetContract' for dataset '{DATASET}': boom",
                reason="Soda Cloud returned status 500: boom",
            ),
            id="DatasetQueryException",
        ),
        pytest.param(ContractNotFoundException(parsed), id="ContractNotFoundException"),
        pytest.param(DatasetNotFoundException(parsed), id="DatasetNotFoundException"),
        pytest.param(DataSourceNotFoundException(parsed), id="DataSourceNotFoundException"),
        pytest.param(
            ContractFetchFailedException(DATASET, "Soda Cloud returned no contract"), id="ContractFetchFailedException"
        ),
    ]


@pytest.mark.parametrize("exception", _fetch_exceptions())
def test_fetch_exception_survives_a_pickle_round_trip(exception):
    # A caller that runs verify_contract in a worker process gets the exception back pickled.
    round_tripped = pickle.loads(pickle.dumps(exception))

    assert type(round_tripped) is type(exception)
    assert str(round_tripped) == str(exception)
    assert round_tripped.args == exception.args
    assert round_tripped.message == exception.message
    assert round_tripped.reason == exception.reason


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_on_a_managed_run_raises_without_marking(mock_from_config, monkeypatch):
    # The Python API never reports to Soda Cloud; the CLI failure boundary does.
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    with pytest.raises(ContractFetchFailedException):
        _verify()

    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()


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
def test_handle_verify_contract_raises_a_scan_execution_failure_for_a_failed_fetch(mock_from_config):
    fetch_exception = SodaCloudException("Soda Cloud is down")
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_exception

    with pytest.raises(ScanExecutionFailedException) as exc_info:
        _handle_verify()

    assert str(exc_info.value) == f"Could not fetch the contract for dataset '{DATASET}': Soda Cloud is down"
    assert isinstance(exc_info.value.__cause__, ContractFetchFailedException)


@pytest.mark.parametrize(
    "fetch_side_effect, fetch_return_value",
    [(SodaCloudException("Soda Cloud is down"), None), (None, None), (None, "\n")],
    ids=["fetch-raises", "no-contract", "whitespace-contract"],
)
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_the_cli_boundary_exits_3_when_the_contract_cannot_be_fetched(
    mock_from_config, fetch_side_effect, fetch_return_value, monkeypatch
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_side_effect
    mock_from_config.return_value.fetch_contract_for_dataset.return_value = fetch_return_value

    exit_code = run_scan(soda_cloud=None, command=lambda logs: _handle_verify())

    assert exit_code == ExitCode.LOG_ERRORS
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_the_cli_boundary_on_a_managed_run_without_a_failure_channel_exits_4(mock_from_config, monkeypatch):
    # Without a channel to report through, Soda Cloud cannot have the failure, so exit 4 instead of claiming it
    # was delivered.
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    exit_code = run_scan(soda_cloud=None, command=lambda logs: _handle_verify(publish=True))

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()
