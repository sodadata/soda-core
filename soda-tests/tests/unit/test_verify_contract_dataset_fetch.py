"""Contract verification with -d/--dataset when Soda Cloud cannot hand over the contract.

A failed fetch, or a fetch that returns no contract, fails the run. The Python API returns a
result that has errors instead of raising, and the CLI maps that result to exit code 3. A
managed run marks its scan failed first, so exit code 3 means Soda Cloud has the failure.
"""

import pickle
import sys
from logging import ERROR
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import (
    ContractFetchFailedException,
    ContractNotFoundException,
    DatasetNotFoundException,
    DatasetQueryException,
    DataSourceNotFoundException,
    SodaCloudException,
)
from soda_core.contracts.api.verify_api import verify_contract, verify_contracts_locally
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    ContractVerificationSessionResult,
    SodaException,
)

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


EMPTY_CONTRACTS = [
    pytest.param(None, id="none"),
    pytest.param("", id="empty"),
    pytest.param("\n", id="newline"),
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
def test_failed_fetch_returns_a_result_with_one_error_naming_the_dataset_once(
    mock_from_config, mock_execute, fetch_exception, reason, use_runner
):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_exception

    result = _verify(use_runner=use_runner)

    assert result.get_errors() == [f"Could not fetch the contract for dataset '{DATASET}': {reason}"]
    assert result.has_errors
    assert not result.is_ok
    assert not result.is_passed
    mock_execute.assert_not_called()


@pytest.mark.parametrize("use_runner", [False, True], ids=["local", "runner"])
@pytest.mark.parametrize("fetched_contract", EMPTY_CONTRACTS)
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_fetch_without_a_contract_returns_a_result_with_an_error_naming_the_dataset(
    mock_from_config, mock_execute, fetched_contract: Optional[str], use_runner
):
    mock_from_config.return_value.fetch_contract_for_dataset.return_value = fetched_contract

    result = _verify(use_runner=use_runner)

    assert result.get_errors() == [
        f"Could not fetch the contract for dataset '{DATASET}': Soda Cloud returned no contract"
    ]
    assert result.has_errors
    assert not result.is_ok
    mock_execute.assert_not_called()


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_result_names_the_dataset_and_chains_the_exception(mock_from_config):
    fetch_exception = SodaCloudException("Soda Cloud is down")
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_exception

    result = _verify()

    [contract_result] = result.contract_verification_results
    assert contract_result.status is CheckCollectionStatus.ERROR
    assert contract_result.check_collection.soda_qualified_dataset_name == DATASET
    assert isinstance(contract_result.error, ContractFetchFailedException)
    assert contract_result.error.dataset_identifier == DATASET
    assert contract_result.error.reason == "Soda Cloud is down"
    assert contract_result.error.__cause__ is fetch_exception
    assert contract_result.check_results == []
    assert not contract_result.sending_results_to_soda_cloud_failed


def test_contract_not_found_message_names_the_dataset_as_typed():
    message = str(ContractNotFoundException(DatasetIdentifier.parse(DATASET)))

    assert f"No data contract found for dataset '{DATASET}' in Soda Cloud." in message
    assert "DatasetIdentifier(" not in message


def _fetch_exceptions() -> list:
    """Each exception with a reason that a fetch raises or a fetch failure result carries."""
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
    # A caller that runs verify_contract in a worker process gets the result and its error back
    # pickled.
    round_tripped = pickle.loads(pickle.dumps(exception))

    assert type(round_tripped) is type(exception)
    assert str(round_tripped) == str(exception)
    assert round_tripped.args == exception.args
    assert round_tripped.message == exception.message
    assert round_tripped.reason == exception.reason


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_error_is_logged(mock_from_config, caplog):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    _verify()

    assert [(r.levelno, r.getMessage()) for r in caplog.records if r.levelno >= ERROR] == [
        (ERROR, f"Could not fetch the contract for dataset '{DATASET}': Soda Cloud is down")
    ]


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_fails_assert_ok(mock_from_config):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    with pytest.raises(SodaException, match=f"Could not fetch the contract for dataset '{DATASET}'"):
        _verify().assert_ok()


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_on_a_managed_run_returns_the_result_without_raising_or_marking(mock_from_config, monkeypatch):
    # The Python API never reports to Soda Cloud; the CLI failure boundary does.
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    result = _verify()

    assert result.has_errors
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_through_the_deprecated_plural_api_returns_a_result_with_an_error(mock_from_config):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    with pytest.warns(DeprecationWarning):
        result = verify_contracts_locally(
            dataset_identifiers=[DATASET],
            data_source_file_paths=["ds.yaml"],
            soda_cloud_file_path="sc.yaml",
        )

    assert result.has_errors
    assert f"Could not fetch the contract for dataset '{DATASET}'" in result.get_errors_str()


@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_fetched_contract_is_verified_as_before(mock_from_config, mock_execute):
    mock_from_config.return_value.fetch_contract_for_dataset.return_value = CONTRACT_YAML
    session_result = MagicMock()
    mock_execute.return_value = session_result

    result = _verify()

    assert result is session_result
    mock_execute.assert_called_once()
    [contract_yaml_source] = mock_execute.call_args.kwargs["contract_yaml_sources"]
    assert contract_yaml_source.yaml_str == CONTRACT_YAML
    assert contract_yaml_source.file_path is None


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


@pytest.mark.parametrize(
    "fetch_side_effect, fetch_return_value",
    [(SodaCloudException("Soda Cloud is down"), None), (None, None), (None, "\n")],
    ids=["fetch-raises", "no-contract", "whitespace-contract"],
)
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_exits_3_when_the_contract_cannot_be_fetched(
    mock_from_config, fetch_side_effect, fetch_return_value, monkeypatch
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_side_effect
    mock_from_config.return_value.fetch_contract_for_dataset.return_value = fetch_return_value

    exit_code = handle_verify_contract(
        contract_file_path=None,
        dataset_identifier=DATASET,
        data_source_file_paths=["ds.yaml"],
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=False,
        verbose=False,
    )

    assert exit_code == ExitCode.LOG_ERRORS
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_on_a_managed_run_without_a_failure_channel_exits_4(mock_from_config, monkeypatch):
    # Outside the CLI bracket there is no channel to report through, so Soda Cloud cannot have the
    # failure: exit 4 hands it to the launcher's fallback instead of claiming it was delivered.
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    exit_code = handle_verify_contract(
        contract_file_path=None,
        dataset_identifier=DATASET,
        data_source_file_paths=["ds.yaml"],
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        verbose=False,
    )

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()


def _run_cli(monkeypatch, *extra_args: str) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        ["soda", "contract", "verify", "-d", DATASET, "-ds", "ds.yaml", "-sc", "sc.yaml", *extra_args],
    )
    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as exit_info:
        args.handler_func(args)
    return exit_info.value.code


@pytest.mark.parametrize("extra_args", [[], ["-r"], ["-p"]], ids=["local", "runner", "publish"])
@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
def test_cli_contract_verify_dataset_exits_3_when_the_fetch_fails(mock_from_config, extra_args, monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")

    exit_code = _run_cli(monkeypatch, *extra_args)

    assert exit_code == ExitCode.LOG_ERRORS
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()


@pytest.mark.parametrize(
    "marked, expected_exit_code",
    [(True, ExitCode.LOG_ERRORS), (False, ExitCode.RESULTS_NOT_SENT_TO_CLOUD)],
    ids=["marked", "mark-rejected"],
)
@pytest.mark.parametrize("extra_args", [[], ["-r"], ["-p"]], ids=["local", "runner", "publish"])
@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
def test_cli_contract_verify_dataset_on_a_managed_run_marks_the_scan_failed_when_the_fetch_fails(
    mock_from_config, extra_args, marked, expected_exit_code, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    soda_cloud = mock_from_config.return_value
    soda_cloud.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")
    soda_cloud.mark_scan_as_failed.return_value = marked

    exit_code = _run_cli(monkeypatch, *extra_args)

    assert exit_code == expected_exit_code
    soda_cloud.mark_scan_as_failed.assert_called_once()
    assert soda_cloud.mark_scan_as_failed.call_args.kwargs["scan_id"] == SCAN_ID
    reported_messages = [record.getMessage() for record in soda_cloud.mark_scan_as_failed.call_args.kwargs["logs"]]
    assert f"Could not fetch the contract for dataset '{DATASET}': Soda Cloud is down" in reported_messages
