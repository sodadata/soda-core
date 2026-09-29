"""Contract verification with -d/--dataset when Soda Cloud cannot hand over the contract.

A failed fetch, or a fetch that returns no contract, fails the run. The Python API returns a
result that has errors instead of raising, and the CLI maps that result to exit code 3.
"""

import sys
from logging import ERROR
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import ContractNotFoundException, DatasetNotFoundException, SodaCloudException
from soda_core.contracts.api.verify_api import verify_contract, verify_contracts_locally
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    ContractVerificationSessionResult,
    SodaException,
)

DATASET = "my_data_source/my_db/my_schema/customers"
CONTRACT_YAML = f"dataset: {DATASET}\nchecks:\n  - row_count:\n"


def _fetch_exceptions() -> list:
    parsed = DatasetIdentifier.parse(DATASET)
    return [
        SodaCloudException("Soda Cloud is down"),
        ContractNotFoundException(parsed),
        DatasetNotFoundException(parsed),
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
@pytest.mark.parametrize("fetch_exception", _fetch_exceptions(), ids=lambda exc: type(exc).__name__)
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_failed_fetch_returns_a_result_with_an_error_naming_the_dataset(
    mock_from_config, mock_execute, fetch_exception, use_runner
):
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_exception

    result = _verify(use_runner=use_runner)

    assert result.get_errors() == [f"Could not fetch the contract for dataset '{DATASET}': {fetch_exception}"]
    assert result.has_errors
    assert not result.is_ok
    assert not result.is_passed
    mock_execute.assert_not_called()


@pytest.mark.parametrize("use_runner", [False, True], ids=["local", "runner"])
@pytest.mark.parametrize("fetched_contract", [None, ""], ids=["none", "empty"])
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
def test_failed_fetch_result_names_the_dataset_and_keeps_the_exception(mock_from_config):
    fetch_exception = SodaCloudException("Soda Cloud is down")
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = fetch_exception

    result = _verify()

    [contract_result] = result.contract_verification_results
    assert contract_result.status is CheckCollectionStatus.ERROR
    assert contract_result.check_collection.soda_qualified_dataset_name == DATASET
    assert contract_result.error is fetch_exception
    assert contract_result.check_results == []
    assert not contract_result.sending_results_to_soda_cloud_failed


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
    [(SodaCloudException("Soda Cloud is down"), None), (None, None)],
    ids=["fetch-raises", "no-contract"],
)
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_exits_3_when_the_contract_cannot_be_fetched(
    mock_from_config, fetch_side_effect, fetch_return_value
):
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


@pytest.mark.parametrize("extra_args", [[], ["-r"], ["-p"]], ids=["local", "runner", "publish"])
@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
def test_cli_contract_verify_dataset_exits_3_when_the_fetch_fails(mock_from_config, extra_args, monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_from_config.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("Soda Cloud is down")
    monkeypatch.setattr(
        sys,
        "argv",
        ["soda", "contract", "verify", "-d", DATASET, "-ds", "ds.yaml", "-sc", "sc.yaml", *extra_args],
    )

    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as exit_info:
        args.handler_func(args)

    assert exit_info.value.code == ExitCode.LOG_ERRORS
    mock_from_config.return_value.mark_scan_as_failed.assert_not_called()
