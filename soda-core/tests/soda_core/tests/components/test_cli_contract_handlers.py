import pytest

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import (
    handle_verify_contract, handle_publish_contract, handle_test_contract, ContractVerificationSession
)
from soda_core.common.logs import Logs
from soda_core.contracts.contract_publication import (
    ContractPublication, ContractPublicationResultList, ContractPublicationResult
)
from unittest.mock import patch, MagicMock


@pytest.mark.parametrize("has_errors, has_failures, cloud_failed, expected_exit_code", [
    (False, False, False, ExitCode.OK),
    (False, True, False, ExitCode.CHECK_FAILURES),
    (True, False, False, ExitCode.LOG_ERRORS),
    (True, True, False, ExitCode.LOG_ERRORS),
    (False, False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
    (True, True, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
])
@patch.object(ContractVerificationSession, "execute")
def test_handle_verify_contract_exit_codes(mock_execute, has_errors, has_failures, cloud_failed, expected_exit_code):
    mock_contract_result = MagicMock()
    mock_contract_result.sending_results_to_soda_cloud_failed = cloud_failed

    mock_result = MagicMock()
    mock_result.has_errors.return_value = has_errors
    mock_result.is_failed.return_value = has_failures
    mock_result.contract_verification_results = [mock_contract_result]

    mock_execute.return_value = mock_result

    exit_code = handle_verify_contract(
        contract_file_paths=["contract.yaml"],
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        publish=True,
        use_agent=False,
        blocking_timeout_in_minutes=10,
    )

    assert exit_code == expected_exit_code


@pytest.mark.parametrize("has_errors, cloud_failed, expected_exit_code", [
    (False, False, ExitCode.OK),
    (True, False, ExitCode.LOG_ERRORS),
    # (False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),  # TODO: support exit code 4 detection
    # (True, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
])
@patch.object(ContractPublication, "builder")
def test_handle_publish_contract_exit_codes(mock_builder, has_errors, cloud_failed, expected_exit_code):
    mock_logs = MagicMock(spec=Logs)
    mock_logs.has_errors.return_value = has_errors

    mock_result_list = ContractPublicationResultList(
        items=[ContractPublicationResult(contract=None)],
        logs=mock_logs,
    )

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value.execute.return_value = mock_result_list
    mock_builder.return_value = mock_builder_instance

    exit_code = handle_publish_contract(
        contract_file_paths=["contract.yaml"],
        soda_cloud_file_path="sc.yaml",
    )

    assert exit_code == expected_exit_code


@pytest.mark.parametrize("has_errors, expected_exit_code", [
    (False, ExitCode.OK),
    (True, ExitCode.LOG_ERRORS),
])
@patch.object(ContractVerificationSession, "execute")
def test_handle_test_contract_exit_codes(mock_execute, has_errors, expected_exit_code):
    mock_result = MagicMock()
    mock_result.has_errors.return_value = has_errors

    mock_execute.return_value = mock_result

    exit_code = handle_test_contract(
        contract_file_paths=["contract.yaml"],
        data_source_file_path="ds.yaml"
    )

    assert exit_code == expected_exit_code
