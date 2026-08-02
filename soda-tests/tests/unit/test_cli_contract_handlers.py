from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import (
    handle_publish_contract,
    handle_test_contract,
    handle_verify_contract,
)
from soda_core.common.logs import Logs
from soda_core.contracts.contract_publication import (
    ContractPublication,
    ContractPublicationResult,
    ContractPublicationResultList,
)
from soda_core.contracts.contract_verification import ContractVerificationSession


@pytest.mark.parametrize(
    "has_errors, has_failures, has_warnings, cloud_failed, expected_exit_code",
    [
        (False, False, False, False, ExitCode.OK),
        (False, False, True, False, ExitCode.CHECK_WARNINGS),
        (False, True, False, False, ExitCode.CHECK_FAILURES),
        (False, True, True, False, ExitCode.CHECK_FAILURES),
        (True, False, False, False, ExitCode.LOG_ERRORS),
        (True, True, False, False, ExitCode.LOG_ERRORS),
        (False, False, False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
        (True, True, False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
    ],
)
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
def test_handle_verify_contract_exit_codes(
    mock_execute, mock_cloud_client, has_errors, has_failures, has_warnings, cloud_failed, expected_exit_code
):
    mock_contract_result = MagicMock()
    mock_contract_result.sending_results_to_soda_cloud_failed = cloud_failed

    mock_result = MagicMock()
    type(mock_result).has_errors = PropertyMock(return_value=has_errors)
    type(mock_result).is_failed = PropertyMock(return_value=has_failures)
    type(mock_result).is_warned = PropertyMock(return_value=has_warnings)
    mock_result.contract_verification_results = [mock_contract_result]

    mock_execute.return_value = mock_result

    exit_code = handle_verify_contract(
        contract_file_path="contract.yaml",
        dataset_identifier=None,
        data_source_file_paths=["ds.yaml"],
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        verbose=False,
        use_runner=False,
        blocking_timeout_in_minutes=10,
        check_paths=None,
        check_selectors=[],
        diagnostics_warehouse_file_path=None,
    )

    assert exit_code == expected_exit_code


def _run_handle_verify_contract():
    return handle_verify_contract(
        contract_file_path="contract.yaml",
        dataset_identifier=None,
        data_source_file_paths=["ds.yaml"],
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        verbose=False,
        use_runner=True,
        blocking_timeout_in_minutes=10,
        check_paths=None,
        check_selectors=[],
        diagnostics_warehouse_file_path=None,
    )


# Exception-path exit codes: the mocked mark_scan_as_failed return value is the Cloud delivery
# signal that decides exit 3 vs 4. Patching SodaCloud.from_config at its source covers both the
# handler's report client and verify_contract's own.
@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
def test_handle_verify_contract_early_failure_undelivered_exits_results_not_sent(
    mock_execute, mock_from_config, monkeypatch
):
    """A managed scan whose early failure could NOT be reported to Cloud must exit
    RESULTS_NOT_SENT_TO_CLOUD (4) so the launcher marks the scan failed itself,
    instead of the old LOG_ERRORS (3) that made the job look succeeded."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_execute.side_effect = Exception("parse error: invalid YAML")
    mock_from_config.return_value.mark_scan_as_failed.return_value = False

    assert _run_handle_verify_contract() == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
def test_handle_verify_contract_early_failure_delivered_exits_log_errors(mock_execute, mock_from_config, monkeypatch):
    """A managed scan whose early failure WAS reported to Cloud stays LOG_ERRORS (3):
    Cloud already shows the failure with logs, so the job legitimately completed."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_execute.side_effect = Exception("parse error: invalid YAML")
    mock_from_config.return_value.mark_scan_as_failed.return_value = True

    assert _run_handle_verify_contract() == ExitCode.LOG_ERRORS


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
def test_handle_verify_contract_early_failure_adhoc_exits_log_errors(mock_execute, mock_from_config, monkeypatch):
    """An ad-hoc run (no SODA_SCAN_ID) has no Cloud scan to update, so an early
    failure exits LOG_ERRORS (3) regardless of Cloud delivery."""
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_execute.side_effect = Exception("parse error: invalid YAML")
    mock_from_config.return_value.mark_scan_as_failed.return_value = False

    assert _run_handle_verify_contract() == ExitCode.LOG_ERRORS


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
def test_handle_verify_contract_use_agent_kwarg_deprecated(mock_execute, mock_cloud_client):
    """Backwards-compat: the legacy ``use_agent`` kwarg still works and emits a DeprecationWarning."""
    mock_contract_result = MagicMock()
    mock_contract_result.sending_results_to_soda_cloud_failed = False
    mock_result = MagicMock()
    type(mock_result).has_errors = PropertyMock(return_value=False)
    type(mock_result).is_failed = PropertyMock(return_value=False)
    type(mock_result).is_warned = PropertyMock(return_value=False)
    mock_result.contract_verification_results = [mock_contract_result]
    mock_execute.return_value = mock_result

    with pytest.warns(DeprecationWarning, match="use_agent"):
        exit_code = handle_verify_contract(
            contract_file_path="contract.yaml",
            dataset_identifier=None,
            data_source_file_paths=["ds.yaml"],
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            verbose=False,
            use_agent=False,
            blocking_timeout_in_minutes=10,
            check_paths=None,
            check_selectors=[],
            diagnostics_warehouse_file_path=None,
        )

    assert exit_code == ExitCode.OK


@pytest.mark.parametrize(
    "has_errors, cloud_failed, expected_exit_code",
    [
        (False, False, ExitCode.OK),
        (True, False, ExitCode.LOG_ERRORS),
        # (False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),  # TODO: support exit code 4 detection
        # (True, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
    ],
)
@patch.object(ContractPublication, "builder")
def test_handle_publish_contract_exit_codes(mock_builder, has_errors, cloud_failed, expected_exit_code):
    mock_logs = MagicMock(spec=Logs)
    type(mock_logs).has_errors = PropertyMock(return_value=has_errors)

    mock_result_list = ContractPublicationResultList(
        items=[ContractPublicationResult(contract=None)],
        logs=mock_logs,
    )

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value.execute.return_value = mock_result_list
    mock_builder.return_value = mock_builder_instance

    exit_code = handle_publish_contract(
        contract_file_path="contract.yaml",
        soda_cloud_file_path="sc.yaml",
    )

    assert exit_code == expected_exit_code


@pytest.mark.parametrize(
    "has_errors, expected_exit_code",
    [
        (False, ExitCode.OK),
        (True, ExitCode.LOG_ERRORS),
    ],
)
@patch.object(ContractVerificationSession, "execute")
def test_handle_test_contract_exit_codes(mock_execute, has_errors, expected_exit_code):
    mock_result = MagicMock()
    type(mock_result).has_errors = PropertyMock(return_value=has_errors)

    mock_execute.return_value = mock_result

    exit_code = handle_test_contract(
        contract_file_path="contract.yaml",
        variables={},
    )

    assert exit_code == expected_exit_code
