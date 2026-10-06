import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from helpers.mock_soda_cloud import MockHttpMethod, MockResponse, MockSodaCloud
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_publish_contract, handle_test_contract, handle_verify_contract
from soda_core.cli.handlers.dependencies import resolve_soda_cloud_for_failure_report
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
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
    # Mirrors the cli.py verify wiring: resolve the reporting channel first, then wrap
    # the bare command in run_scan (the single Cloud-marking site).
    soda_cloud = resolve_soda_cloud_for_failure_report("sc.yaml", {})
    return run_scan(
        soda_cloud,
        lambda logs: handle_verify_contract(
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
            logs=logs,
        ),
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


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.contracts.api.verify_api.ContractVerificationSession.execute")
def test_handle_verify_contract_early_failure_marks_scan_failed_exactly_once_with_logs(
    mock_execute, mock_from_config, monkeypatch
):
    """The CLI failure boundary is the single Cloud-marking site for escaped exceptions.
    A second sodaCoreMarkScanFailed (historically sent from the session's pre-reraise
    hook and verify_contract's except arm) re-dispatches the backend's scan-ended
    events — duplicate failed-scan notifications — and re-promotes the scan logs."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_execute.side_effect = Exception("parse error: invalid YAML")
    mock_cloud = mock_from_config.return_value
    mock_cloud.mark_scan_as_failed.return_value = True

    assert _run_handle_verify_contract() == ExitCode.LOG_ERRORS

    assert mock_cloud.mark_scan_as_failed.call_count == 1
    _, kwargs = mock_cloud.mark_scan_as_failed.call_args
    assert kwargs.get("logs"), "the single mark must carry the captured log records"


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
def test_handle_verify_contract_broken_cloud_config_managed_exits_results_not_sent(mock_from_config, monkeypatch):
    """A managed run whose Cloud config can't even build a client has no reporting
    channel at all (resolve_soda_cloud_for_failure_report swallows to None): nothing
    can reach Cloud, so exit RESULTS_NOT_SENT_TO_CLOUD (4) and the launcher's
    fallback marks the scan failed. The real config error still surfaces through the
    boundary because verify_contract re-raises it building its own client."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_from_config.side_effect = Exception("broken cloud config")

    assert _run_handle_verify_contract() == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


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


def _publish_contract_file(tmp_path, contract_yaml_str: str) -> tuple[ExitCode, MockSodaCloud, str]:
    contract_file_path = str(tmp_path / "contract.yml")
    with open(contract_file_path, "w") as contract_file:
        contract_file.write(contract_yaml_str)
    soda_cloud_file_path = str(tmp_path / "sc.yml")
    with open(soda_cloud_file_path, "w") as soda_cloud_file:
        soda_cloud_file.write("soda_cloud:\n  api_key_id: id\n  api_key_secret: secret\n")

    mock_cloud = MockSodaCloud(
        [
            MockResponse(method=MockHttpMethod.POST, json_object={"allowed": True}),
            MockResponse(method=MockHttpMethod.POST, json_object={"fileId": "fake_file_id"}),
            MockResponse(
                method=MockHttpMethod.POST,
                json_object={"publishedContract": {}, "metadata": {"source": {"filePath": contract_file_path}}},
            ),
        ]
    )
    with patch.object(SodaCloud, "from_yaml_source", return_value=mock_cloud):
        exit_code = handle_publish_contract(contract_file_path, soda_cloud_file_path)
    return exit_code, mock_cloud, contract_file_path


@pytest.mark.parametrize(
    "contract_yaml_str",
    [
        pytest.param("dataset: ds/db/sch/CUSTOMERS\nfilter: [id, name]\ncolumns:\n  - name: id\n", id="yaml_error"),
        pytest.param(
            "dataset: ds/db/sch/CUSTOMERS\ncolumns:\n  - name: id\n  - name: id\n", id="contract_validation_error"
        ),
        pytest.param("dataset: ds/db/sch/CUSTOMERS\ncolumns: [\n", id="yaml_syntax_error"),
        pytest.param(
            "dataset: ds/db/sch/CUSTOMERS\ncolumns:\n  - name: id\nchecks:\n  - not_a_check:\n",
            id="invalid_check_type",
        ),
        pytest.param(
            'dataset: ds/db/sch/CUSTOMERS\nfilter: "id > ${var.UNDECLARED}"\ncolumns:\n  - name: id\n',
            id="undeclared_variable",
        ),
    ],
)
def test_handle_publish_contract_uploads_nothing_when_the_contract_has_errors(tmp_path, contract_yaml_str):
    exit_code, mock_cloud, _ = _publish_contract_file(tmp_path, contract_yaml_str)

    assert exit_code == ExitCode.LOG_ERRORS
    assert mock_cloud.requests == []


def test_handle_publish_contract_uploads_a_valid_contract(tmp_path):
    contract_yaml_str = "dataset: ds/db/sch/CUSTOMERS\ncolumns:\n  - name: id\n"

    exit_code, mock_cloud, contract_file_path = _publish_contract_file(tmp_path, contract_yaml_str)

    assert exit_code == ExitCode.OK
    assert [request.json["type"] for request in mock_cloud.requests] == [
        "sodaCoreCanManageContracts",
        "sodaCoreUploadContractFile",
        "sodaCorePublishContract",
    ]
    assert mock_cloud.requests[1].json["contents"] == contract_yaml_str
    assert mock_cloud.requests[2].json["contract"] == {
        "fileId": "fake_file_id",
        "metadata": {"source": {"type": "local", "filePath": contract_file_path}},
    }


REQUIRED_VARIABLE_CONTRACT_YAML = (
    "dataset: ds/db/sch/CUSTOMERS\nvariables:\n  QUERY:\ncolumns:\n  - name: id\nchecks:\n"
    "  - metric:\n      query: ${var.QUERY}\n      threshold:\n        must_be_greater_than: 0\n"
)


def test_handle_publish_contract_uploads_a_contract_with_a_variable_without_value(tmp_path, caplog):
    exit_code, mock_cloud, _ = _publish_contract_file(tmp_path, REQUIRED_VARIABLE_CONTRACT_YAML)

    assert exit_code == ExitCode.OK
    assert [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR] == []
    assert [request.json["type"] for request in mock_cloud.requests] == [
        "sodaCoreCanManageContracts",
        "sodaCoreUploadContractFile",
        "sodaCorePublishContract",
    ]
    assert mock_cloud.requests[1].json["contents"] == REQUIRED_VARIABLE_CONTRACT_YAML


def test_handle_test_contract_still_reports_a_variable_without_value(tmp_path, caplog):
    contract_file_path = str(tmp_path / "contract.yml")
    with open(contract_file_path, "w") as contract_file:
        contract_file.write(REQUIRED_VARIABLE_CONTRACT_YAML)

    exit_code = handle_test_contract(contract_file_path=contract_file_path, variables={})

    assert exit_code == ExitCode.LOG_ERRORS
    assert "Required variable 'QUERY' did not get a value" in [
        record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
    ]


THRESHOLD_VARIABLE_CONTRACT_YAML = (
    "dataset: ds/db/sch/CUSTOMERS\nvariables:\n  MAX:\ncolumns:\n  - name: id\nchecks:\n"
    "  - row_count:\n      threshold:\n        must_be_less_than: ${var.MAX}\n"
)


def test_handle_publish_contract_uploads_a_contract_with_a_threshold_variable_without_value(tmp_path, caplog):
    exit_code, mock_cloud, _ = _publish_contract_file(tmp_path, THRESHOLD_VARIABLE_CONTRACT_YAML)

    assert [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR] == []
    assert exit_code == ExitCode.OK
    assert [request.json["type"] for request in mock_cloud.requests] == [
        "sodaCoreCanManageContracts",
        "sodaCoreUploadContractFile",
        "sodaCorePublishContract",
    ]
    assert mock_cloud.requests[1].json["contents"] == THRESHOLD_VARIABLE_CONTRACT_YAML


def test_handle_test_contract_still_reports_a_threshold_variable_without_value(tmp_path, caplog):
    contract_file_path = str(tmp_path / "contract.yml")
    with open(contract_file_path, "w") as contract_file:
        contract_file.write(THRESHOLD_VARIABLE_CONTRACT_YAML)

    exit_code = handle_test_contract(contract_file_path=contract_file_path, variables={})

    assert exit_code == ExitCode.LOG_ERRORS
    assert [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR] == [
        "Required variable 'MAX' did not get a value",
    ]


def test_handle_test_contract_accepts_a_threshold_variable_with_a_number_value(tmp_path, caplog):
    contract_file_path = str(tmp_path / "contract.yml")
    with open(contract_file_path, "w") as contract_file:
        contract_file.write(THRESHOLD_VARIABLE_CONTRACT_YAML)

    exit_code = handle_test_contract(contract_file_path=contract_file_path, variables={"MAX": 5})

    assert [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR] == []
    assert exit_code == ExitCode.OK


def test_handle_test_contract_still_reports_an_additional_threshold_without_its_own_comparison(tmp_path, caplog):
    contract_file_path = str(tmp_path / "contract.yml")
    with open(contract_file_path, "w") as contract_file:
        contract_file.write(
            THRESHOLD_VARIABLE_CONTRACT_YAML + "        additional:\n          must_be_less_than: 1000\n"
            "          level: warn\n"
        )

    exit_code = handle_test_contract(contract_file_path=contract_file_path, variables={})

    assert exit_code == ExitCode.LOG_ERRORS
    error_messages = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert "Required variable 'MAX' did not get a value" in error_messages
    assert any("'additional' threshold" in message for message in error_messages)


def test_handle_publish_contract_logs_a_yaml_syntax_error_in_one_line(tmp_path, caplog):
    exit_code, mock_cloud, contract_file_path = _publish_contract_file(
        tmp_path, "dataset: ds/db/sch/CUSTOMERS\ncolumns: [\n"
    )

    assert exit_code == ExitCode.LOG_ERRORS
    assert mock_cloud.requests == []
    [error_record] = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert "YAML syntax error" in error_record.getMessage()
    assert contract_file_path in error_record.getMessage()
    assert error_record.exc_info is None


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
