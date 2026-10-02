"""
A publishing run whose YAML file upload is rejected by Soda Cloud must not end silently.

Without a ``fileId`` the results cannot be sent, so the run used to log the upload
error, skip the send, and exit 0. On a runner that left the PENDING scan with no
results and no logs, and Cloud reported "The runner completed without reporting
scan results."

Now the failure is reported. With a runner scan id, the scan is marked FAILED with
the captured logs and the run exits LOG_ERRORS, the code for a failure that reached
Cloud. Without a scan id, or when Cloud rejects the mark, the result is flagged as
not sent, so the run exits RESULTS_NOT_SENT_TO_CLOUD.
"""

from typing import Optional

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.check_collections.session import execute_check_collections
from soda_core.cli.exit_codes import ExitCode, session_result_to_exit_code
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession, ContractVerificationSessionResult
from soda_core.contracts.impl.contract_verification_impl import ContractImpl

_CONTRACT_YAML = """
dataset: test_ds/main/my_table
columns:
  - name: id
checks:
  - row_count:
"""

_REJECTED_MARKER = "# rejected by Soda Cloud"

_UPLOAD_ERROR = "Schema validation failed: required property 'columns' not found"


@pytest.fixture
def data_source_impl(tmp_path) -> DataSourceImpl:
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE my_table (id VARCHAR)")
    connection.execute("INSERT INTO my_table VALUES ('a')")
    connection.close()
    data_source_yaml = (
        "type: duckdb\n" "name: test_ds\n" "connection:\n" f'    database: "{db_path}"\n' "    schema: main\n"
    )
    return DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(data_source_yaml))


def _upload(contract_yaml: str, *args, **kwargs) -> Optional[str]:
    if _REJECTED_MARKER not in contract_yaml:
        return "contract-file-id"
    # Mirrors SodaCloud._upload_contract_yaml_file on a 400: it logs and returns None.
    soda_logger.critical(f"No fileId received in response: {_UPLOAD_ERROR}")
    return None


def _verify(
    data_source_impl: DataSourceImpl, mock_cloud: MockSodaCloud, contract_yamls: Optional[list[str]] = None
) -> ContractVerificationSessionResult:
    mock_cloud._upload_contract_yaml_file = _upload
    mark_calls: list[Optional[str]] = []
    mark_scan_as_failed = mock_cloud.mark_scan_as_failed

    def recording_mark_scan_as_failed(*args, **kwargs):
        mark_calls.append(kwargs.get("scan_id"))
        return mark_scan_as_failed(*args, **kwargs)

    mock_cloud.mark_scan_as_failed = recording_mark_scan_as_failed
    mock_cloud.mark_calls = mark_calls
    return ContractVerificationSession.execute(
        contract_yaml_sources=[
            ContractYamlSource.from_str(contract_yaml)
            for contract_yaml in (contract_yamls or [_CONTRACT_YAML + _REJECTED_MARKER])
        ],
        data_source_impls=[data_source_impl],
        soda_cloud_impl=mock_cloud,
        soda_cloud_publish_results=True,
    )


def _requests_of_type(mock_cloud: MockSodaCloud, request_type: str) -> list[dict]:
    return [r.json for r in mock_cloud.requests if isinstance(r.json, dict) and r.json.get("type") == request_type]


@pytest.fixture(params=[False, True], ids=["per-file", "combined"])
def combine_uploads(request, monkeypatch) -> bool:
    # Data standards use the combined, session-level upload; contracts upload per file.
    monkeypatch.setattr(ContractImpl, "combine_uploads", request.param)
    return request.param


def test_rejected_upload_without_scan_id_exits_results_not_sent(monkeypatch, data_source_impl, combine_uploads):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_cloud = MockSodaCloud()

    session_result = _verify(data_source_impl, mock_cloud)
    result = session_result.contract_verification_results[0]

    assert result.check_results, "the checks themselves must still run"
    assert result.sending_results_to_soda_cloud_failed is True
    assert mock_cloud.mark_calls == [], "without a scan id there is no scan to mark"
    assert _requests_of_type(mock_cloud, "sodaCoreInsertScanResults") == []
    assert interpret_contract_verification_result(session_result) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_rejected_upload_on_runner_marks_scan_failed_and_exits_log_errors(
    monkeypatch, data_source_impl, combine_uploads
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=200, json_object={})])

    session_result = _verify(data_source_impl, mock_cloud)
    result = session_result.contract_verification_results[0]

    assert _requests_of_type(mock_cloud, "sodaCoreInsertScanResults") == []
    mark_requests = _requests_of_type(mock_cloud, "sodaCoreMarkScanFailed")
    assert len(mark_requests) == 1
    assert mark_requests[0]["scanId"] == "scan-under-test"
    assert _UPLOAD_ERROR in str(mark_requests[0]["logs"]), "the upload error must reach Cloud in the scan logs"
    # The failure is visible in Cloud; flagging it too would make the launcher mark the scan a second time.
    assert result.sending_results_to_soda_cloud_failed is False
    assert result.scan_id == "scan-under-test"
    assert interpret_contract_verification_result(session_result) == ExitCode.LOG_ERRORS


def test_rejected_upload_on_runner_with_rejected_mark_exits_results_not_sent(
    monkeypatch, data_source_impl, combine_uploads
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=500, json_object={})])

    session_result = _verify(data_source_impl, mock_cloud)

    assert len(_requests_of_type(mock_cloud, "sodaCoreMarkScanFailed")) == 1
    assert session_result.contract_verification_results[0].sending_results_to_soda_cloud_failed is True
    assert interpret_contract_verification_result(session_result) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def _verify_combined_session(data_source_impl: DataSourceImpl, mock_cloud: MockSodaCloud, contract_yamls: list[str]):
    # Drives the executor the way verify_data_standards does: no shared Logs, so each file
    # keeps its own log records and status.
    mock_cloud._upload_contract_yaml_file = _upload
    mark_calls: list[Optional[str]] = []
    mark_scan_as_failed = mock_cloud.mark_scan_as_failed

    def recording_mark_scan_as_failed(*args, **kwargs):
        mark_calls.append(kwargs.get("scan_id"))
        return mark_scan_as_failed(*args, **kwargs)

    mock_cloud.mark_scan_as_failed = recording_mark_scan_as_failed
    mock_cloud.mark_calls = mark_calls
    return execute_check_collections(
        yaml_sources=[ContractYamlSource.from_str(contract_yaml) for contract_yaml in contract_yamls],
        data_source_impl=None,
        soda_cloud_impl=mock_cloud,
        publish_results=True,
        all_data_source_impls={data_source_impl.name: data_source_impl},
        default_impl_class=ContractImpl,
    )


def test_combined_session_with_two_rejected_uploads_marks_once_and_exits_log_errors(monkeypatch, data_source_impl):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    monkeypatch.setattr(ContractImpl, "combine_uploads", True)
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=200, json_object={})])

    session_result = _verify_combined_session(
        data_source_impl,
        mock_cloud,
        [_CONTRACT_YAML + _REJECTED_MARKER, _CONTRACT_YAML + _REJECTED_MARKER + " too"],
    )

    assert mock_cloud.mark_calls == ["scan-under-test"]
    assert [r.sending_results_to_soda_cloud_failed for r in session_result.results] == [False, False]
    assert session_result_to_exit_code(session_result) == ExitCode.LOG_ERRORS


def test_combined_session_does_not_mark_over_an_uploaded_sibling(monkeypatch, data_source_impl):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    monkeypatch.setattr(ContractImpl, "combine_uploads", True)
    mock_cloud = MockSodaCloud()

    session_result = _verify_combined_session(
        data_source_impl, mock_cloud, [_CONTRACT_YAML + _REJECTED_MARKER, _CONTRACT_YAML]
    )

    assert mock_cloud.mark_calls == [], "the sibling's results reached Cloud, so the scan must not be marked FAILED"
    assert len(_requests_of_type(mock_cloud, "sodaCoreInsertScanResults")) == 1
    assert session_result.results[0].sending_results_to_soda_cloud_failed is True
    assert session_result_to_exit_code(session_result) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
