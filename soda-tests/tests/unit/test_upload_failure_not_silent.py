"""
ADO-373: a publishing run whose YAML file upload is rejected by Soda Cloud must not
end silently.

Without a ``fileId`` the results cannot be sent, so the run used to log the upload
error, skip the send, and exit 0. On a runner that left the PENDING scan with no
results and no logs, and Cloud reported "The runner completed without reporting
scan results."

Now the failure is reported. With a runner scan id, the scan is marked FAILED with
the captured logs, so the upload error is visible in Cloud. Without a scan id, or
when Cloud rejects the mark, the result is flagged as not sent, so the CLI exits
with RESULTS_NOT_SENT_TO_CLOUD.
"""

from typing import Optional

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_core.contracts.impl.contract_verification_impl import ContractImpl

_CONTRACT_YAML = """
dataset: test_ds/main/my_table
columns:
  - name: id
checks:
  - row_count:
"""

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


def _rejected_upload(*args, **kwargs) -> Optional[str]:
    # Mirrors SodaCloud._upload_contract_yaml_file on a 400: it logs and returns None.
    soda_logger.critical(f"No fileId received in response: {_UPLOAD_ERROR}")
    return None


def _verify(data_source_impl: DataSourceImpl, mock_cloud: MockSodaCloud):
    mock_cloud._upload_contract_yaml_file = _rejected_upload
    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
        data_source_impls=[data_source_impl],
        soda_cloud_impl=mock_cloud,
        soda_cloud_publish_results=True,
    )
    return session_result.contract_verification_results[0]


def _requests_of_type(mock_cloud: MockSodaCloud, request_type: str) -> list[dict]:
    return [r.json for r in mock_cloud.requests if isinstance(r.json, dict) and r.json.get("type") == request_type]


@pytest.fixture(params=[False, True], ids=["per-file", "combined"])
def combine_uploads(request, monkeypatch) -> bool:
    # Data standards use the combined, session-level upload; contracts upload per file.
    monkeypatch.setattr(ContractImpl, "combine_uploads", request.param)
    return request.param


def test_rejected_upload_without_scan_id_flags_results_not_sent(monkeypatch, data_source_impl, combine_uploads):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_cloud = MockSodaCloud()

    result = _verify(data_source_impl, mock_cloud)

    assert result.check_results, "the checks themselves must still run"
    assert result.sending_results_to_soda_cloud_failed is True
    assert _requests_of_type(mock_cloud, "sodaCoreInsertScanResults") == []
    assert _requests_of_type(mock_cloud, "sodaCoreMarkScanFailed") == []


def test_rejected_upload_on_runner_marks_scan_failed_with_logs(monkeypatch, data_source_impl, combine_uploads):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=200, json_object={})])

    result = _verify(data_source_impl, mock_cloud)

    assert _requests_of_type(mock_cloud, "sodaCoreInsertScanResults") == []
    mark_requests = _requests_of_type(mock_cloud, "sodaCoreMarkScanFailed")
    assert len(mark_requests) == 1
    assert mark_requests[0]["scanId"] == "scan-under-test"
    assert _UPLOAD_ERROR in str(mark_requests[0]["logs"]), "the upload error must reach Cloud in the scan logs"
    # The failure is visible in Cloud; flagging it too would make the launcher mark the scan a second time.
    assert result.sending_results_to_soda_cloud_failed is False
    assert result.scan_id == "scan-under-test"


def test_rejected_upload_on_runner_with_rejected_mark_flags_results_not_sent(
    monkeypatch, data_source_impl, combine_uploads
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=500, json_object={})])

    result = _verify(data_source_impl, mock_cloud)

    assert len(_requests_of_type(mock_cloud, "sodaCoreMarkScanFailed")) == 1
    assert result.sending_results_to_soda_cloud_failed is True
