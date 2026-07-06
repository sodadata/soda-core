"""
When a contract verification fails before producing any check results — e.g. the
data source connection fails — the run must be reported to Soda Cloud as a FAILED
scan (``sodaCoreMarkScanFailed``), not sent as results (``sodaCoreInsertScanResults``)
which the backend interprets as COMPLETED_WITH_ERRORS.

A managed/agent scan is still in PENDING at this point, and the Cloud state machine
forbids PENDING -> COMPLETED_WITH_ERRORS (only PENDING -> FAILED is allowed), so the
result upload 400s with ``invalid_scan_state`` -> exit code 4. Reporting FAILED keeps
the send valid.
"""

from unittest.mock import patch

import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_core.contracts.impl.contract_verification_impl import ContractImpl

_DATA_SOURCE_YAML = """
type: duckdb
name: test_ds
connection:
    database: ":memory:"
    schema: main
"""

_CONTRACT_YAML = """
dataset: test_ds/main/my_table
columns:
  - name: id
"""


def test_connection_failure_marks_scan_failed_not_completed_with_errors(monkeypatch):
    # A runner-created scan id is the precondition for reporting FAILED; mark_scan_as_failed
    # uses it to transition the (still PENDING) scan to FAILED.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))

    mock_cloud = MockSodaCloud()
    # The contract YAML file always uploads first; pin its result so the send/mark
    # decision is the only thing under test (independent of response ordering).
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    with patch(
        "soda_duckdb.common.data_sources.duckdb_data_source.DuckDBDataSourceConnection._create_connection",
        side_effect=RuntimeError("Invalid access token"),
    ):
        session_result = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
            data_source_impls=[data_source_impl],
            soda_cloud_impl=mock_cloud,
            soda_cloud_publish_results=True,
        )

    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]

    assert "sodaCoreInsertScanResults" not in request_types, (
        "A scan that errored before producing any check results must NOT be sent as results "
        f"(COMPLETED_WITH_ERRORS). Requests seen: {request_types}"
    )
    mark_requests = [
        r.json
        for r in mock_cloud.requests
        if isinstance(r.json, dict) and r.json.get("type") == "sodaCoreMarkScanFailed"
    ]
    assert (
        mark_requests
    ), f"A scan that errored before producing any check results must be marked FAILED. {request_types}"
    # The known scan id is passed to the mark request and stamped on the result.
    assert mark_requests[0].get("scanId") == "scan-under-test"
    assert session_result.contract_verification_results[0].scan_id == "scan-under-test"


def test_combine_uploads_path_marks_scan_failed_on_connection_error(monkeypatch):
    # Data Standards is the combine_uploads=True subtype; its results are sent via the
    # session-level combined-upload path, not the per-file verify() path. Force combine
    # mode on the contract impl to exercise that path with the same connection failure.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    monkeypatch.setattr(ContractImpl, "combine_uploads", True)

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))

    mock_cloud = MockSodaCloud()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    with patch(
        "soda_duckdb.common.data_sources.duckdb_data_source.DuckDBDataSourceConnection._create_connection",
        side_effect=RuntimeError("Invalid access token"),
    ):
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
            data_source_impls=[data_source_impl],
            soda_cloud_impl=mock_cloud,
            soda_cloud_publish_results=True,
        )

    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]

    assert "sodaCoreInsertScanResults" not in request_types, (
        "Combine-uploads path must not send an errored-before-results scan as results. "
        f"Requests seen: {request_types}"
    )
    mark_requests = [
        r.json
        for r in mock_cloud.requests
        if isinstance(r.json, dict) and r.json.get("type") == "sodaCoreMarkScanFailed"
    ]
    assert mark_requests, f"Combine-uploads path must mark the scan FAILED. Requests seen: {request_types}"
    assert mark_requests[0].get("scanId") == "scan-under-test"


def test_ad_hoc_run_without_scan_id_still_uploads_results(monkeypatch):
    # Ad-hoc CLI runs have no pre-created PENDING scan and no SODA_SCAN_ID, so they don't hit
    # the invalid_scan_state transition. mark_scan_as_failed would be a no-op (losing the
    # errored scan in Cloud), so the engine must keep uploading results to create the scan.
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))

    mock_cloud = MockSodaCloud()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    with patch(
        "soda_duckdb.common.data_sources.duckdb_data_source.DuckDBDataSourceConnection._create_connection",
        side_effect=RuntimeError("Invalid access token"),
    ):
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
            data_source_impls=[data_source_impl],
            soda_cloud_impl=mock_cloud,
            soda_cloud_publish_results=True,
        )

    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]

    assert "sodaCoreInsertScanResults" in request_types, (
        "Ad-hoc run (no SODA_SCAN_ID) must still upload results to create the scan in Cloud. "
        f"Requests seen: {request_types}"
    )
    assert "sodaCoreMarkScanFailed" not in request_types, (
        "Ad-hoc run has no scan id to mark failed; mark_scan_as_failed must not be used. "
        f"Requests seen: {request_types}"
    )


def test_uncaught_exception_during_verify_marks_scan_failed_with_logs(monkeypatch):
    """A single-contract (runner) scan that raises an *uncaught* exception during verify aborts
    and re-raises before phase 3's combined upload runs. The engine must still report the runner
    scan as FAILED with the captured logs, otherwise the Cloud scan record shows no logs and the
    failure is undiagnosable (SAS-13001)."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    def _boom(self, *args, **kwargs):
        soda_logger.error("Boom: could not build check collection")
        raise RuntimeError("verify exploded")

    monkeypatch.setattr(ContractImpl, "verify", _boom)

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))
    mock_cloud = MockSodaCloud()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    # The abort-on-first-error contract path re-raises the exception verbatim.
    with pytest.raises(RuntimeError, match="verify exploded"):
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
            data_source_impls=[data_source_impl],
            soda_cloud_impl=mock_cloud,
            soda_cloud_publish_results=True,
        )

    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]
    mark_requests = [
        r.json
        for r in mock_cloud.requests
        if isinstance(r.json, dict) and r.json.get("type") == "sodaCoreMarkScanFailed"
    ]
    assert mark_requests, (
        "A scan that raised an uncaught exception during verify must be marked FAILED so the "
        f"failure is visible in Cloud. Requests seen: {request_types}"
    )
    assert mark_requests[0].get("scanId") == "scan-under-test"
    # The captured engine logs must be shipped, not an empty payload — that is the whole point.
    assert mark_requests[0].get("logs"), "mark-scan-failed must carry the captured engine logs, not an empty payload"


def test_uncaught_exception_during_construction_marks_scan_failed_with_logs(monkeypatch):
    """The other abort pathway: an uncaught exception during contract *construction* (phase 1, before
    verify) must likewise mark the runner scan FAILED with the captured logs before re-raising. Both
    the construction and verify abort points call the same reporting helper (SAS-13001)."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    def _boom_init(self, *args, **kwargs):
        soda_logger.error("Boom: could not construct contract")
        raise RuntimeError("construction exploded")

    # Fail inside ContractImpl.__init__ so the exception escapes phase-1 construction, not verify().
    monkeypatch.setattr(ContractImpl, "__init__", _boom_init)

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))
    mock_cloud = MockSodaCloud()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    with pytest.raises(RuntimeError, match="construction exploded"):
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
            data_source_impls=[data_source_impl],
            soda_cloud_impl=mock_cloud,
            soda_cloud_publish_results=True,
        )

    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]
    mark_requests = [
        r.json
        for r in mock_cloud.requests
        if isinstance(r.json, dict) and r.json.get("type") == "sodaCoreMarkScanFailed"
    ]
    assert mark_requests, (
        "A scan that raised an uncaught exception during construction must be marked FAILED. "
        f"Requests seen: {request_types}"
    )
    assert mark_requests[0].get("scanId") == "scan-under-test"
    assert mark_requests[0].get("logs"), "mark-scan-failed must carry the captured engine logs, not an empty payload"
