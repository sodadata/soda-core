"""
When a contract verification fails before producing any check results — e.g. the
data source connection fails — the run must be reported to Soda Cloud as a FAILED
scan (``sodaCoreMarkScanFailed``), not sent as results (``sodaCoreInsertScanResults``)
which the backend interprets as COMPLETED_WITH_ERRORS.

A managed/agent scan is still in PENDING at this point, and the Cloud state machine
forbids PENDING -> COMPLETED_WITH_ERRORS (only PENDING -> FAILED is allowed), so the
result upload 400s with ``invalid_scan_state`` -> exit code 4. Reporting FAILED keeps
the send valid.

Cloud marking has exactly two sites, and they never overlap:
- the send-results site substitutes a mark for the upload when the run *returned*
  an errored result without check results (the tests above the SAS-13001 section);
- the CLI failure boundary (``run_with_failure_reporting`` /
  ``report_scan_execution_failure``) marks for exceptions that *escape* the run
  (the SAS-13001 tests below). The engine layers underneath — the session's abort
  re-raise and ``verify_contract`` — must NOT mark: a second
  ``sodaCoreMarkScanFailed`` re-dispatches the backend's scan-ended events
  (duplicate failed-scan notifications) and re-promotes the scan logs.
"""

from unittest.mock import patch

import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.cli.handlers.dependencies import resolve_soda_cloud_for_failure_report, run_with_failure_reporting
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


def test_rejected_mark_surfaces_as_send_failure(monkeypatch):
    # When Soda Cloud rejects the mark request, the failure is invisible on Cloud, so
    # the result must be flagged sending_results_to_soda_cloud_failed: the exit code
    # then goes > 3 and the launcher fallback marks the scan failed itself.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))

    # The only HTTP request in this flow is the mark command; make it fail.
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=500, json_object={})])
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

    result = session_result.contract_verification_results[0]
    assert result.sending_results_to_soda_cloud_failed is True


def test_combine_uploads_path_rejected_mark_surfaces_as_send_failure(monkeypatch):
    # Same as above, on the session-level combined-upload path.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    monkeypatch.setattr(ContractImpl, "combine_uploads", True)

    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))

    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=500, json_object={})])
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

    result = session_result.contract_verification_results[0]
    assert result.sending_results_to_soda_cloud_failed is True


def _mark_requests(mock_cloud: MockSodaCloud) -> list[dict]:
    return [
        r.json
        for r in mock_cloud.requests
        if isinstance(r.json, dict) and r.json.get("type") == "sodaCoreMarkScanFailed"
    ]


def test_uncaught_exception_during_verify_reraises_without_marking_scan_failed(monkeypatch):
    """A single-contract (runner) scan that raises an *uncaught* exception during verify aborts
    and re-raises verbatim. The session must NOT mark the scan failed on the way out: the CLI
    failure boundary owns that mark, and a session-level mark would make it a duplicate
    (double backend scan-ended events). See the CLI-boundary tests below for where the mark
    (with the captured logs, SAS-13001) now happens."""
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

    assert not _mark_requests(mock_cloud), (
        "An exception escaping the session must leave the Cloud scan untouched — the CLI "
        "failure boundary owns the single mark-scan-failed."
    )


def test_uncaught_exception_during_construction_reraises_without_marking_scan_failed(monkeypatch):
    """The other abort pathway: an uncaught exception during contract *construction* (phase 1,
    before verify) likewise re-raises without a session-level mark."""
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

    assert not _mark_requests(mock_cloud), (
        "An exception escaping phase-1 construction must leave the Cloud scan untouched — the "
        "CLI failure boundary owns the single mark-scan-failed."
    )


def _handle_verify_contract_with_files(tmp_path, mock_cloud: MockSodaCloud, data_source_yaml: str = None) -> ExitCode:
    """Run the real CLI flow end-to-end (real session, real duckdb data source),
    with ``SodaCloud.from_config`` pinned to the given mock. Mirrors the cli.py
    verify wiring: channel resolution first, then the bare command wrapped in
    ``run_with_failure_reporting`` (the single Cloud-marking site)."""
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(_CONTRACT_YAML)
    data_source_path = tmp_path / "ds.yaml"
    data_source_path.write_text(data_source_yaml if data_source_yaml is not None else _DATA_SOURCE_YAML)

    with patch("soda_core.common.soda_cloud.SodaCloud.from_config", return_value=mock_cloud):
        soda_cloud = resolve_soda_cloud_for_failure_report("sc.yaml", {})
        return run_with_failure_reporting(
            soda_cloud,
            lambda logs: handle_verify_contract(
                contract_file_path=str(contract_path),
                dataset_identifier=None,
                data_source_file_paths=[str(data_source_path)],
                soda_cloud_file_path="sc.yaml",
                variables={},
                publish=True,
                verbose=False,
                use_runner=False,
                blocking_timeout_in_minutes=10,
                check_paths=None,
                check_selectors=[],
                diagnostics_warehouse_file_path=None,
                logs=logs,
            ),
        )


def test_cli_boundary_marks_scan_failed_exactly_once_with_engine_logs(monkeypatch, tmp_path):
    """SAS-13001, relocated: an uncaught verify exception reaches Cloud as exactly ONE
    mark-scan-failed — sent by the CLI failure boundary — carrying the captured engine logs.
    Exit code 3: the failure is visible in Cloud, the run is delivered."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    def _boom(self, *args, **kwargs):
        soda_logger.error("Boom: could not build check collection")
        raise RuntimeError("verify exploded")

    monkeypatch.setattr(ContractImpl, "verify", _boom)

    mock_cloud = MockSodaCloud()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    exit_code = _handle_verify_contract_with_files(tmp_path, mock_cloud)

    assert exit_code == ExitCode.LOG_ERRORS
    mark_requests = _mark_requests(mock_cloud)
    assert len(mark_requests) == 1, (
        f"Exactly one mark-scan-failed must reach Cloud; got {len(mark_requests)}. "
        "A duplicate re-dispatches the backend's scan-ended events (duplicate notifications)."
    )
    assert mark_requests[0].get("scanId") == "scan-under-test"
    # The captured engine logs must be shipped, not an empty payload — that is the whole point.
    payload = str(mark_requests[0].get("logs"))
    assert (
        "Boom: could not build check collection" in payload
    ), "mark-scan-failed must carry the captured engine logs, not an empty payload"


def test_cli_boundary_rejected_mark_exits_results_not_sent(monkeypatch, tmp_path):
    """When Cloud rejects the boundary's single mark, nothing reached Cloud: exit
    RESULTS_NOT_SENT_TO_CLOUD (4) so the launcher's fallback marks the scan failed itself."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    def _boom(self, *args, **kwargs):
        raise RuntimeError("verify exploded")

    monkeypatch.setattr(ContractImpl, "verify", _boom)

    # The only HTTP request in this flow is the boundary's mark command; make it fail.
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=500, json_object={})])
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    exit_code = _handle_verify_contract_with_files(tmp_path, mock_cloud)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert len(_mark_requests(mock_cloud)) == 1


def test_cli_boundary_construction_abort_marks_scan_failed_exactly_once_with_engine_logs(monkeypatch, tmp_path):
    """The construction-phase counterpart of the verify-abort E2E above: a phase-1
    construction exception escapes the session unmarked and reaches Cloud as exactly
    ONE boundary mark carrying the captured engine logs."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    def _boom_init(self, *args, **kwargs):
        soda_logger.error("Boom: could not construct contract")
        raise RuntimeError("construction exploded")

    monkeypatch.setattr(ContractImpl, "__init__", _boom_init)

    mock_cloud = MockSodaCloud()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    exit_code = _handle_verify_contract_with_files(tmp_path, mock_cloud)

    assert exit_code == ExitCode.LOG_ERRORS
    mark_requests = _mark_requests(mock_cloud)
    assert len(mark_requests) == 1
    assert mark_requests[0].get("scanId") == "scan-under-test"
    assert "Boom: could not construct contract" in str(mark_requests[0].get("logs"))


def test_cli_boundary_success_path_uploads_results_without_marking(monkeypatch, tmp_path):
    """The normal path through the boundary: a managed run that verifies cleanly
    uploads its results and never touches mark-scan-failed — exercising the shared
    Logs collector (wrapper -> session) on the success path."""
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")

    import duckdb

    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE my_table (id VARCHAR)")
    connection.execute("INSERT INTO my_table VALUES ('a')")
    connection.close()

    data_source_yaml = (
        "type: duckdb\n" "name: test_ds\n" "connection:\n" f'    database: "{db_path}"\n' "    schema: main\n"
    )

    # The insert response must carry the shared scanId — a 200 without it is
    # deliberately marked failed-to-send (exit 4).
    mock_cloud = MockSodaCloud(responses=[MockResponse(status_code=200, json_object={"scanId": "scan-under-test"})])
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    exit_code = _handle_verify_contract_with_files(tmp_path, mock_cloud, data_source_yaml=data_source_yaml)

    assert exit_code == ExitCode.OK
    assert _mark_requests(mock_cloud) == []
    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]
    assert "sodaCoreInsertScanResults" in request_types
