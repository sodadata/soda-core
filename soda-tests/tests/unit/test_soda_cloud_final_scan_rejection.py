"""Soda Cloud refusing a report because the scan is finished or gone.

A managed run reports to a scan Soda Cloud created up front. That scan can
reach a terminal state while the run is still going — the user cancels it, or
the scan definition is deleted underneath it. Cloud then refuses the report
with ``invalid_scan_state`` or ``scan_not_found``.

Nothing is lost when that happens: there is no longer anyone waiting for the
results. Treating it as a delivery failure exits 4, which makes the contract
launcher raise and the runner pod die on a scan the user themselves cancelled.
These tests hold that apart from a real delivery failure, where exit 4 is
exactly right.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.common.logs import Location
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.soda_cloud_dto import ReportOutcome
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    ContractVerificationSessionResult,
    DataSource,
    YamlFileContentInfo,
)

SCAN_GONE_CODES = ["invalid_scan_state", "scan_not_found"]


def _cloud_answering(status_code: int, body: dict) -> SodaCloud:
    cloud = SodaCloud.__new__(SodaCloud)  # bypass __init__; only _execute_command is used
    response = MagicMock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 300
    response.json.return_value = body
    cloud._execute_command = MagicMock(return_value=response)
    return cloud


def _check_result(outcome: CheckOutcome, path: str) -> CheckResult:
    return CheckResult(
        check=Check(
            column_name="id",
            type="row_count",
            qualifier=None,
            name=path,
            relative_path=path,
            check_path=path,
            identity=path,
            definition="row_count: ...",
            contract_file_line=1,
            contract_file_column=1,
            threshold=None,
            attributes={},
            location=Location(file_path="fake.yml", line=1, column=1),
        ),
        outcome=outcome,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )


def _make_result(
    *,
    label: str = "alpha",
    status: CheckCollectionStatus = CheckCollectionStatus.PASSED,
    check_results=None,
) -> ContractVerificationResult:
    started = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ended = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    return ContractVerificationResult(
        check_collection=Contract(
            data_source_name="test_ds",
            dataset_prefix=["s"],
            dataset_name="t",
            soda_qualified_dataset_name="test_ds/s/t",
            source=YamlFileContentInfo(
                source_content_str=f"# {label}",
                local_file_path=f"/fake/{label}.yml",
                soda_cloud_file_id=f"file-{label}",
            ),
        ),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=started,
        started_timestamp=started,
        ended_timestamp=ended,
        status=status,
        measurements=[],
        check_results=check_results or [_check_result(CheckOutcome.PASSED, "checks.row_count")],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )


@pytest.mark.parametrize("code", SCAN_GONE_CODES)
def test_results_upload_refused_by_a_finished_scan_is_not_a_delivery_failure(code):
    cloud = _cloud_answering(400, {"type": "_ROOT", "code": code, "message": "scan is done"})
    results = [_make_result(label="alpha"), _make_result(label="beta")]

    assert cloud.send_check_collection_results(results=results, wire_source="soda-contract") is None
    assert all(r.sending_results_to_soda_cloud_failed is False for r in results)


def test_results_upload_refused_for_any_other_reason_is_still_a_delivery_failure():
    """A 500 means the results genuinely did not land. Exit 4 is the point."""
    cloud = _cloud_answering(500, {})
    results = [_make_result(label="alpha")]

    assert cloud.send_check_collection_results(results=results, wire_source="soda-contract") is None
    assert all(r.sending_results_to_soda_cloud_failed is True for r in results)


def test_a_non_json_rejection_is_still_a_delivery_failure():
    """An HTML error page from a proxy carries no code; it is not evidence the scan is gone."""
    cloud = SodaCloud.__new__(SodaCloud)
    response = MagicMock()
    response.status_code = 502
    response.ok = False
    response.json.side_effect = ValueError("not json")
    response.text = "<html>Bad Gateway</html>"
    cloud._execute_command = MagicMock(return_value=response)
    results = [_make_result(label="alpha")]

    assert cloud.send_check_collection_results(results=results, wire_source="soda-contract") is None
    assert all(r.sending_results_to_soda_cloud_failed is True for r in results)


def test_cancelled_scan_exits_on_the_check_outcome_not_on_a_delivery_failure():
    """The end-to-end point: a cancelled scan must not exit 4.

    Exit codes above 3 make the contract launcher raise
    ``LibraryExecutionEngineException``, which kills the pod and raises an
    error-tracking alert. The checks ran fine, so their outcome decides.
    """
    cloud = _cloud_answering(400, {"code": "invalid_scan_state", "message": "already CANCELED"})
    result = _make_result(
        status=CheckCollectionStatus.FAILED,
        check_results=[
            _check_result(CheckOutcome.PASSED, "checks.todays_run"),
            _check_result(CheckOutcome.FAILED, "checks.row_count_match"),
        ],
    )

    cloud.send_check_collection_results(results=[result], wire_source="soda-contract")
    exit_code = interpret_contract_verification_result(ContractVerificationSessionResult([result]))

    assert exit_code == ExitCode.CHECK_FAILURES


@pytest.mark.parametrize("code", SCAN_GONE_CODES)
def test_marking_a_finished_scan_as_failed_needs_no_fallback(code):
    """``mark_scan_as_failed`` returns False to tell the caller to fall back to the
    exit code. A scan Cloud has already finalised needs no fallback — it is not
    waiting on us, and ``report_scan_execution_failure`` must not turn that into
    exit 4."""
    cloud = _cloud_answering(400, {"code": code, "message": "scan is done"})

    assert cloud.mark_scan_as_failed(scan_id="scan-1", logs=[]) is True


def test_marking_a_scan_as_failed_still_reports_a_real_rejection():
    cloud = _cloud_answering(500, {})

    assert cloud.mark_scan_as_failed(scan_id="scan-1", logs=[]) is False


@pytest.mark.parametrize("code", SCAN_GONE_CODES)
def test_batch_upload_to_a_finished_scan_reports_scan_gone(code):
    """``sodaCoreInsertScanDataBatch`` is the command that failed in the original incident:
    the scan definition of a running backfill was deleted, and every batch from that point
    on came back ``scan_not_found``."""
    cloud = _cloud_answering(400, {"code": code, "message": "scan is done"})

    outcome = cloud.insert_scan_data_batch({"definitionName": "s"}, scan_reference="ref-1")

    assert outcome is ReportOutcome.SCAN_GONE


def test_batch_upload_still_reports_a_real_rejection():
    cloud = _cloud_answering(503, {})

    outcome = cloud.insert_scan_data_batch({"definitionName": "s"}, scan_reference="ref-1")

    assert outcome is ReportOutcome.REFUSED


@pytest.mark.parametrize("code", SCAN_GONE_CODES)
def test_scan_results_insert_to_a_finished_scan_reports_scan_gone(code):
    """The discovery and profiling upload path shares the same contract."""
    cloud = _cloud_answering(400, {"code": code, "message": "scan is done"})

    assert cloud.insert_scan_results({"type": "sodaCoreInsertScanResults"}) is ReportOutcome.SCAN_GONE


def test_closing_the_ingestion_of_a_finished_scan_reports_scan_gone():
    """The scan can also reach its terminal state between the last batch and the end command."""
    cloud = _cloud_answering(400, {"code": "invalid_scan_state", "message": "already CANCELED"})

    assert cloud.scan_end_async(scan_reference="ref-1") is ReportOutcome.SCAN_GONE
