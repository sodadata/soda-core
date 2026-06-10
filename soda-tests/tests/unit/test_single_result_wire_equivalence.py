"""Wire equivalence: the unified builder's output for N=1 matches the legacy
contract per-file shape, and the contract field is conditional on
``wire_source == "soda-contract"``.

This is the load-bearing test for the single+multi builder unification:
contracts go through the same code path as combine-upload subtypes, just
with a 1-element list. Any drift in the wire shape would break either
the contract path (BC) or the combined path (data-standards).
"""

from __future__ import annotations

from datetime import datetime, timezone

from soda_core.common.logs import Location
from soda_core.common.soda_cloud import _build_check_collection_results_json_dict
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    DataSource,
    PostProcessingStage,
    PostProcessingStageState,
    YamlFileContentInfo,
)


def _make_check_result(*, path: str = "checks.row_count", outcome: CheckOutcome = CheckOutcome.PASSED) -> CheckResult:
    return CheckResult(
        check=Check(
            column_name="id",
            type="row_count",
            qualifier=None,
            name="row count",
            path=path,
            full_path=path,
            identity="abc",
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


def _make_single_result(
    *,
    status: CheckCollectionStatus = CheckCollectionStatus.PASSED,
    check_results=None,
    post_processing_stages=None,
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
                source_content_str="# fake",
                local_file_path="/fake/x.yml",
                soda_cloud_file_id="file-x",
            ),
        ),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=started,
        started_timestamp=started,
        ended_timestamp=ended,
        status=status,
        measurements=[],
        check_results=check_results or [_make_check_result()],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=post_processing_stages or [],
    )


def test_n1_contract_payload_carries_contract_field():
    """A 1-element batch with wire_source='soda-contract' includes the
    ``contract`` field (contract-handler routing on the backend)."""
    payload = _build_check_collection_results_json_dict([_make_single_result()], wire_source="soda-contract")
    assert "contract" in payload, "contract field MUST be present for soda-contract wire source"
    assert payload["contract"]["fileId"] == "file-x"


def test_n1_non_contract_payload_omits_contract_field():
    """A 1-element batch with non-contract wire_source has ``contract = None``;
    ``to_jsonnable`` strips it from the serialized payload — backend routing
    falls through to scan-def-type dispatch."""
    payload = _build_check_collection_results_json_dict([_make_single_result()], wire_source="data-standard")
    assert "contract" not in payload


def test_n1_session_fields_match_single_result():
    """Session-level fields (timestamps, has_*, dataTimestamp) on a 1-element
    batch come from the single result — degenerate min/max/any return the
    same value."""
    result = _make_single_result(status=CheckCollectionStatus.FAILED)
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")

    assert payload["scanStartTimestamp"] == "2026-01-01T12:00:00+00:00"
    assert payload["scanEndTimestamp"] == "2026-01-01T12:01:00+00:00"
    assert payload["dataTimestamp"] == "2026-01-01T12:00:00+00:00"
    assert payload["hasErrors"] is False
    assert payload["hasFailures"] is True
    assert payload["hasWarns"] is False
    assert payload["definitionName"] == "test_ds/s/t"
    assert payload["defaultDataSource"] == "test_ds"
    assert payload["defaultDataSourceProperties"] == {"type": "postgres"}


def test_n1_checks_list_matches_single_result_checks():
    """A 1-element batch's ``checks`` list equals the single result's
    per-file checks — flatten([cvr.checks]) is just cvr.checks."""
    result = _make_single_result(
        check_results=[_make_check_result(path="checks.a"), _make_check_result(path="checks.b")]
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["checks"] is not None
    assert [c["checkPath"] for c in payload["checks"]] == ["checks.a", "checks.b"]


def test_n1_post_processing_stages_match_single_result():
    """Dedup-by-name on a 1-element batch with unique stage names is a no-op
    — output is the single result's stages verbatim."""
    result = _make_single_result(
        post_processing_stages=[
            PostProcessingStage(name="dwh", stage=PostProcessingStageState.ONGOING),
            PostProcessingStage(name="other", stage=PostProcessingStageState.ONGOING),
        ],
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["postProcessingStages"] == [{"name": "dwh"}, {"name": "other"}]


def test_n1_ingestion_mode_partial_when_check_excluded():
    """Single result with one EXCLUDED check → PARTIAL — same as the
    legacy ``determine_verification_ingestion_mode`` behaviour."""
    result = _make_single_result(check_results=[_make_check_result(outcome=CheckOutcome.EXCLUDED)])
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["resultsIngestionMode"].upper() == "PARTIAL"


def test_n1_ingestion_mode_full_when_no_excluded():
    """Single result with no EXCLUDED checks → FULL."""
    result = _make_single_result(check_results=[_make_check_result(outcome=CheckOutcome.PASSED)])
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["resultsIngestionMode"].upper() == "FULL"
