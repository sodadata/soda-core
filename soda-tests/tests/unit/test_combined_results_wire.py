"""Unit tests for the combined-upload wire payload.

Locks the shape of ``_build_check_collection_results_json_dict`` (the
unified single+multi builder) when called with N≥2 results from
``combine_uploads = True`` subtypes, and the ``SodaCloud
.send_check_collection_results`` API the executor uses. The single-result
case is exercised separately in
``test_contract_wire_source_plumbed_to_upload.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from soda_core.common.logs import Location
from soda_core.common.soda_cloud import (
    SodaCloud,
    _build_check_collection_results_json_dict,
)
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


def _make_check_result(*, outcome: CheckOutcome = CheckOutcome.PASSED, path: str = "checks.row_count") -> CheckResult:
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


def _make_result(
    *,
    label: str,
    status: CheckCollectionStatus = CheckCollectionStatus.PASSED,
    check_results=None,
    post_processing_stages=None,
    started_minute: int = 0,
    ended_minute: int = 1,
) -> ContractVerificationResult:
    started = datetime(2026, 1, 1, 12, started_minute, 0, tzinfo=timezone.utc)
    ended = datetime(2026, 1, 1, 12, ended_minute, 0, tzinfo=timezone.utc)
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
        check_results=check_results or [_make_check_result()],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=post_processing_stages or [],
    )


def test_combined_payload_flattens_check_results():
    """Two files with 2+1 checks → 3-element ``checks`` array, in order."""
    r1 = _make_result(
        label="alpha",
        check_results=[
            _make_check_result(path="checks.a"),
            _make_check_result(path="checks.b"),
        ],
    )
    r2 = _make_result(label="beta", check_results=[_make_check_result(path="checks.c")])

    payload = _build_check_collection_results_json_dict([r1, r2], wire_source="data-standard")

    assert payload["checks"] is not None
    assert [c["checkPath"] for c in payload["checks"]] == ["checks.a", "checks.b", "checks.c"]
    # Every check carries the supplied wire_source.
    assert all(c["source"] == "data-standard" for c in payload["checks"])


def test_combined_payload_or_aggregates_status_flags():
    """``hasErrors / hasWarns / hasFailures`` are ORs over per-file values."""
    r_passed = _make_result(label="ok", status=CheckCollectionStatus.PASSED)
    r_warned = _make_result(label="warn", status=CheckCollectionStatus.WARNED)
    r_failed = _make_result(label="fail", status=CheckCollectionStatus.FAILED)
    r_errored = _make_result(label="err", status=CheckCollectionStatus.ERROR)

    payload = _build_check_collection_results_json_dict(
        [r_passed, r_warned, r_failed, r_errored], wire_source="data-standard"
    )

    assert payload["hasErrors"] is True
    assert payload["hasWarns"] is True
    assert payload["hasFailures"] is True


def test_combined_payload_status_flags_all_false_when_no_problems():
    """All-PASSED batch → all flags False."""
    r1 = _make_result(label="alpha")
    r2 = _make_result(label="beta")
    payload = _build_check_collection_results_json_dict([r1, r2], wire_source="data-standard")
    assert payload["hasErrors"] is False
    assert payload["hasWarns"] is False
    assert payload["hasFailures"] is False


def test_combined_payload_picks_min_max_timestamps():
    """``scanStartTimestamp = min(starts)``, ``scanEndTimestamp = max(ends)``."""
    early = _make_result(label="early", started_minute=10, ended_minute=12)
    late = _make_result(label="late", started_minute=20, ended_minute=25)
    middle = _make_result(label="middle", started_minute=15, ended_minute=18)

    payload = _build_check_collection_results_json_dict([late, early, middle], wire_source="data-standard")

    assert payload["scanStartTimestamp"] == "2026-01-01T12:10:00+00:00"
    assert payload["scanEndTimestamp"] == "2026-01-01T12:25:00+00:00"


def test_combined_payload_omits_contract_field():
    """Combined uploads only fire for non-contract subtypes; ``contract``
    is null so the backend's contract-handler routing falls through to
    scan-def-type dispatch for the subtype. ``to_jsonnable`` strips
    null-valued dict keys, so the field doesn't appear at all in the
    serialized payload — same shape as the contract path produces for
    non-contract wire sources today."""
    payload = _build_check_collection_results_json_dict([_make_result(label="alpha")], wire_source="data-standard")
    assert "contract" not in payload


def test_combined_payload_dedups_post_processing_stages_by_name():
    """Same stage name in two files → one entry in the combined payload."""
    r1 = _make_result(
        label="alpha",
        post_processing_stages=[PostProcessingStage(name="dwh-stage", stage=PostProcessingStageState.ONGOING)],
    )
    r2 = _make_result(
        label="beta",
        post_processing_stages=[
            PostProcessingStage(name="dwh-stage", stage=PostProcessingStageState.ONGOING),
            PostProcessingStage(name="other-stage", stage=PostProcessingStageState.ONGOING),
        ],
    )
    payload = _build_check_collection_results_json_dict([r1, r2], wire_source="data-standard")
    assert payload["postProcessingStages"] == [{"name": "dwh-stage"}, {"name": "other-stage"}]


def test_combined_payload_definition_name_uses_scan_suffix():
    """``definitionName`` uses the dataset qualified name + scan suffix."""
    payload = _build_check_collection_results_json_dict(
        [_make_result(label="alpha")],
        wire_source="data-standard",
        scan_definition_suffix="data_standards_scan",
    )
    assert payload["definitionName"] == "test_ds/s/t_data_standards_scan"


def test_combined_payload_ingestion_mode_partial_when_any_check_excluded():
    """One EXCLUDED check anywhere in the batch → mode = PARTIAL."""
    r1 = _make_result(label="alpha", check_results=[_make_check_result(outcome=CheckOutcome.PASSED)])
    r2 = _make_result(label="beta", check_results=[_make_check_result(outcome=CheckOutcome.EXCLUDED)])
    payload = _build_check_collection_results_json_dict([r1, r2], wire_source="data-standard")
    assert payload["resultsIngestionMode"] == "partial"


def test_combined_payload_ingestion_mode_full_otherwise():
    """No EXCLUDED checks anywhere → mode = FULL."""
    r1 = _make_result(label="alpha")
    r2 = _make_result(label="beta")
    payload = _build_check_collection_results_json_dict([r1, r2], wire_source="data-standard")
    assert payload["resultsIngestionMode"] == "full"


def test_send_check_collection_results_no_op_on_empty_list():
    """Empty ``results`` returns None without hitting the backend."""
    cloud = SodaCloud.__new__(SodaCloud)  # bypass __init__; we mock _execute_command
    cloud._execute_command = MagicMock()
    out = cloud.send_check_collection_results(results=[], wire_source="data-standard")
    assert out is None
    cloud._execute_command.assert_not_called()


def test_send_check_collection_results_stamps_scan_id_on_every_result():
    """200 OK with a scanId → every per-file result gets ``scan_id`` set."""
    cloud = SodaCloud.__new__(SodaCloud)
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"scanId": "scan-42"}
    cloud._execute_command = MagicMock(return_value=response)

    results = [_make_result(label="alpha"), _make_result(label="beta")]
    out = cloud.send_check_collection_results(results=results, wire_source="data-standard")

    assert out == {"scanId": "scan-42"}
    assert all(r.scan_id == "scan-42" for r in results)
    assert all(r.sending_results_to_soda_cloud_failed is False for r in results)


def test_send_check_collection_results_n1_stamps_scan_id_and_dataset_id():
    """1-element call (production path: autopilot + non-combine ``verify()``)
    stamps both ``scan_id`` and ``dataset_id`` on the single result.

    The dataset_id resolution moved from the per-file ``verify()`` call
    site into the unified sender as part of the unification; this test
    locks the N=1 stamping behaviour the migrated callers rely on.
    """
    cloud = SodaCloud.__new__(SodaCloud)
    response = MagicMock()
    response.status_code = 200
    # Per-check ``datasets`` shape that ``extract_dataset_id_from_response``
    # walks — same dqn as the result's ``soda_qualified_dataset_name``.
    response.json.return_value = {
        "scanId": "scan-n1",
        "checks": [{"datasets": [{"dqn": "test_ds/s/t", "id": "ds-id-1"}]}],
    }
    cloud._execute_command = MagicMock(return_value=response)

    (result,) = [_make_result(label="solo")]
    out = cloud.send_check_collection_results(results=[result], wire_source="soda-contract")

    assert out == response.json.return_value
    assert result.scan_id == "scan-n1"
    assert result.check_collection.dataset_id == "ds-id-1"
    assert result.sending_results_to_soda_cloud_failed is False


def test_send_check_collection_results_marks_all_failed_on_non_200():
    """Non-200 → every per-file result gets ``sending_results_to_soda_cloud_failed = True``."""
    cloud = SodaCloud.__new__(SodaCloud)
    response = MagicMock()
    response.status_code = 500
    response.json.return_value = {}
    cloud._execute_command = MagicMock(return_value=response)

    results = [_make_result(label="alpha"), _make_result(label="beta")]
    out = cloud.send_check_collection_results(results=results, wire_source="data-standard")

    assert out is None
    assert all(r.sending_results_to_soda_cloud_failed is True for r in results)


def test_send_check_collection_results_marks_all_failed_when_response_omits_scan_id():
    """200 OK but no scanId in the body → mark all results failed-to-send.

    We can't pick which file's ingest failed without a scanId to correlate,
    so the conservative move is to flag the whole batch."""
    cloud = SodaCloud.__new__(SodaCloud)
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"cloudUrl": "https://cloud.example/dataset"}
    cloud._execute_command = MagicMock(return_value=response)

    results = [_make_result(label="alpha"), _make_result(label="beta")]
    out = cloud.send_check_collection_results(results=results, wire_source="data-standard")

    assert out == {"cloudUrl": "https://cloud.example/dataset"}
    assert all(r.sending_results_to_soda_cloud_failed is True for r in results)
    assert all(r.scan_id is None for r in results)
