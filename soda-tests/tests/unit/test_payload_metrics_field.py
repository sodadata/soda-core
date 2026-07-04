"""``metrics: [...]`` plumbing in the scan-results payload."""

from __future__ import annotations

from datetime import datetime, timezone

from soda_core.common.soda_cloud import _build_check_collection_results_json_dict
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    ContractVerificationResult,
    DataSource,
    YamlFileContentInfo,
)


def _make_result(measurement_dicts=None) -> ContractVerificationResult:
    now = datetime.now(tz=timezone.utc)
    result = ContractVerificationResult(
        check_collection=Contract(
            data_source_name="test_ds",
            dataset_prefix=[],
            dataset_name="ORDERS",
            soda_qualified_dataset_name="test_ds/ORDERS",
            source=YamlFileContentInfo(source_content_str=None, local_file_path="fake.yml"),
        ),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )
    if measurement_dicts is not None:
        result.measurement_dicts = measurement_dicts
    return result


def test_contracts_only_payload_omits_metrics_key():
    """Byte-identical guard: without measurement_dicts there must be no ``metrics``
    key at all (``metrics: null`` or ``metrics: []`` are both wrong)."""
    result = _make_result()
    payload = _build_check_collection_results_json_dict([result])

    assert (
        "metrics" not in payload
    ), f"'metrics' key must be absent from contracts-only payload, got: {list(payload.keys())}"


def test_measurement_dicts_present_emits_metrics_array():
    """When measurement_dicts are attached the payload must contain ``metrics: [those dicts]``."""
    measurement_dicts = [
        {"identity": "m1", "value": 10},
        {"identity": "m2", "value": 20},
    ]
    result = _make_result(measurement_dicts=measurement_dicts)
    payload = _build_check_collection_results_json_dict([result])

    assert "metrics" in payload, f"'metrics' key missing from payload. Keys: {list(payload.keys())}"
    assert payload["metrics"] == measurement_dicts, f"Expected metrics={measurement_dicts}, got {payload['metrics']}"
