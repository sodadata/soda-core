from datetime import datetime, timezone

from soda_core.common.datetime_conversions import convert_datetime_to_str
from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO
from soda_core.discovery.discovery_payload import build_discovery_payload


def test_payload_conforms_to_insert_scan_results_dto():
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    # Every emitted key is a declared DTO field, and every required DTO field
    # (the backend's @NotNull/@NotEmpty set plus the discriminator) is emitted.
    assert set(payload.keys()) <= set(SodaCoreInsertScanResultsDTO.__annotations__.keys())
    assert SodaCoreInsertScanResultsDTO.__required_keys__ <= set(payload.keys())


def test_payload_is_dqn_only_v4():
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers", "postgres/soda/public/orders"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    assert payload["type"] == "sodaCoreInsertScanResults"
    assert payload["version"] == "4"
    assert payload["definitionName"] == "postgres_schema_discovery_scan"
    assert payload["defaultDataSource"] == "postgres"
    assert payload["metadata"] == [
        {"datasetQualifiedName": "postgres/soda/public/customers"},
        {"datasetQualifiedName": "postgres/soda/public/orders"},
    ]
    assert all(set(entry.keys()) == {"datasetQualifiedName"} for entry in payload["metadata"])


def test_payload_empty_discovery_yields_empty_metadata(monkeypatch):
    # A data source with nothing to discover: metadata is empty but the envelope
    # is still valid (version + @NotNull-validated timestamps present).
    monkeypatch.delenv("SODA_SCAN_DATA_TIMESTAMP", raising=False)
    payload = build_discovery_payload(
        dqns=[],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    assert payload["metadata"] == []
    assert payload["version"] == "4"
    for key in ("dataTimestamp", "scanStartTimestamp", "scanEndTimestamp"):
        assert isinstance(payload[key], str)
    assert SodaCoreInsertScanResultsDTO.__required_keys__ <= set(payload.keys())


def test_payload_includes_scan_id_when_env_set(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-1")
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    assert payload["scanId"] == "scan-1"


def test_payload_scan_id_is_none_when_env_unset(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    assert payload["scanId"] is None


def test_payload_contains_not_null_validated_scan_fields(monkeypatch):
    """dataTimestamp, scanStartTimestamp, scanEndTimestamp and hasErrors are
    @NotNull-validated by Soda Cloud's SodaCoreInsertScanResultsCommand."""
    monkeypatch.delenv("SODA_SCAN_DATA_TIMESTAMP", raising=False)
    scan_start = datetime(2026, 7, 10, 8, 30, 0, tzinfo=timezone.utc)
    scan_end = datetime(2026, 7, 10, 8, 30, 5, tzinfo=timezone.utc)
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
        scan_start_timestamp=scan_start,
        scan_end_timestamp=scan_end,
    )
    assert payload["hasErrors"] is False
    # Serialized as ISO-8601 UTC strings, identical to soda-core's to_jsonnable.
    assert payload["scanStartTimestamp"] == "2026-07-10T08:30:00+00:00"
    assert payload["scanEndTimestamp"] == "2026-07-10T08:30:05+00:00"
    # No SODA_SCAN_DATA_TIMESTAMP: data timestamp defaults to scan start.
    assert payload["dataTimestamp"] == "2026-07-10T08:30:00+00:00"


def test_payload_timestamps_default_to_now_when_not_provided(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_DATA_TIMESTAMP", raising=False)
    before = datetime.now(timezone.utc)
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    after = datetime.now(timezone.utc)
    for key in ("dataTimestamp", "scanStartTimestamp", "scanEndTimestamp"):
        assert isinstance(payload[key], str)
        assert convert_datetime_to_str(before) <= payload[key] <= convert_datetime_to_str(after)


def test_payload_data_timestamp_honors_env(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_DATA_TIMESTAMP", "2026-07-09T22:00:00Z")
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
        scan_start_timestamp=datetime(2026, 7, 10, 8, 30, 0, tzinfo=timezone.utc),
        scan_end_timestamp=datetime(2026, 7, 10, 8, 30, 5, tzinfo=timezone.utc),
    )
    assert payload["dataTimestamp"] == "2026-07-09T22:00:00+00:00"


def test_payload_data_timestamp_falls_back_to_scan_start_on_unparseable_env(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_DATA_TIMESTAMP", "not-a-timestamp")
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
        scan_start_timestamp=datetime(2026, 7, 10, 8, 30, 0, tzinfo=timezone.utc),
        scan_end_timestamp=datetime(2026, 7, 10, 8, 30, 5, tzinfo=timezone.utc),
    )
    assert payload["dataTimestamp"] == "2026-07-10T08:30:00+00:00"
