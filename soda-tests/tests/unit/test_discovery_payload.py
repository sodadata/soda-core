# soda-tests/tests/unit/test_discovery_payload.py
from soda_core.discovery.discovery_payload import build_discovery_payload


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
