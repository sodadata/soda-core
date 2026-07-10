import pytest

from soda_core.discovery.discovery_payload import build_discovery_payload
from soda_core.cli.handlers.data_source import resolve_scan_definition_name


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


def test_scan_definition_name_prefers_cli_arg(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_DEFINITION", "env_scan_def")
    result = resolve_scan_definition_name(
        scan_definition_name="cli_arg_scan_def",
        data_source_name="postgres",
    )
    assert result == "cli_arg_scan_def"


def test_scan_definition_name_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_DEFINITION", "env_scan_def")
    result = resolve_scan_definition_name(
        scan_definition_name=None,
        data_source_name="postgres",
    )
    assert result == "env_scan_def"


def test_scan_definition_name_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_DEFINITION", raising=False)
    result = resolve_scan_definition_name(
        scan_definition_name=None,
        data_source_name="postgres",
    )
    assert result == "postgres_schema_discovery_scan"
