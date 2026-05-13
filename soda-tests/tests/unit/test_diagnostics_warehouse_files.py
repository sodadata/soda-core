from soda_core.contracts.impl.diagnostics_warehouse_files import (
    DiagnosticsWarehouseFiles,
)


def test_normalize_none_returns_none():
    assert DiagnosticsWarehouseFiles.normalize(None) is None


def test_normalize_string_wraps_as_primary_only():
    result = DiagnosticsWarehouseFiles.normalize("/path/to/dwh.yaml")
    assert result == DiagnosticsWarehouseFiles(primary_path="/path/to/dwh.yaml", metadata_path=None)


def test_normalize_empty_string_wraps_as_primary_only():
    # Defensive: empty string is still passed through unchanged as primary path.
    # Callers above us are responsible for deciding what "empty" means.
    result = DiagnosticsWarehouseFiles.normalize("")
    assert result == DiagnosticsWarehouseFiles(primary_path="", metadata_path=None)


def test_normalize_instance_passes_through():
    instance = DiagnosticsWarehouseFiles(primary_path="/p.yaml", metadata_path="/m.yaml")
    assert DiagnosticsWarehouseFiles.normalize(instance) is instance


def test_is_empty():
    assert DiagnosticsWarehouseFiles().is_empty is True
    assert DiagnosticsWarehouseFiles(primary_path="/p").is_empty is False
    assert DiagnosticsWarehouseFiles(metadata_path="/m").is_empty is False
