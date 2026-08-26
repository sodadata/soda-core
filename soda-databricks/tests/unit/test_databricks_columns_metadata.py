"""Databricks absorbs columns-metadata failures for bulk callers but not for the schema check, which
would otherwise read a metadata failure as a dataset with no columns."""

import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import ColumnMetadata, SqlDataType
from soda_core.common.yaml import DataSourceYamlSource

_COLUMNS = [ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="string"))]

_DATA_SOURCE_YAML = """
type: databricks
name: test_databricks
connection:
    host: example.cloud.databricks.com
    http_path: /sql/1.0/warehouses/abc
    access_token: not-used-no-connection-is-opened
    catalog: unity_catalog
"""


def _data_source_impl() -> DataSourceImpl:
    return DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))


@pytest.fixture
def failing_accessor(monkeypatch) -> DataSourceImpl:
    def _raise(self, dataset_prefixes, dataset_name):
        raise RuntimeError("metadata query failed")

    monkeypatch.setattr(DataSourceImpl, "get_columns_metadata", _raise)
    return _data_source_impl()


def test_the_bulk_accessor_still_absorbs_the_failure(failing_accessor, caplog):
    assert failing_accessor.get_columns_metadata(["catalog", "schema"], "orders") == []
    assert "Returning empty list" in caplog.text


def test_the_schema_check_accessor_lets_the_failure_surface(failing_accessor):
    with pytest.raises(RuntimeError, match="metadata query failed"):
        failing_accessor.get_schema_check_columns_metadata(["catalog", "schema"], "orders")


def test_the_schema_check_accessor_returns_columns_when_the_base_succeeds(monkeypatch):
    monkeypatch.setattr(DataSourceImpl, "get_columns_metadata", lambda self, dataset_prefixes, dataset_name: _COLUMNS)

    assert _data_source_impl().get_schema_check_columns_metadata(["catalog", "schema"], "orders") == _COLUMNS
