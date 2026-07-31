from __future__ import annotations

from unittest import mock

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import ColumnMetadata
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_primary_keys_query import (
    MetadataPrimaryKeysQuery,
)


def _query(sql_dialect: SqlDialect | None = None) -> MetadataPrimaryKeysQuery:
    return MetadataPrimaryKeysQuery(sql_dialect=sql_dialect or SqlDialect(), data_source_connection=None)


def test_column_metadata_carries_is_primary_key():
    # Default is False so existing construction sites keep working.
    assert ColumnMetadata(column_name="id").is_primary_key is False

    pk_column = ColumnMetadata(column_name="id", is_primary_key=True)
    assert pk_column.is_primary_key is True


def test_base_get_primary_keys_returns_empty_set():
    # The base implementation must return an empty set (safe default),
    # so data sources without an override don't break.
    data_source_impl = mock.MagicMock(spec=DataSourceImpl)
    result = DataSourceImpl.get_primary_keys(data_source_impl, dataset_prefixes=["public"], dataset_name="orders")
    assert result == set()


def test_get_results_extracts_column_names():
    query = _query()
    query_result = QueryResult(columns=[("column_name",)], rows=[("id",), ("tenant_id",)])
    assert query.get_results(query_result) == {"id", "tenant_id"}

    empty_result = QueryResult(columns=[("column_name",)], rows=[])
    assert query.get_results(empty_result) == set()
