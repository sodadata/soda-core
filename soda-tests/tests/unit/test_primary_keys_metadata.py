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


def test_base_get_primary_keys_returns_empty_dict():
    # The base implementation must return an empty dict (safe default),
    # so data sources without an override don't break.
    data_source_impl = mock.MagicMock(spec=DataSourceImpl)
    result = DataSourceImpl.get_primary_keys(data_source_impl, dataset_prefixes=["public"], dataset_names=["orders"])
    assert result == {}


def test_get_results_groups_columns_by_table():
    query = _query()
    # Two tables, one with a composite primary key, to prove grouping.
    query_result = QueryResult(
        columns=[("table_name",), ("column_name",)],
        rows=[
            ("orders", "tenant_id"),
            ("orders", "id"),
            ("customers", "id"),
        ],
    )
    assert query.get_results(query_result) == {
        "orders": {"tenant_id", "id"},
        "customers": {"id"},
    }

    empty_result = QueryResult(columns=[("table_name",), ("column_name",)], rows=[])
    assert query.get_results(empty_result) == {}
