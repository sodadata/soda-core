from __future__ import annotations

from unittest import mock

import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import ColumnMetadata
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_primary_keys_query import (
    MetadataPrimaryKeysQuery,
)


class _SchemaOnlyDialect(SqlDialect, sqlglot_dialect="schema-only"):
    # Schema-only dialect (DuckDB shape): schema at index 0, no database.
    def get_database_prefix_index(self) -> int | None:
        return None

    def get_schema_prefix_index(self) -> int | None:
        return 0


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


def test_get_results_groups_columns_by_table_in_key_order():
    query = _query()
    # Two tables, one with a composite primary key. The ordinal position (third column) is fed out
    # of row order to prove get_results orders each key by it rather than by row order.
    query_result = QueryResult(
        columns=[("table_name",), ("column_name",), ("ordinal_position",)],
        rows=[
            ("orders", "id", 2),
            ("orders", "tenant_id", 1),
            ("customers", "id", 1),
        ],
    )
    assert query.get_results(query_result) == {
        "orders": ["tenant_id", "id"],
        "customers": ["id"],
    }

    empty_result = QueryResult(columns=[("table_name",), ("column_name",), ("ordinal_position",)], rows=[])
    assert query.get_results(empty_result) == {}


def test_build_namespace_raises_when_schema_prefix_missing():
    # Base SqlDialect requires a schema at prefix index 1 (Postgres shape, db+schema).
    # A prefix list too short to hold that index must fail loud, not emit table_schema = NULL.
    query = _query()
    with pytest.raises(ValueError) as excinfo:
        query._build_namespace(["only_database"])
    message = str(excinfo.value)
    assert "only_database" in message
    assert "schema" in message.lower()


def test_build_namespace_resolves_schema_for_db_schema_dialect():
    # A valid db+schema prefix list must not raise and must carry both parts.
    namespace = _query()._build_namespace(["my_db", "my_schema"])
    assert namespace.get_database_for_metadata_query() == "my_db"
    assert namespace.get_schema_for_metadata_query() == "my_schema"


def test_build_namespace_resolves_schema_for_schema_only_dialect():
    # Schema-only dialect: schema at index 0, missing database is expected (not an error).
    namespace = _query(_SchemaOnlyDialect())._build_namespace(["my_schema"])
    assert namespace.get_database_for_metadata_query() is None
    assert namespace.get_schema_for_metadata_query() == "my_schema"


def test_build_namespace_raises_for_schema_only_dialect_with_empty_prefixes():
    # Even schema-only dialects require the schema; an empty prefix list must fail loud.
    with pytest.raises(ValueError):
        _query(_SchemaOnlyDialect())._build_namespace([])


def test_build_sql_statement_joins_on_schema_and_table_not_just_constraint_name():
    """Guards the constraint-namespace JOIN: constraint_name is unique only within
    (constraint_catalog, constraint_schema), so the table_constraints -> key_column_usage
    JOIN must also match schema + table (+ catalog when a database is present) or PK
    columns leak across schemas (see the cross-schema integration test). A regression
    dropping one of these join conditions must fail here, not only on live postgres."""
    query = _query()
    namespace = query._build_namespace(["my_db", "my_schema"])
    sql = SqlDialect().build_select_sql(query.build_sql_statement(namespace, ["orders", "customers"]))

    join_conditions = [
        '"constraints"."constraint_name" = "key_columns"."constraint_name"',
        '"constraints"."table_schema" = "key_columns"."table_schema"',
        '"constraints"."table_name" = "key_columns"."table_name"',
        '"constraints"."table_catalog" = "key_columns"."table_catalog"',
    ]
    for join_condition in join_conditions:
        assert join_condition in sql, f"missing JOIN condition: {join_condition}\n{sql}"

    # The ordinal keeps composite keys in declared order.
    assert '"key_columns"."ordinal_position"' in sql
    # Both filters and the table IN-list are present.
    assert "'my_schema'" in sql and "'my_db'" in sql and "'orders'" in sql and "'customers'" in sql
