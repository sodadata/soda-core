"""MetadataTablesQuery.get_results row-to-object mapping.

The DiscoveryRun unit tests feed pre-built FullyQualified*Name objects and so
bypass convert_table_type_to_enum. These tests pin the coercion path itself:
an unrecognized table type (e.g. postgres' "FOREIGN TABLE") defaults to TABLE
and is still returned (v3 parity).
"""

from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.statements.table_types import (
    FullyQualifiedMaterializedViewName,
    FullyQualifiedTableName,
    FullyQualifiedViewName,
    TableType,
)

_COLUMNS = (("database_name",), ("schema_name",), ("table_name",), ("table_type",))
_ALL_TYPES = [TableType.TABLE, TableType.VIEW, TableType.MATERIALIZED_VIEW]


def _get_results(rows, types_to_return=_ALL_TYPES):
    query = MetadataTablesQuery(sql_dialect=SqlDialect(), data_source_connection=None)
    return query.get_results(QueryResult(rows=rows, columns=_COLUMNS), types_to_return)


def test_unknown_table_type_coerces_to_table_and_is_returned():
    results = _get_results([("soda", "public", "remote_customers", "FOREIGN TABLE")])
    assert results == [
        FullyQualifiedTableName(database_name="soda", schema_name="public", table_name="remote_customers")
    ]


def test_maps_each_known_table_type_to_its_object():
    results = _get_results(
        [
            ("soda", "public", "customers", "BASE TABLE"),
            ("soda", "public", "customers_view", "VIEW"),
            ("soda", "public", "customers_mv", "MATERIALIZED VIEW"),
        ]
    )
    assert results == [
        FullyQualifiedTableName(database_name="soda", schema_name="public", table_name="customers"),
        FullyQualifiedViewName(database_name="soda", schema_name="public", view_name="customers_view"),
        FullyQualifiedMaterializedViewName(
            database_name="soda", schema_name="public", materialized_view_name="customers_mv"
        ),
    ]


def test_filters_out_types_not_requested():
    results = _get_results(
        [
            ("soda", "public", "customers", "BASE TABLE"),
            ("soda", "public", "customers_view", "VIEW"),
        ],
        types_to_return=[TableType.TABLE],
    )
    assert results == [FullyQualifiedTableName(database_name="soda", schema_name="public", table_name="customers")]
