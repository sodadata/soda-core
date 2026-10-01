"""System-table-name exclusion in discovery.

Some data source internal objects live inside customer schemas (e.g. Databricks'
``__materialization_mat_*`` metric-view materializations), so the schema filter cannot
catch them. SqlDialect.is_system_table_name is applied as a row filter in
DataSourceImpl.discover_qualified_objects, next to is_system_schema.
"""

from types import SimpleNamespace

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.table_types import FullyQualifiedTableName, FullyQualifiedViewName


def test_base_dialect_has_no_system_table_names():
    assert SqlDialect().is_system_table_name("__materialization_mat_x") is False
    assert SqlDialect().is_system_table_name("customers") is False


class _FakeMetadataTablesQuery:
    def __init__(self, objects):
        self._objects = objects

    def execute(self, **_kwargs):
        return self._objects


class _DialectWithSystemTables(SqlDialect, sqlglot_dialect="postgres"):
    def is_system_table_name(self, table_name: str) -> bool:
        return table_name.startswith("__internal_")


def _discover(sql_dialect, objects):
    """Calls the real DataSourceImpl.discover_qualified_objects with a stub self."""
    fake_data_source_impl = SimpleNamespace(
        sql_dialect=sql_dialect,
        create_metadata_tables_query=lambda: _FakeMetadataTablesQuery(objects),
        extract_database_from_prefix=lambda prefixes: None,
        extract_schema_from_prefix=lambda prefixes: None,
    )
    return DataSourceImpl.discover_qualified_objects(fake_data_source_impl, prefixes=[])


def test_discover_qualified_objects_drops_system_table_names_of_every_object_type():
    objects = _discover(
        _DialectWithSystemTables(),
        [
            FullyQualifiedTableName(database_name="main", schema_name="sales", table_name="orders"),
            FullyQualifiedTableName(database_name="main", schema_name="sales", table_name="__internal_orders_mat"),
            FullyQualifiedViewName(database_name="main", schema_name="sales", view_name="__internal_orders_view"),
        ],
    )
    assert [o.get_object_name() for o in objects] == ["orders"]


def test_discover_qualified_objects_drops_databricks_materializations():
    from soda_databricks.common.data_sources.databricks_data_source import DatabricksSqlDialect

    objects = _discover(
        DatabricksSqlDialect(),
        [
            FullyQualifiedTableName(database_name="main", schema_name="dms_nifty", table_name="customers"),
            FullyQualifiedTableName(
                database_name="main",
                schema_name="dms_nifty",
                table_name="__materialization_mat_0a1b2c___metric_view_mat_revenue",
            ),
        ],
    )
    assert [o.get_object_name() for o in objects] == ["customers"]
