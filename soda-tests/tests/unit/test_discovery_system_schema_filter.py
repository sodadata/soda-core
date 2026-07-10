"""System-schema exclusion in discovery, ported from soda-library (OBSL-1013).

v3 soda-library filters metadata query rows through DataSource.is_system_schema
(soda/execution/data_source.py) so that data source internal schemas such as
postgres' pg_catalog never show up in discovery. v4 mirrors that as a SqlDialect
hook applied in DataSourceImpl.discover_qualified_objects.
"""

from types import SimpleNamespace

import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.table_types import FullyQualifiedTableName
from soda_postgres.common.data_sources.postgres_data_source import PostgresSqlDialect


@pytest.mark.parametrize(
    "schema_name, expected",
    [
        # v3 base default: `return schema_name.lower() == "information_schema"`
        ("information_schema", True),
        ("INFORMATION_SCHEMA", True),
        ("public", False),
        ("main", False),
    ],
)
def test_base_dialect_treats_information_schema_as_system_schema(schema_name, expected):
    assert SqlDialect().is_system_schema(schema_name) is expected


class _FakeMetadataTablesQuery:
    def __init__(self, objects):
        self._objects = objects

    def execute(self, **_kwargs):
        return self._objects


def _discover(sql_dialect, objects):
    """Calls the real DataSourceImpl.discover_qualified_objects with a stub self."""
    fake_data_source_impl = SimpleNamespace(
        sql_dialect=sql_dialect,
        create_metadata_tables_query=lambda: _FakeMetadataTablesQuery(objects),
        extract_database_from_prefix=lambda prefixes: None,
        extract_schema_from_prefix=lambda prefixes: None,
    )
    return DataSourceImpl.discover_qualified_objects(fake_data_source_impl, prefixes=[])


def _table(schema_name, table_name):
    return FullyQualifiedTableName(database_name="soda", schema_name=schema_name, table_name=table_name)


def test_discover_qualified_objects_drops_system_schemas():
    objects = _discover(
        PostgresSqlDialect(),
        [
            _table("public", "customers"),
            _table("pg_catalog", "pg_class"),
            _table("information_schema", "tables"),
            _table("pg_toast", "pg_toast_1234"),
        ],
    )
    assert [(o.schema_name, o.table_name) for o in objects] == [("public", "customers")]


def test_discover_qualified_objects_keeps_objects_without_schema():
    # Some dialects (e.g. sparkdf) can produce objects without a schema; the
    # system-schema filter must not choke on None.
    no_schema_table = FullyQualifiedTableName(database_name=None, schema_name=None, table_name="t")
    assert _discover(SqlDialect(), [no_schema_table]) == [no_schema_table]
