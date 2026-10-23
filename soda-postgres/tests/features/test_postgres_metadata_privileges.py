"""Postgres shows every role the names in pg_catalog, so the metadata queries must filter by
privilege themselves. Otherwise discovery lists tables the connecting role cannot read."""

from __future__ import annotations

from uuid import uuid4

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.statements.metadata_tables_query import TableType

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("postgres_metadata_privileges")
    .column_varchar("id")
    .column_integer("age")
    .build()
)


# SET ROLE changes the results of identical SQL, which a replayed snapshot cannot tell apart.
@pytest.mark.no_snapshot
def test_metadata_queries_hide_objects_the_role_cannot_read(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    schema_name = data_source_test_helper.extract_schema_from_prefix()
    qualified_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)
    role = f"soda_test_no_access_{uuid4().hex[:8]}"

    def visible_table_names() -> list[str]:
        tables = data_source_impl.create_metadata_tables_query().execute(
            schema_name=schema_name, types_to_return=[TableType.TABLE]
        )
        return [table.table_name for table in tables]

    def visible_column_names() -> list[str]:
        columns = data_source_impl.get_columns_metadata(
            dataset_prefixes=data_source_test_helper.dataset_prefix, dataset_name=test_table.unique_name
        )
        return [column.column_name for column in columns]

    def as_role() -> tuple[list[str], list[str]]:
        data_source_impl.execute_update(f'SET ROLE "{role}"')
        try:
            return visible_table_names(), visible_column_names()
        finally:
            data_source_impl.execute_update("RESET ROLE")

    data_source_impl.execute_update(f'CREATE ROLE "{role}" NOLOGIN')
    try:
        assert as_role() == ([], [])

        data_source_impl.execute_update(f'GRANT USAGE ON SCHEMA "{schema_name}" TO "{role}"')
        data_source_impl.execute_update(f'GRANT SELECT ("id") ON {qualified_name} TO "{role}"')
        assert as_role() == ([test_table.unique_name], ["id"])

        data_source_impl.execute_update(f'GRANT SELECT ON {qualified_name} TO "{role}"')
        assert as_role() == ([test_table.unique_name], ["id", "age"])
    finally:
        data_source_impl.execute_update(f'DROP OWNED BY "{role}"')
        data_source_impl.execute_update(f'DROP ROLE "{role}"')
