"""HiveMetadataTablesQuery SHOW TABLES/VIEWS catalog qualification.

SHOW TABLES resolves an unqualified schema against the session's current catalog. The
catalog in the prefixes is not necessarily that one, so it must be emitted when known —
while callers with no catalog concept (sparkdf legacy mode, database_prefix_index=None)
must keep the unqualified form.
"""

import pytest
from soda_core.common.statements.table_types import TableType
from soda_databricks.common.data_sources.databricks_data_source import (
    DatabricksHiveSqlDialect,
)
from soda_databricks.common.statements.hive_metadata_tables_query import (
    HiveMetadataTablesQuery,
)


def _query() -> HiveMetadataTablesQuery:
    return HiveMetadataTablesQuery(sql_dialect=DatabricksHiveSqlDialect(), data_source_connection=None)


@pytest.mark.parametrize(
    "database_name, schema_name, object_type, expected_sql",
    [
        pytest.param(
            "my_uc_catalog",
            "soda_diagnostics",
            TableType.TABLE,
            "SHOW TABLES FROM `my_uc_catalog`.`soda_diagnostics`",
            id="tables_qualified_with_catalog",
        ),
        pytest.param(
            "my_uc_catalog",
            "soda_diagnostics",
            TableType.VIEW,
            "SHOW VIEWS FROM `my_uc_catalog`.`soda_diagnostics`",
            id="views_qualified_with_catalog",
        ),
        pytest.param(
            None,
            "soda_diagnostics",
            TableType.TABLE,
            "SHOW TABLES FROM `soda_diagnostics`",
            id="no_catalog_stays_unqualified",
        ),
        pytest.param(
            "my_uc_catalog",
            None,
            TableType.TABLE,
            "SHOW TABLES",
            id="no_schema_emits_no_from",
        ),
    ],
)
def test_build_sql_statement(database_name, schema_name, object_type, expected_sql):
    sql = _query().build_sql_statement(
        database_name=database_name, schema_name=schema_name, object_type_to_fetch=object_type
    )
    assert sql == expected_sql


def test_invalid_object_type_raises():
    with pytest.raises(ValueError, match="Invalid object type to fetch"):
        _query().build_sql_statement(
            database_name="my_uc_catalog",
            schema_name="soda_diagnostics",
            object_type_to_fetch=TableType.MATERIALIZED_VIEW,
        )
