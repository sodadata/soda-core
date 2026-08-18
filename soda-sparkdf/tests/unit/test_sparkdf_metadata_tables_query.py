"""SparkDataFrameMetadataTablesQuery inherits HiveMetadataTablesQuery.build_sql_statement.

Unlike the Databricks SQL connector it is used unconditionally, so catalog mode must emit
the catalog: nothing in sparkdf issues USE CATALOG, and the session's current_catalog() is
typically hive_metastore even for Unity-Catalog-only users.
"""

from soda_core.common.statements.table_types import TableType
from soda_sparkdf.common.data_sources.sparkdf_data_source import (
    SparkDataFrameMetadataTablesQuery,
    SparkDataFrameSqlDialect,
)


def _query(use_catalog: bool) -> SparkDataFrameMetadataTablesQuery:
    return SparkDataFrameMetadataTablesQuery(
        sql_dialect=SparkDataFrameSqlDialect(use_catalog=use_catalog), data_source_connection=None
    )


def test_catalog_mode_qualifies_show_tables_with_the_catalog():
    sql = _query(use_catalog=True).build_sql_statement(
        database_name="my_uc_catalog", schema_name="soda_diagnostics", object_type_to_fetch=TableType.TABLE
    )
    assert sql == "SHOW TABLES FROM `my_uc_catalog`.`soda_diagnostics`"


def test_legacy_mode_has_no_catalog_and_stays_unqualified():
    # get_database_prefix_index() is None in legacy mode, so database_name is always None.
    sql = _query(use_catalog=False).build_sql_statement(
        database_name=None, schema_name="soda_diagnostics", object_type_to_fetch=TableType.TABLE
    )
    assert sql == "SHOW TABLES FROM `soda_diagnostics`"
