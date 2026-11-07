"""The keys of every dialect's native-to-Soda data type map are lowercase.

Callers look up a `SqlDataType.name`, which is always lowercase, so a key with an uppercase letter
would never match and its type would silently go unmapped.
"""

from __future__ import annotations

import pytest
from soda_core.common.sql_dialect import SqlDialect

# Every dialect class shipped in soda-core, including the ones derived from another, since a derived
# dialect can extend its parent's map.
_DIALECT_CASES = [
    ("athena", "soda_athena.common.data_sources.athena_data_source", "AthenaSqlDialect"),
    ("bigquery", "soda_bigquery.common.data_sources.bigquery_data_source", "BigQuerySqlDialect"),
    ("databricks", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksSqlDialect"),
    ("databricks_hive", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksHiveSqlDialect"),
    ("duckdb", "soda_duckdb.common.data_sources.duckdb_data_source", "DuckDBSqlDialect"),
    ("fabric", "soda_fabric.common.data_sources.fabric_data_source", "FabricSqlDialect"),
    ("postgres", "soda_postgres.common.data_sources.postgres_data_source", "PostgresSqlDialect"),
    ("redshift", "soda_redshift.common.data_sources.redshift_data_source", "RedshiftSqlDialect"),
    ("snowflake", "soda_snowflake.common.data_sources.snowflake_data_source", "SnowflakeSqlDialect"),
    ("sparkdf", "soda_sparkdf.common.data_sources.sparkdf_data_source", "SparkDataFrameSqlDialect"),
    ("sqlserver", "soda_sqlserver.common.data_sources.sqlserver_data_source", "SqlServerSqlDialect"),
    ("synapse", "soda_synapse.common.data_sources.synapse_data_source", "SynapseSqlDialect"),
    ("trino", "soda_trino.common.data_sources.trino_data_source", "TrinoSqlDialect"),
]


@pytest.mark.parametrize(
    "module_path, class_name",
    [case[1:] for case in _DIALECT_CASES],
    ids=[case[0] for case in _DIALECT_CASES],
)
def test_type_name_map_keys_are_lowercase(module_path, class_name):
    sql_dialect: SqlDialect = getattr(pytest.importorskip(module_path), class_name)()

    type_name_map = sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    assert [name for name in type_name_map if name != name.lower()] == []
