from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_duckdb.common.data_sources.duckdb_data_source import (
    DuckDBMetadataPrimaryKeysQuery,
    DuckDBSqlDialect,
)


def test_random():
    sql_dialect: DuckDBSqlDialect = DuckDBSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


def test_primary_keys_query_uses_constraint_column_usage():
    # DuckDB exposes constraint columns through constraint_column_usage, not the ANSI
    # key_column_usage view, so the primary-key query must read from constraint_column_usage.
    dialect = DuckDBSqlDialect()
    query = DuckDBMetadataPrimaryKeysQuery(sql_dialect=dialect, data_source_connection=None)
    namespace = query._build_namespace(["myschema"])
    sql = dialect.build_select_sql(query.build_sql_statement(namespace, ["orders"]))
    assert '"information_schema"."table_constraints"' in sql
    assert '"information_schema"."constraint_column_usage"' in sql
    assert "key_column_usage" not in sql
