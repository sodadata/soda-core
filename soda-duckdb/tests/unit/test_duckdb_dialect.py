from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_core.common.statements.metadata_primary_keys_query import (
    MetadataPrimaryKeysQuery,
)
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBSqlDialect


def test_random():
    sql_dialect: DuckDBSqlDialect = DuckDBSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


def test_primary_keys_query_uses_ansi_key_column_usage():
    # DuckDB's key_column_usage carries ordinal_position (unlike constraint_column_usage), so the
    # primary-key query uses the ANSI base query to return composite keys in order.
    dialect = DuckDBSqlDialect()
    query = MetadataPrimaryKeysQuery(sql_dialect=dialect, data_source_connection=None)
    namespace = query._build_namespace(["myschema"])
    sql = dialect.build_select_sql(query.build_sql_statement(namespace, ["orders"]))
    assert '"information_schema"."table_constraints"' in sql
    assert '"information_schema"."key_column_usage"' in sql
    assert "ordinal_position" in sql
