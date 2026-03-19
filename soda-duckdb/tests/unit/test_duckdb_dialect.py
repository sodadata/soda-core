from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBSqlDialect


def test_random():
    sql_dialect: DuckDBSqlDialect = DuckDBSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'
