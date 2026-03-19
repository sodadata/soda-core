from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_sqlserver.common.data_sources.sqlserver_data_source import SqlServerSqlDialect


def test_random():
    sql_dialect: SqlServerSqlDialect = SqlServerSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM [a];"
