from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_trino.common.data_sources.trino_data_source import TrinoSqlDialect


def test_random():
    sql_dialect: TrinoSqlDialect = TrinoSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a"'
