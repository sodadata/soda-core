from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_athena.common.data_sources.athena_data_source import AthenaSqlDialect


def test_random():
    sql_dialect: AthenaSqlDialect = AthenaSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'
