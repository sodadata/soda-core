from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_redshift.common.data_sources.redshift_data_source import RedshiftSqlDialect


def test_random():
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'
