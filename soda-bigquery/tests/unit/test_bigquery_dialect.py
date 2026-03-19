from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_bigquery.common.data_sources.bigquery_data_source import BigQuerySqlDialect


def test_random():
    sql_dialect: BigQuerySqlDialect = BigQuerySqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"
