from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_sparkdf.common.data_sources.sparkdf_data_source import SparkDataFrameSqlDialect


def test_random():
    sql_dialect: SparkDataFrameSqlDialect = SparkDataFrameSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"
