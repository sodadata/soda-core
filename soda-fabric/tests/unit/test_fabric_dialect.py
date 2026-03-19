from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_fabric.common.data_sources.fabric_data_source import FabricSqlDialect


def test_random():
    sql_dialect: FabricSqlDialect = FabricSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM [a];"
