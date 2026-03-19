from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_synapse.common.data_sources.synapse_data_source import SynapseSqlDialect


def test_random():
    sql_dialect: SynapseSqlDialect = SynapseSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"
