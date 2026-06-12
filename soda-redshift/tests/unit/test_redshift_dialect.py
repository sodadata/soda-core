from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_redshift.common.data_sources.redshift_data_source import RedshiftSqlDialect


def test_random():
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


def test_max_sql_statement_length_respects_redshift_16mb_cap():
    # Redshift's documented statement cap is 16 MB; the dialect reserves
    # 1 MB for multi-byte characters (we count chars, the server counts bytes).
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    assert sql_dialect.get_max_sql_statement_length() == 15 * 1024 * 1024
    assert sql_dialect.get_max_sql_statement_length() < 16 * 1024 * 1024
