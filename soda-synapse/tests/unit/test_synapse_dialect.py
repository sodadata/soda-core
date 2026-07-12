from soda_core.common.sql_ast import COUNT, STAR
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_synapse.common.data_sources.synapse_data_source import SynapseSqlDialect


def test_random():
    sql_dialect: SynapseSqlDialect = SynapseSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"


def test_count_renders_as_count_big():
    # Inherits the COUNT_BIG override from SqlServerSqlDialect — guards against accidental regressions.
    assert SynapseSqlDialect().build_expression_sql(COUNT(STAR())) == "COUNT_BIG(*)"


# ---------------------------------------------------------------------------
# Time-bucket + percentile seams. DATEDIFF/DATEADD and the typed DATETIME2
# literal are inherited from SqlServerSqlDialect; percentile is not available
# on Synapse dedicated SQL pools (APPROX_PERCENTILE_DISC's applies-to list
# excludes Synapse).
# ---------------------------------------------------------------------------


def test_supports_percentile_within_group_is_false():
    assert SynapseSqlDialect().supports_percentile_within_group() is False


def test_time_delta_inherits_sqlserver_datediff_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = SynapseSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("[ts]"), "days", 1)
    )
    assert sql == "DATEDIFF(second, '2020-06-20T00:00:00.000', ([ts])) / 86400"


def test_add_interval_inherits_sqlserver_dateadd_form():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = SynapseSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "DATEADD(DAY, ((soda_partition__ + 1) * 1), '2020-06-20T00:00:00.000')"


def test_literal_timestamp_typed_inherits_datetime2_cast():
    from datetime import datetime

    assert (
        SynapseSqlDialect().literal_timestamp_typed(datetime(2020, 6, 20, 1, 2, 3))
        == "CAST('2020-06-20 01:02:03' AS DATETIME2)"
    )
