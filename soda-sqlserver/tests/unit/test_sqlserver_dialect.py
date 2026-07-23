from datetime import date

from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import COUNT, CREATE_TABLE_COLUMN, STAR
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_sqlserver.common.data_sources.sqlserver_data_source import SqlServerSqlDialect


def test_random():
    sql_dialect: SqlServerSqlDialect = SqlServerSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"


def test_literal_date_pads_year_to_four_digits():
    """C strftime("%Y") drops leading zeros for years < 1000 on glibc; the CAST
    string must always carry a 4-digit, zero-padded year."""
    assert SqlServerSqlDialect().literal(date(200, 12, 17)) == "CAST('0200-12-17' AS DATE)"


def test_count_renders_as_count_big():
    # T-SQL COUNT returns INT and overflows above 2.1B rows; the dialect must emit COUNT_BIG.
    assert SqlServerSqlDialect().build_expression_sql(COUNT(STAR())) == "COUNT_BIG(*)"


# ---------------------------------------------------------------------------
# Datetime precision clamp — SQL Server's datetime2/time/datetimeoffset cap at 7.
# Cross-source flows from sources with higher native precision (e.g. Snowflake's
# TIMESTAMP_NTZ defaults to 9) used to emit e.g. `datetime2(9)` and fail CREATE
# TABLE; the dialect override clamps anything > 7 down to 7.
# ---------------------------------------------------------------------------


def test_datetime2_precision_above_max_clamped_to_7():
    dialect = SqlServerSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2", datetime_precision=9))
    assert dialect._build_create_table_column_type(column) == "datetime2(7)"


def test_datetimeoffset_precision_above_max_clamped_to_7():
    dialect = SqlServerSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetimeoffset", datetime_precision=9))
    assert dialect._build_create_table_column_type(column) == "datetimeoffset(7)"


def test_time_precision_above_max_clamped_to_7():
    dialect = SqlServerSqlDialect()
    column = CREATE_TABLE_COLUMN(name="t", type=SqlDataType(name="time", datetime_precision=9))
    assert dialect._build_create_table_column_type(column) == "time(7)"


def test_datetime2_precision_at_or_below_max_passes_through():
    """Precision ≤ 7 must be preserved verbatim — the clamp is upper-bound only."""
    dialect = SqlServerSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2", datetime_precision=4))
    assert dialect._build_create_table_column_type(column) == "datetime2(4)"


def test_datetime2_precision_exactly_max_is_no_op():
    dialect = SqlServerSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2", datetime_precision=7))
    assert dialect._build_create_table_column_type(column) == "datetime2(7)"


def test_clamp_skips_non_datetime_types():
    """Only datetime2 / datetimeoffset / time are clamped; an int column with
    a stray datetime_precision must not have it inserted into the DDL."""
    dialect = SqlServerSqlDialect()
    column = CREATE_TABLE_COLUMN(name="age", type=SqlDataType(name="int", datetime_precision=9))
    # The base strip pass zeroes datetime_precision on non-datetime types before render.
    assert dialect._build_create_table_column_type(column) == "int"


# ---------------------------------------------------------------------------
# TIME_DELTA / ADD_INTERVAL — MM time-bucket nodes.
# DATEDIFF counts crossed boundaries of the given unit, so the dialect
# computes in SECONDS and divides by the seconds-per-interval; add-interval
# is DATEADD.
# ---------------------------------------------------------------------------


def test_time_delta_renders_datediff_seconds_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = SqlServerSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("[ts]"), "days", 1)
    )
    assert sql == "(DATEDIFF(second, '2020-06-20T00:00:00.000', ([ts])) / 86400)"


def test_time_delta_datediff_count_2_hours():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = SqlServerSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("[ts]"), "hours", 2)
    )
    assert sql == "(DATEDIFF(second, '2020-06-20T00:00:00.000', ([ts])) / 7200)"


def test_add_interval_renders_dateadd_singular_unit():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = SqlServerSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "DATEADD(DAY, ((soda_partition__ + 1) * 1), '2020-06-20T00:00:00.000')"


def test_add_interval_weeks_unit_name():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = SqlServerSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "weeks", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "DATEADD(WEEK, ((soda_partition__ + 1) * 1), '2020-06-20T00:00:00.000')"


# ---------------------------------------------------------------------------
# PERCENTILE_WITHIN_GROUP — T-SQL PERCENTILE_DISC is window-only; the
# aggregate form is APPROX_PERCENTILE_DISC.
# ---------------------------------------------------------------------------


def test_percentile_within_group_renders_approx_percentile_disc():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = SqlServerSqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.25))
    assert sql == "APPROX_PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY [c])"


def test_supports_percentile_within_group_defaults_true_when_unprobed():
    # A dialect with no live connection (pure SQL rendering) hasn't probed the
    # engine; it assumes support to preserve rendering behavior.
    assert SqlServerSqlDialect().supports_percentile_within_group() is True


def test_supports_percentile_within_group_reflects_injected_capability():
    # APPROX_PERCENTILE_DISC needs SQL Server 2022+/Azure; the data source probes
    # the engine on connect and injects the result into the dialect seam.
    dialect = SqlServerSqlDialect()
    dialect.set_approx_percentile_disc_supported(False)
    assert dialect.supports_percentile_within_group() is False
    dialect.set_approx_percentile_disc_supported(True)
    assert dialect.supports_percentile_within_group() is True


def test_engine_supports_approx_percentile_disc_matrix():
    from soda_sqlserver.common.data_sources.sqlserver_data_source import (
        SqlServerDataSourceImpl,
    )

    f = SqlServerDataSourceImpl._engine_supports_approx_percentile_disc
    assert f(16, 3) is True  # SQL Server 2022, Enterprise (on-prem)
    assert f(17, 2) is True  # future major, Standard
    assert f(15, 3) is False  # SQL Server 2019 — no aggregate APPROX_PERCENTILE_DISC
    assert f(12, 5) is True  # Azure SQL Database (reports legacy major 12)
    assert f(12, 8) is True  # Azure SQL Managed Instance
    assert f(15, 4) is False  # SQL Server 2019 Express
    assert f(None, None) is False  # unknown engine — be safe


# ---------------------------------------------------------------------------
# literal_timestamp_typed — T-SQL has no TIMESTAMP '...' literal (TIMESTAMP is
# the deprecated rowversion type); CAST(... AS DATETIME2) is the typed form.
# ---------------------------------------------------------------------------


def test_literal_timestamp_typed_casts_to_datetime2():
    from datetime import datetime

    sql = SqlServerSqlDialect().literal_timestamp_typed(datetime(2020, 6, 20, 1, 2, 3))
    assert sql == "CAST('2020-06-20 01:02:03' AS DATETIME2)"


def test_literal_timestamp_typed_truncates_sub_seconds():
    from datetime import datetime

    sql = SqlServerSqlDialect().literal_timestamp_typed(datetime(2020, 6, 20, 1, 2, 3, 999999))
    assert sql == "CAST('2020-06-20 01:02:03' AS DATETIME2)"


def test_literal_timestamp_typed_normalizes_tz_aware_to_utc():
    # A tz-aware non-UTC datetime must render its UTC wall-clock, not local time
    # with the zone silently dropped.
    from datetime import datetime, timedelta, timezone

    plus_two = timezone(timedelta(hours=2))
    sql = SqlServerSqlDialect().literal_timestamp_typed(datetime(2020, 6, 20, 3, 2, 3, tzinfo=plus_two))
    assert sql == "CAST('2020-06-20 01:02:03' AS DATETIME2)"
