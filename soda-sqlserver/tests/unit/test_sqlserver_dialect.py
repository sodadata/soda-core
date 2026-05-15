from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import CREATE_TABLE_COLUMN
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_sqlserver.common.data_sources.sqlserver_data_source import SqlServerSqlDialect


def test_random():
    sql_dialect: SqlServerSqlDialect = SqlServerSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"


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
