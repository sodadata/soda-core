from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import COUNT, CREATE_TABLE_COLUMN, STAR
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_fabric.common.data_sources.fabric_data_source import FabricSqlDialect


def test_random():
    sql_dialect: FabricSqlDialect = FabricSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"


def test_count_renders_as_count_big():
    # Inherits the COUNT_BIG override from SqlServerSqlDialect — guards against accidental regressions.
    assert FabricSqlDialect().build_expression_sql(COUNT(STAR())) == "COUNT_BIG(*)"


# ---------------------------------------------------------------------------
# Datetime precision default + clamp — Fabric's datetime2 / time / datetimeoffset
# cap at 6 (one less than SQL Server). datetime2 / time without an explicit
# precision are defaulted to 6 so the rendered DDL is `datetime2(6)` /
# `time(6)`; precision > 6 is clamped down to 6.
# ---------------------------------------------------------------------------


def test_datetime2_with_no_precision_defaults_to_6():
    """Fabric requires an explicit fractional-seconds precision on datetime2;
    if the source omits it, the dialect injects 6 (Fabric's max)."""
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2"))
    assert dialect._build_create_table_column_type(column) == "datetime2(6)"


def test_time_with_no_precision_defaults_to_6():
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="t", type=SqlDataType(name="time"))
    assert dialect._build_create_table_column_type(column) == "time(6)"


def test_datetime2_precision_above_max_clamped_to_6():
    """Cross-source flows from sources with native precision > 6 (e.g. SQL Server's
    datetime2(7), Snowflake's TIMESTAMP_NTZ default of 9) must clamp down to
    Fabric's max so CREATE TABLE doesn't fail."""
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2", datetime_precision=9))
    assert dialect._build_create_table_column_type(column) == "datetime2(6)"


def test_datetimeoffset_precision_above_max_clamped_to_6():
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetimeoffset", datetime_precision=9))
    assert dialect._build_create_table_column_type(column) == "datetimeoffset(6)"


def test_time_precision_above_max_clamped_to_6():
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="t", type=SqlDataType(name="time", datetime_precision=9))
    assert dialect._build_create_table_column_type(column) == "time(6)"


def test_datetime2_precision_at_or_below_max_passes_through():
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2", datetime_precision=3))
    assert dialect._build_create_table_column_type(column) == "datetime2(3)"


def test_datetime2_precision_exactly_max_is_no_op():
    dialect = FabricSqlDialect()
    column = CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="datetime2", datetime_precision=6))
    assert dialect._build_create_table_column_type(column) == "datetime2(6)"
