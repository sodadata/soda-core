from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_sparkdf.common.data_sources.sparkdf_data_source import (
    SparkDataFrameSqlDialect,
)


def test_random():
    sql_dialect: SparkDataFrameSqlDialect = SparkDataFrameSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"


def test_legacy_mode_prefix_indexes_unchanged():
    dialect = SparkDataFrameSqlDialect()
    assert dialect.use_catalog is False
    assert dialect.get_database_prefix_index() is None
    assert dialect.get_schema_prefix_index() == 0


def test_catalog_mode_prefix_indexes():
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    assert dialect.use_catalog is True
    assert dialect.get_database_prefix_index() == 0
    assert dialect.get_schema_prefix_index() == 1


def test_legacy_create_schema_sql_uses_single_prefix():
    dialect = SparkDataFrameSqlDialect()
    sql = dialect.create_schema_if_not_exists_sql(["my_schema"])
    assert sql == "CREATE SCHEMA IF NOT EXISTS `my_schema`;"


def test_catalog_mode_create_schema_sql_qualifies_with_catalog():
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    sql = dialect.create_schema_if_not_exists_sql(["my_cat", "my_schema"])
    assert sql == "CREATE SCHEMA IF NOT EXISTS `my_cat`.`my_schema`;"


def test_catalog_mode_create_schema_sql_no_semicolon_option():
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    sql = dialect.create_schema_if_not_exists_sql(["my_cat", "my_schema"], add_semicolon=False)
    assert sql == "CREATE SCHEMA IF NOT EXISTS `my_cat`.`my_schema`"


def test_drop_table_cascade_disabled_in_legacy_mode():
    # Local Spark rejects DROP TABLE ... CASCADE with a ParseException, so the dialect
    # must NOT emit the CASCADE suffix when running on a local SparkSession.
    dialect = SparkDataFrameSqlDialect()
    assert dialect.SUPPORTS_DROP_TABLE_CASCADE is False


def test_drop_table_cascade_enabled_in_catalog_mode():
    # In catalog mode the engine is Databricks (or another UC-aware Spark), which accepts
    # DROP TABLE ... CASCADE. The dialect promotes the class default to True per-instance.
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    assert dialect.SUPPORTS_DROP_TABLE_CASCADE is True


# ---------------------------------------------------------------------------
# Time-bucket / percentile / NaN seams — all inherited from
# DatabricksSqlDialect (3-arg DATEDIFF, TIMESTAMPADD and percentile_disc
# WITHIN GROUP are OSS Spark 3.3/3.4+ syntax too: SPARK-38389, SPARK-38195).
# Pins the inheritance against accidental regressions.
# ---------------------------------------------------------------------------


def test_time_delta_inherits_databricks_datediff_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = SparkDataFrameSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("`ts`"), "days", 1)
    )
    # SparkDf's literal_datetime wraps in to_utc_timestamp(...) — the DATEDIFF
    # shape itself is the inherited Databricks form.
    assert sql == "FLOOR(DATEDIFF(SECOND, to_utc_timestamp('2020-06-20T00:00:00', 'UTC'), (`ts`)) / 86400)"


def test_add_interval_inherits_databricks_timestampadd_form():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = SparkDataFrameSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "TIMESTAMPADD(DAY, ((soda_partition__ + 1) * 1), to_utc_timestamp('2020-06-20T00:00:00', 'UTC'))"


def test_sql_expr_is_not_nan_inherits_not_isnan():
    assert SparkDataFrameSqlDialect().sql_expr_is_not_nan("`c`") == "NOT ISNAN(`c`)"


def test_supports_percentile_within_group_is_true():
    assert SparkDataFrameSqlDialect().supports_percentile_within_group() is True
