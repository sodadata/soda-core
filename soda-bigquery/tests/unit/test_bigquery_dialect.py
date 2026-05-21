from soda_bigquery.common.data_sources.bigquery_data_source import BigQuerySqlDialect
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT


def test_random():
    sql_dialect: BigQuerySqlDialect = BigQuerySqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"


# ---------------------------------------------------------------------------
# BigQuery NUMERIC / BIGNUMERIC precision & scale — INFORMATION_SCHEMA.COLUMNS
# doesn't expose precision/scale for either type, so the dialect hardcodes the
# documented fixed values. Without these, cross-source CREATE TABLE on target
# dialects that *do* require precision/scale would default to scale 0 and
# silently truncate fractional values.
# ---------------------------------------------------------------------------


def test_extract_numeric_precision_for_numeric_returns_38():
    """NUMERIC is documented as NUMERIC(38, 9)."""
    dialect = BigQuerySqlDialect()
    row = ("col", "numeric")
    assert dialect.extract_numeric_precision(row, columns=[]) == 38


def test_extract_numeric_precision_for_bignumeric_returns_76():
    """BIGNUMERIC is documented as BIGNUMERIC(76, 38)."""
    dialect = BigQuerySqlDialect()
    row = ("col", "bignumeric")
    assert dialect.extract_numeric_precision(row, columns=[]) == 76


def test_extract_numeric_scale_for_numeric_returns_9():
    dialect = BigQuerySqlDialect()
    row = ("col", "numeric")
    assert dialect.extract_numeric_scale(row, columns=[]) == 9


def test_extract_numeric_scale_for_bignumeric_returns_38():
    dialect = BigQuerySqlDialect()
    row = ("col", "bignumeric")
    assert dialect.extract_numeric_scale(row, columns=[]) == 38


def test_extract_numeric_precision_case_insensitive():
    """INFORMATION_SCHEMA reports types upper-case; the override lowercases first."""
    dialect = BigQuerySqlDialect()
    row = ("col", "NUMERIC")
    assert dialect.extract_numeric_precision(row, columns=[]) == 38
    row = ("col", "BIGNUMERIC")
    assert dialect.extract_numeric_precision(row, columns=[]) == 76
