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


# ---------------------------------------------------------------------------
# Soda-type reverse map coverage (OBSL-1005).
#
# BigQuery DATETIME and BIGNUMERIC columns are real, reachable metadata type
# names (v3 profiled both: soda-library bigquery_data_source.py:133,:138) but
# were missing from get_soda_data_type_name_by_data_source_data_type_names —
# consumers doing a raw map lookup (profiling classification, the partition
# detector's is_date_like_column, DWH reverse-map lookups) silently skipped
# those columns. The remaining aliases (int/integer/bigint/tinyint/decimal/
# bigdecimal/boolean) are unreachable via INFORMATION_SCHEMA — BigQuery
# canonicalizes them to INT64/NUMERIC/BIGNUMERIC/BOOL — kept as defensive
# entries mirroring the dialect's own _get_data_type_name_synonyms.
# ---------------------------------------------------------------------------


def test_reverse_map_covers_datetime_and_bignumeric():
    from soda_core.common.metadata_types import SodaDataTypeName

    dialect = BigQuerySqlDialect()
    reverse_map = dialect.get_soda_data_type_name_by_data_source_data_type_names()

    # BigQuery DATETIME is a naive (timezone-less) timestamp → canonical TIMESTAMP.
    assert reverse_map["datetime"] == SodaDataTypeName.TIMESTAMP
    assert reverse_map["bignumeric"] == SodaDataTypeName.NUMERIC


def test_reverse_map_defensive_aliases():
    from soda_core.common.metadata_types import SodaDataTypeName

    dialect = BigQuerySqlDialect()
    reverse_map = dialect.get_soda_data_type_name_by_data_source_data_type_names()
    assert reverse_map["int"] == SodaDataTypeName.BIGINT
    assert reverse_map["integer"] == SodaDataTypeName.BIGINT
    assert reverse_map["bigint"] == SodaDataTypeName.BIGINT
    assert reverse_map["tinyint"] == SodaDataTypeName.SMALLINT
    assert reverse_map["decimal"] == SodaDataTypeName.NUMERIC
    assert reverse_map["bigdecimal"] == SodaDataTypeName.NUMERIC
    assert reverse_map["boolean"] == SodaDataTypeName.BOOLEAN


# ---------------------------------------------------------------------------
# format_metadata_data_type parameter stripping (OBSL-1005).
#
# BigQuery INFORMATION_SCHEMA reports parameterized columns WITH the suffix
# (e.g. "NUMERIC(20, 4)", "STRING(10)") and, unlike DuckDB
# (duckdb_data_source.py format_metadata_data_type), BigQuery inherited the
# base identity implementation (soda_core sql_dialect.py) — so
# sql_data_type.name could be "numeric(20, 4)" and miss every map lookup.
# ---------------------------------------------------------------------------


def test_format_metadata_data_type_strips_parameters():
    dialect = BigQuerySqlDialect()
    assert dialect.format_metadata_data_type("numeric(20, 4)") == "numeric"
    assert dialect.format_metadata_data_type("NUMERIC(20, 4)") == "NUMERIC"
    assert dialect.format_metadata_data_type("string(10)") == "string"
    assert dialect.format_metadata_data_type("bignumeric(40, 10)") == "bignumeric"


def test_format_metadata_data_type_passes_bare_names_through():
    dialect = BigQuerySqlDialect()
    assert dialect.format_metadata_data_type("int64") == "int64"
    assert dialect.format_metadata_data_type("string") == "string"
    assert dialect.format_metadata_data_type("struct<a int64>") == "struct<a int64>"
