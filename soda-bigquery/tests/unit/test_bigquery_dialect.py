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


# Soda-type reverse map: DATETIME/BIGNUMERIC are reachable metadata type names;
# raw map consumers (profiling classification, is_date_like_column, DWH reverse
# map) silently skip columns whose type name is missing.


def test_reverse_map_covers_datetime_and_bignumeric():
    from soda_core.common.metadata_types import SodaDataTypeName

    dialect = BigQuerySqlDialect()
    reverse_map = dialect.get_soda_data_type_name_by_data_source_data_type_names()
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


# format_metadata_data_type parameter stripping


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


# UNION rendering (see BigQuerySqlDialect.build_union_sql for the rationale)


def _make_union(node_cls):
    from soda_core.common.sql_dialect import FROM, SELECT, STAR

    return node_cls(
        [
            [SELECT(STAR()), FROM("a")],
            [SELECT(STAR()), FROM("b")],
        ]
    )


def test_union_renders_as_union_all():
    from soda_core.common.sql_ast import UNION

    dialect = BigQuerySqlDialect()
    sql = dialect.build_union_sql(_make_union(UNION), add_semicolon=False)
    assert "\nUNION ALL\n" in sql
    assert "\nUNION\n" not in sql


def test_union_all_still_renders_union_all():
    from soda_core.common.sql_ast import UNION_ALL

    dialect = BigQuerySqlDialect()
    sql = dialect.build_union_sql(_make_union(UNION_ALL), add_semicolon=False)
    assert "\nUNION ALL\n" in sql


def test_get_large_numeric_cast_type_name_is_numeric():
    assert BigQuerySqlDialect().get_large_numeric_cast_type_name() == "NUMERIC"


def test_sql_expr_timestamp_coerce_wraps_in_timestamp():
    assert BigQuerySqlDialect().sql_expr_timestamp_coerce("`dt`") == "timestamp(`dt`)"
