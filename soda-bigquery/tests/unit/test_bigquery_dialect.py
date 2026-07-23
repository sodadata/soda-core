import pytest
from soda_bigquery.common.data_sources.bigquery_data_source import BigQuerySqlDialect
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT


def test_random():
    sql_dialect: BigQuerySqlDialect = BigQuerySqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"


def test_create_schema_if_not_exists_sql_is_project_qualified():
    """CREATE SCHEMA must be qualified with the project (prefixes[0]) so the
    dataset lands in the intended project — the storage project in a split compute/storage
    setup — rather than the client's default (compute) project. The base implementation
    qualifies with the dataset name only, which created the diagnostics dataset in the wrong
    project and made every diagnostics-warehouse write fail."""
    dialect = BigQuerySqlDialect()
    sql = dialect.create_schema_if_not_exists_sql(["mm-storage-project", "soda_diagnostics"], add_semicolon=False)
    assert sql == "CREATE SCHEMA IF NOT EXISTS `mm-storage-project`.`soda_diagnostics`"


def test_create_schema_if_not_exists_sql_default_appends_semicolon():
    dialect = BigQuerySqlDialect()
    assert dialect.create_schema_if_not_exists_sql(["proj", "ds"]).endswith("`proj`.`ds`;")


def test_create_schema_if_not_exists_sql_raises_on_unexpected_prefix_count():
    """Prefixes must be [project, dataset]; any other shape is a caller bug and must raise a
    clear error rather than silently crashing on the tuple unpack."""
    dialect = BigQuerySqlDialect()
    for bad_prefixes in (["only_one_prefix"], ["a", "b", "c"]):
        with pytest.raises(ValueError, match="prefixes"):
            dialect.create_schema_if_not_exists_sql(bad_prefixes)


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


def test_union_renders_as_union_distinct():
    # Plain UNION must keep set semantics: BigQuery rejects bare UNION, so it
    # renders as UNION DISTINCT (not UNION ALL, which would silently keep dups).
    from soda_core.common.sql_ast import UNION

    dialect = BigQuerySqlDialect()
    sql = dialect.build_union_sql(_make_union(UNION), add_semicolon=False)
    assert "\nUNION DISTINCT\n" in sql
    assert "\nUNION ALL\n" not in sql


def test_union_all_still_renders_union_all():
    from soda_core.common.sql_ast import UNION_ALL

    dialect = BigQuerySqlDialect()
    sql = dialect.build_union_sql(_make_union(UNION_ALL), add_semicolon=False)
    assert "\nUNION ALL\n" in sql
    assert "\nUNION DISTINCT\n" not in sql


def test_get_large_numeric_cast_type_name_is_numeric():
    assert BigQuerySqlDialect().get_large_numeric_cast_type_name() == "NUMERIC"


def test_sql_expr_timestamp_coerce_wraps_in_timestamp():
    assert BigQuerySqlDialect().sql_expr_timestamp_coerce("`dt`") == "timestamp(`dt`)"


# ---------------------------------------------------------------------------
# PERCENTILE_WITHIN_GROUP — BigQuery has no ordered-set aggregates; rendered
# as APPROX_QUANTILES({expr}, 1000)[{int(p*1000)}].
# ---------------------------------------------------------------------------


def test_percentile_within_group_renders_approx_quantiles_q1():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = BigQuerySqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.25))
    assert sql == "APPROX_QUANTILES(`c`, 1000)[250]"


def test_percentile_within_group_renders_approx_quantiles_median():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = BigQuerySqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.5))
    assert sql == "APPROX_QUANTILES(`c`, 1000)[500]"


def test_percentile_within_group_renders_approx_quantiles_q3():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = BigQuerySqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.75))
    assert sql == "APPROX_QUANTILES(`c`, 1000)[750]"


# ---------------------------------------------------------------------------
# TIME_DELTA / ADD_INTERVAL — metric-monitoring time-bucket nodes:
# TIMESTAMP_DIFF with singular unit names + CAST(FLOOR(../count) AS INT) when
# count != 1; TIMESTAMP_ADD with an INTERVAL taking the count expression.
# ---------------------------------------------------------------------------


def test_time_delta_renders_timestamp_diff_singular_unit():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = BigQuerySqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("`ts`"), "days", 1)
    )
    assert sql == "TIMESTAMP_DIFF((`ts`), '2020-06-20T00:00:00', DAY)"


def test_time_delta_timestamp_diff_count_2_wraps_cast_floor():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = BigQuerySqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("`ts`"), "hours", 2)
    )
    assert sql == "CAST(FLOOR(TIMESTAMP_DIFF((`ts`), '2020-06-20T00:00:00', HOUR) / 2) AS INT)"


def test_add_interval_renders_timestamp_add():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = BigQuerySqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "TIMESTAMP_ADD('2020-06-20T00:00:00', INTERVAL ((soda_partition__ + 1) * 1) DAY)"


def test_add_interval_weeks_renders_as_days_times_seven():
    # BigQuery TIMESTAMP_ADD accepts only MICROSECOND..DAY parts; WEEK is invalid
    # there (though valid for TIMESTAMP_DIFF), so weekly buckets must be expressed
    # as INTERVAL <count> * 7 DAY.
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = BigQuerySqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "weeks", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "TIMESTAMP_ADD('2020-06-20T00:00:00', INTERVAL ((soda_partition__ + 1) * 1) * 7 DAY)"


def test_time_delta_weeks_keeps_week_part():
    # The TIMESTAMP_DIFF path DOES accept WEEK; it must stay unchanged.
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = BigQuerySqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("`ts`"), "weeks", 1)
    )
    assert sql == "TIMESTAMP_DIFF((`ts`), '2020-06-20T00:00:00', WEEK)"


# ---------------------------------------------------------------------------
# BigQuery string literal escaping — literal_string wraps values in triple
# single quotes ('''...'''). If a bare ''' survives inside the content it
# prematurely closes the string and BigQuery rejects the statement with
# "Syntax error: concatenated string literals must be separated by whitespace
# or comments". This happened for values ending in a quote (boundary merges
# with the closing ''') and for internal runs of 4/5/7/8 quotes. Regression
# for the AST-fallback check-result INSERT reported for BigQuery.
# ---------------------------------------------------------------------------


def _decode_bq_triple_quoted(sql_literal: str) -> tuple[bool, str]:
    """Tokenise a BigQuery '''...''' string literal the way BigQuery does.

    A backslash escapes the next character; a ``'''`` closes the string.
    Returns ``(closes_exactly_at_end, decoded_value)`` — ``closes_exactly_at_end``
    is False when the literal never closes OR closes before the final character
    (i.e. trailing content would be parsed as a second, adjacent literal).
    """
    assert sql_literal.startswith("'''") and len(sql_literal) >= 6
    escapes = {"n": "\n", "t": "\t", "r": "\r"}
    i, n, out = 3, len(sql_literal), []
    while i < n:
        c = sql_literal[i]
        if c == "\\":
            nxt = sql_literal[i + 1] if i + 1 < n else ""
            out.append(escapes.get(nxt, nxt))
            i += 2
            continue
        if c == "'" and sql_literal[i : i + 3] == "'''":
            return (i + 3 == n, "".join(out))
        out.append(c)
        i += 1
    return (False, "".join(out))


@pytest.mark.parametrize(
    "value",
    [
        "",
        "no quotes here",
        "'",
        "''",
        "'''",
        "abc'",  # trailing single quote -> boundary '''' before the fix
        "abc''",  # trailing double quote -> boundary ''''' before the fix
        "'abc",  # leading quote
        "SELECT 'x'",  # ends in a quoted literal (the common customer trigger)
        "x'y",
        "x''y",
        "x'''y",
        "x''''y",  # internal run of 4 -> broke before the fix
        "x'''''y",  # 5
        "x''''''y",  # 6
        "x'''''''y",  # 7
        "x''''''''y",  # 8
        "multi\nline 'quoted'\nvalue",  # multiline must survive the triple-quote wrapper
        "back\\slash and 'quote'",  # backslash + quote
        "WITH a AS (SELECT 'Zakenadres') SELECT 'missing'",  # customer-style query
    ],
)
def test_literal_string_is_well_formed_and_roundtrips(value: str):
    dialect = BigQuerySqlDialect()
    sql_literal = dialect.literal_string(value)
    closes_at_end, decoded = _decode_bq_triple_quoted(sql_literal)
    assert closes_at_end, f"literal not closed exactly at end (premature '''): {sql_literal!r}"
    assert decoded == value, f"round-trip mismatch: {decoded!r} != {value!r} (literal {sql_literal!r})"


def test_literal_string_embedded_scan_id_query_roundtrips():
    """A failed-rows query that itself embeds a scan-id literal ('''...''') is a
    real check-result value; wrapping it again must stay well-formed."""
    dialect = BigQuerySqlDialect()
    scan_id = "481214e3-a379-49f2-9edf-87fdb0d4e73b"
    inner = dialect.literal_string(scan_id)  # '''481214...'''
    failed_rows_query = f"SELECT * FROM `t` AS `FAILED_ROWS` WHERE `__soda_scan_id` = {inner};"
    sql_literal = dialect.literal_string(failed_rows_query)
    closes_at_end, decoded = _decode_bq_triple_quoted(sql_literal)
    assert closes_at_end, f"premature ''' in {sql_literal!r}"
    assert decoded == failed_rows_query


def test_literal_string_none_returns_none():
    assert BigQuerySqlDialect().literal_string(None) is None
