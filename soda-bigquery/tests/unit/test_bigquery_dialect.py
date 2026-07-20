import pytest
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
