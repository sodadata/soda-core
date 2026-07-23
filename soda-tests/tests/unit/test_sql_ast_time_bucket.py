"""TIME_DELTA + ADD_INTERVAL time-bucket node rendering (metric monitoring
bulk SQL, start-anchored interval windows).

- ``TIME_DELTA(start, end, unit, count)`` — the partition-index expression.
  Base renderer is the epoch-floor form
  ``FLOOR(EXTRACT(EPOCH FROM {end} - {start}) / {seconds_per_interval})``,
  valid on postgres/duckdb and unit-safe because every supported unit
  (weeks/days/hours/seconds) is fixed-length.
  Overrides: snowflake ``FLOOR(TIMESTAMPDIFF(second, {start}, {end}) / {multiplier})``,
  bigquery ``TIMESTAMP_DIFF({end}, {start}, {UNIT})`` + ``CAST(FLOOR(../count) AS INT)``
  when count != 1 — tested in the respective package unit suites.

- ``ADD_INTERVAL(timestamp, unit, count_expression)`` — scan_time reconstruction,
  ``{ts} + INTERVAL '1 {unit}' * {count_expr}``; the paren pair around the count
  comes from the count expression's own rendering (SqlExpressionStr parenthesizes).
"""

from __future__ import annotations

from datetime import datetime

import pytest
from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, TIME_DELTA, SqlExpressionStr
from soda_core.common.sql_dialect import SqlDialect

START = datetime(2020, 6, 20, 0, 0, 0)
START_LITERAL = "'2020-06-20T00:00:00'"


def dialect() -> SqlDialect:
    return SqlDialect()


# ---------------------------------------------------------------------------
# TIME_DELTA — base epoch-floor renderer
# ---------------------------------------------------------------------------


def test_time_delta_days_count_1():
    sql = dialect().build_expression_sql(TIME_DELTA(LITERAL(START), SqlExpressionStr('"ts"'), "days", 1))
    assert sql == f'FLOOR(EXTRACT(EPOCH FROM ("ts") - {START_LITERAL}) / 86400)'


def test_time_delta_hours_count_2():
    sql = dialect().build_expression_sql(TIME_DELTA(LITERAL(START), SqlExpressionStr('"ts"'), "hours", 2))
    assert sql == f'FLOOR(EXTRACT(EPOCH FROM ("ts") - {START_LITERAL}) / 7200)'


def test_time_delta_weeks_count_1():
    sql = dialect().build_expression_sql(TIME_DELTA(LITERAL(START), SqlExpressionStr('"ts"'), "weeks", 1))
    assert sql == f'FLOOR(EXTRACT(EPOCH FROM ("ts") - {START_LITERAL}) / 604800)'


def test_time_delta_seconds_count_1():
    sql = dialect().build_expression_sql(TIME_DELTA(LITERAL(START), SqlExpressionStr('"ts"'), "seconds", 1))
    assert sql == f'FLOOR(EXTRACT(EPOCH FROM ("ts") - {START_LITERAL}) / 1)'


def test_time_delta_rejects_unknown_unit():
    with pytest.raises(ValueError, match="unit"):
        TIME_DELTA(LITERAL(START), SqlExpressionStr('"ts"'), "months", 1)


def test_time_delta_bare_str_end_is_quoted():
    """Bare str expressions are quoted as identifiers, like every other node."""
    sql = dialect().build_expression_sql(TIME_DELTA(LITERAL(START), "ts", "days", 1))
    assert sql == f'FLOOR(EXTRACT(EPOCH FROM "ts" - {START_LITERAL}) / 86400)'


# ---------------------------------------------------------------------------
# ADD_INTERVAL — base interval-multiply renderer
# ---------------------------------------------------------------------------


def test_add_interval_with_expression_count():
    """The scan_time reconstruction shape of the metric-monitoring bulk CTE:
    parens around the count come from the SqlExpressionStr rendering."""
    sql = dialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(START), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == f"{START_LITERAL} + INTERVAL '1 days' * ((soda_partition__ + 1) * 1)"


def test_add_interval_hours():
    sql = dialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(START), "hours", SqlExpressionStr("(soda_partition__ + 1) * 4"))
    )
    assert sql == f"{START_LITERAL} + INTERVAL '1 hours' * ((soda_partition__ + 1) * 4)"


def test_add_interval_weeks():
    sql = dialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(START), "weeks", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == f"{START_LITERAL} + INTERVAL '1 weeks' * ((soda_partition__ + 1) * 1)"


def test_add_interval_rejects_unknown_unit():
    with pytest.raises(ValueError, match="unit"):
        ADD_INTERVAL(LITERAL(START), "months", SqlExpressionStr("1"))


def test_add_interval_supports_alias():
    """The metric-monitoring bulk CTE aliases the reconstruction ``AS scan_time``."""
    sql = dialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(START), "days", SqlExpressionStr("(soda_partition__ + 1) * 1")).AS("scan_time")
    )
    assert sql == (f"{START_LITERAL} + INTERVAL '1 days' * ((soda_partition__ + 1) * 1)" ' AS "scan_time"')
