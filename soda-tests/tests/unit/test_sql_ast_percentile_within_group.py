"""PERCENTILE_WITHIN_GROUP ordered-set aggregate rendering.

The base renderer emits ``PERCENTILE_DISC({p}) WITHIN GROUP (ORDER BY {expr})``,
valid on postgres/duckdb/snowflake. BigQuery overrides with
``APPROX_QUANTILES({expr}, 1000)[{int(p*1000)}]`` — tested in soda-bigquery's
unit tests.
"""

from __future__ import annotations

from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP, SqlExpressionStr
from soda_core.common.sql_dialect import SqlDialect


def dialect() -> SqlDialect:
    return SqlDialect()


def test_percentile_within_group_q1():
    sql = dialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.25))
    assert sql == 'PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY "c")'


def test_percentile_within_group_median():
    sql = dialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.5))
    assert sql == 'PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY "c")'


def test_percentile_within_group_q3():
    sql = dialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.75))
    assert sql == 'PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY "c")'


def test_percentile_within_group_bare_str_is_quoted():
    """Bare str expressions are quoted as identifiers, like every other node."""
    sql = dialect().build_expression_sql(PERCENTILE_WITHIN_GROUP("c", 0.5))
    assert sql == 'PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY "c")'


def test_percentile_within_group_raw_sql_expression():
    """SqlExpressionStr leaves render raw (parenthesized) — the raw-SQL escape hatch."""
    sql = dialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(SqlExpressionStr("a + b"), 0.5))
    assert sql == "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY (a + b))"


def test_percentile_within_group_supports_alias():
    sql = dialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.25).AS("q1"))
    assert sql == 'PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY "c") AS "q1"'
