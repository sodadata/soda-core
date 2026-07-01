from __future__ import annotations

from soda_core.common.sql_ast import (
    COLUMN,
    CTE,
    FROM,
    ORDER_BY_ASC,
    ORDER_BY_DESC,
    SELECT,
    STAR,
    UNION,
    UNION_ALL,
    WINDOW_FUNCTION,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect


def dialect() -> SqlDialect:
    return SqlDialect()


# ---------------------------------------------------------------------------
# P1 — WINDOW_FUNCTION rendering
# ---------------------------------------------------------------------------


def test_window_function_order_by_only():
    """ROW_NUMBER() OVER (ORDER BY value_ DESC)"""
    sql = dialect().build_expression_sql(
        WINDOW_FUNCTION(
            name="ROW_NUMBER",
            order_by=[ORDER_BY_DESC("value_")],
        ).AS("rn")
    )
    # Build expectation from actual output once node is implemented.
    # The expression renders as <fn>(<args>) OVER (<clauses>) AS <alias>.
    assert 'ROW_NUMBER() OVER (ORDER BY "value_" DESC)' in sql


def test_window_function_partition_and_order_by():
    """SUM(amount) OVER (PARTITION BY category ORDER BY id ASC)"""
    sql = dialect().build_expression_sql(
        WINDOW_FUNCTION(
            name="SUM",
            args=["amount"],
            partition_by=[COLUMN("category")],
            order_by=[ORDER_BY_ASC("id")],
        )
    )
    assert 'SUM("amount") OVER (PARTITION BY "category" ORDER BY "id" ASC)' == sql


def test_window_function_no_partition_no_order_by():
    """ROW_NUMBER() OVER ()"""
    sql = dialect().build_expression_sql(WINDOW_FUNCTION(name="ROW_NUMBER"))
    assert sql == "ROW_NUMBER() OVER ()"


def test_window_function_partition_only():
    """COUNT(*) OVER (PARTITION BY dept)"""
    sql = dialect().build_expression_sql(
        WINDOW_FUNCTION(
            name="COUNT",
            args=[STAR()],
            partition_by=[COLUMN("dept")],
        )
    )
    assert 'COUNT(*) OVER (PARTITION BY "dept")' == sql


def test_window_function_multiple_partition_cols():
    """RANK() OVER (PARTITION BY a, b ORDER BY c DESC)"""
    sql = dialect().build_expression_sql(
        WINDOW_FUNCTION(
            name="RANK",
            partition_by=[COLUMN("a"), COLUMN("b")],
            order_by=[ORDER_BY_DESC("c")],
        )
    )
    assert 'RANK() OVER (PARTITION BY "a", "b" ORDER BY "c" DESC)' == sql


# ---------------------------------------------------------------------------
# P2 — UNION inside a CTE body
# ---------------------------------------------------------------------------


def test_cte_with_union_body():
    """WITH cte AS (SELECT ... UNION SELECT ...) SELECT * FROM cte"""
    d = dialect()
    select_a = [SELECT(COLUMN("id")), FROM("table_a")]
    select_b = [SELECT(COLUMN("id")), FROM("table_b")]
    sql = d.build_select_sql(
        [
            WITH([CTE("combined").AS(UNION([select_a, select_b]))]),
            SELECT(COLUMN("id")),
            FROM("combined"),
        ]
    )
    assert '"combined" AS (' in sql
    assert "UNION" in sql
    assert 'FROM "table_a"' in sql
    assert 'FROM "table_b"' in sql


def test_cte_with_union_all_body():
    """WITH cte AS (SELECT ... UNION ALL SELECT ...) SELECT * FROM cte"""
    d = dialect()
    select_a = [SELECT(COLUMN("val")), FROM("src_a")]
    select_b = [SELECT(COLUMN("val")), FROM("src_b")]
    sql = d.build_select_sql(
        [
            WITH([CTE("all_vals").AS(UNION_ALL([select_a, select_b]))]),
            SELECT(COLUMN("val")),
            FROM("all_vals"),
        ]
    )
    assert '"all_vals" AS (' in sql
    assert "UNION ALL" in sql
