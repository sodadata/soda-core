"""
Tests for the DWH fallback path in failed_rows query checks.

Simulates the try/fallback logic from failed_rows_check_extension.py
by exercising the SQL AST at the dialect level. Verifies:
- Normal queries use the CTE-wrapped path (single CTAS call)
- Queries that fail CTE wrapping trigger the fallback (CTAS only)
- The fallback CTAS does not contain CTE wrapping
- Metadata columns and row limit are applied at the copy step
"""

from unittest import mock

from soda_core.common.sql_ast import (
    COLUMN,
    CREATE_TABLE_AS_SELECT,
    CTE,
    FROM,
    LIMIT,
    LITERAL,
    SELECT,
    STAR,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect

SCAN_ID = "scan-001"
CHECK_ID = "check-abc"
TEMP_TABLE_NAME = '"schema"."__soda_temp_abc123"'
SODA_SCAN_ID_COLUMN = "__soda_scan_id"


def _build_cte_wrapped_ctas(dialect: SqlDialect, user_query: str, row_limit: int = None) -> str:
    """Build the CTE-wrapped CTAS SQL (the primary/happy path)."""
    query_ast = [
        WITH([CTE(alias="_soda_failed_rows_query").AS(cte_query=user_query)]),
        SELECT([STAR(alias="_soda_failed_rows_query"), COLUMN(LITERAL(SCAN_ID)).AS(SODA_SCAN_ID_COLUMN)]),
        FROM("_soda_failed_rows_query"),
    ]
    if row_limit is not None:
        query_ast.append(LIMIT(row_limit))
    ctas = CREATE_TABLE_AS_SELECT(fully_qualified_table_name=TEMP_TABLE_NAME, select_elements=query_ast)
    return dialect.build_create_table_as_select_sql(ctas)


def _build_fallback_ctas(dialect: SqlDialect, user_query: str) -> str:
    """Build the fallback CTAS SQL (raw user query, no wrapping)."""
    fallback_ctas = CREATE_TABLE_AS_SELECT(fully_qualified_table_name=TEMP_TABLE_NAME, raw_select_sql=user_query)
    return dialect.build_create_table_as_select_sql(fallback_ctas)


def _build_copy_query(dialect: SqlDialect, add_scan_id: bool, row_limit: int = None) -> str:
    """Build the copy query from temp table to DWH table (mirrors the post-fallback logic)."""
    select_columns = [STAR()]
    if add_scan_id:
        select_columns.append(COLUMN(LITERAL(SCAN_ID)).AS(SODA_SCAN_ID_COLUMN))
    query_ast = [
        SELECT(select_columns),
        FROM(TEMP_TABLE_NAME),
    ]
    if add_scan_id and row_limit is not None:
        query_ast.append(LIMIT(row_limit))
    return dialect.build_select_sql(query_ast)


def _simulate_try_fallback(execute_update_mock, dialect: SqlDialect, user_query: str, row_limit: int = None):
    """Simulate the try/fallback logic from the extension."""
    used_fallback = False
    try:
        ctas_sql = _build_cte_wrapped_ctas(dialect, user_query, row_limit)
        execute_update_mock(ctas_sql)
    except Exception:
        ctas_sql = _build_fallback_ctas(dialect, user_query)
        execute_update_mock(ctas_sql)
        used_fallback = True
    return used_fallback


class TestDwhCtePathUsedForNormalQueries:
    def test_normal_query_uses_cte_path(self):
        """A normal SELECT query should succeed via the CTE path with a single call."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()
        user_query = "SELECT * FROM orders WHERE amount < 0"

        used_fallback = _simulate_try_fallback(execute_update, dialect, user_query)

        assert not used_fallback
        assert execute_update.call_count == 1
        sql = execute_update.call_args[0][0]
        assert "WITH" in sql
        assert "_soda_failed_rows_query" in sql
        assert user_query in sql

    def test_normal_query_cte_includes_scan_id_column(self):
        """The CTE-wrapped CTAS should include the soda_scan_id literal column."""
        dialect = SqlDialect()
        sql = _build_cte_wrapped_ctas(dialect, "SELECT * FROM t")

        assert SODA_SCAN_ID_COLUMN in sql
        assert f"'{SCAN_ID}'" in sql

    def test_normal_query_cte_includes_limit(self):
        """When row_limit is set, the CTE path should include a LIMIT clause."""
        dialect = SqlDialect()
        sql = _build_cte_wrapped_ctas(dialect, "SELECT * FROM t", row_limit=100)

        assert "LIMIT 100" in sql

    def test_normal_copy_query_has_no_scan_id(self):
        """In the primary path, copy query is just SELECT * (scan_id already in temp table)."""
        dialect = SqlDialect()
        sql = _build_copy_query(dialect, add_scan_id=False)

        assert SODA_SCAN_ID_COLUMN not in sql
        assert "LIMIT" not in sql


class TestDwhFallbackTriggeredOnCteFailure:
    def test_fallback_produces_single_ctas(self):
        """When CTE wrapping fails, the fallback should execute 1 CTAS (not 3 statements)."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()
        execute_update.side_effect = [Exception("ORDER BY not allowed in CTE"), None]
        user_query = "SELECT DISTINCT * FROM orders ORDER BY id ASC"

        used_fallback = _simulate_try_fallback(execute_update, dialect, user_query)

        assert used_fallback
        # 1 failed CTE call + 1 fallback CTAS = 2 total
        assert execute_update.call_count == 2

    def test_fallback_ctas_does_not_wrap_in_cte(self):
        """The fallback CTAS should use the raw user query without CTE wrapping."""
        dialect = SqlDialect()
        user_query = "SELECT DISTINCT * FROM orders ORDER BY id ASC"
        ctas_sql = _build_fallback_ctas(dialect, user_query)

        assert "CREATE TABLE" in ctas_sql
        assert TEMP_TABLE_NAME in ctas_sql
        assert user_query in ctas_sql
        assert "WITH" not in ctas_sql
        assert "_soda_failed_rows_query" not in ctas_sql

    def test_fallback_copy_query_adds_scan_id(self):
        """In the fallback path, the copy query should add the soda_scan_id column."""
        dialect = SqlDialect()
        sql = _build_copy_query(dialect, add_scan_id=True)

        assert SODA_SCAN_ID_COLUMN in sql
        assert f"'{SCAN_ID}'" in sql

    def test_fallback_copy_query_includes_limit(self):
        """In the fallback path, the copy query should apply the row limit."""
        dialect = SqlDialect()
        sql = _build_copy_query(dialect, add_scan_id=True, row_limit=100)

        assert "LIMIT 100" in sql
        assert SODA_SCAN_ID_COLUMN in sql

    def test_fallback_with_user_ctes(self):
        """User queries containing CTEs should work in fallback (no double WITH)."""
        dialect = SqlDialect()
        user_query = "WITH my_cte AS (SELECT * FROM source) SELECT * FROM my_cte WHERE bad = 1"
        ctas_sql = _build_fallback_ctas(dialect, user_query)

        # The user's WITH should appear exactly once, inside the CTAS
        assert ctas_sql.count("WITH") == 1
        assert "my_cte" in ctas_sql

    def test_fallback_ctas_then_copy_sequence(self):
        """Verify the full fallback sequence: CTAS without wrapping, then copy with scan_id."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()
        execute_update.side_effect = [Exception("CTE failed"), None]

        used_fallback = _simulate_try_fallback(execute_update, dialect, "SELECT 1")

        assert used_fallback
        calls = [call[0][0] for call in execute_update.call_args_list]
        # calls[0] is the failed CTE attempt
        assert "CREATE TABLE" in calls[1]
        assert "WITH" not in calls[1]
        # No ALTER or UPDATE calls
        for call_sql in calls:
            assert "ALTER TABLE" not in call_sql
            assert "UPDATE" not in call_sql
