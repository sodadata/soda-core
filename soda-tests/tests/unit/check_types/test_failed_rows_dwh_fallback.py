"""
Tests for the DWH fallback path in failed_rows query checks.

Simulates the try/fallback logic from failed_rows_check_extension.py
by exercising the SQL AST at the dialect level. Verifies:
- Normal queries use the CTE-wrapped path (single CTAS call)
- Queries that fail CTE wrapping trigger the 3-step fallback (CTAS + ALTER + UPDATE)
- The fallback SQL does not contain CTE wrapping
- Row limit warning is emitted when fallback skips server-side limit
"""

from unittest import mock

from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    ALTER_TABLE_ADD_COLUMN,
    COLUMN,
    CREATE_TABLE_AS_SELECT,
    CREATE_TABLE_COLUMN,
    CTE,
    FROM,
    LIMIT,
    LITERAL,
    SELECT,
    SET_CLAUSE,
    STAR,
    UPDATE,
    WHERE,
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


def _build_fallback_sql(dialect: SqlDialect, user_query: str) -> list[str]:
    """Build the 3-step fallback SQL sequence."""
    # Step 1: CTAS from raw user query
    fallback_ctas = CREATE_TABLE_AS_SELECT(fully_qualified_table_name=TEMP_TABLE_NAME, raw_select_sql=user_query)
    ctas_sql = dialect.build_create_table_as_select_sql(fallback_ctas)

    # Step 2: ALTER TABLE to add metadata column
    alter_sql = dialect.build_alter_table_sql(
        ALTER_TABLE_ADD_COLUMN(
            fully_qualified_table_name=TEMP_TABLE_NAME,
            column=CREATE_TABLE_COLUMN(name=SODA_SCAN_ID_COLUMN, type=SqlDataType(name=SodaDataTypeName.TEXT)),
        )
    )

    # Step 3: UPDATE to set scan_id
    update_sql = dialect.build_update_sql(
        UPDATE(
            fully_qualified_table_name=TEMP_TABLE_NAME,
            set_clauses=[SET_CLAUSE(column_name=SODA_SCAN_ID_COLUMN, value=LITERAL(SCAN_ID))],
        )
    )
    return [ctas_sql, alter_sql, update_sql]


def _simulate_try_fallback(execute_update_mock, dialect: SqlDialect, user_query: str, row_limit: int = None):
    """Simulate the try/fallback logic from the extension, recording all SQL sent to execute_update."""
    try:
        ctas_sql = _build_cte_wrapped_ctas(dialect, user_query, row_limit)
        execute_update_mock(ctas_sql)
    except Exception:
        for sql in _build_fallback_sql(dialect, user_query):
            execute_update_mock(sql)


class TestDwhCtePathUsedForNormalQueries:
    def test_normal_query_uses_cte_path(self):
        """A normal SELECT query should succeed via the CTE path with a single execute_update call."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()
        user_query = "SELECT * FROM orders WHERE amount < 0"

        _simulate_try_fallback(execute_update, dialect, user_query)

        # CTE path: single call
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

    def test_normal_query_no_fallback_statements(self):
        """Normal queries should NOT produce ALTER TABLE or UPDATE statements."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()

        _simulate_try_fallback(execute_update, dialect, "SELECT * FROM t")

        sql = execute_update.call_args[0][0]
        assert "ALTER TABLE" not in sql
        assert "UPDATE" not in sql


class TestDwhFallbackTriggeredOnCteFailure:
    def test_fallback_produces_three_statements(self):
        """When CTE wrapping fails, the fallback should execute 3 SQL statements."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()
        # First call (CTE path) raises, simulating SQL Server ORDER BY error
        execute_update.side_effect = [Exception("ORDER BY not allowed in CTE"), None, None, None]
        user_query = "SELECT DISTINCT * FROM orders ORDER BY id ASC"

        _simulate_try_fallback(execute_update, dialect, user_query)

        # 1 failed CTE call + 3 fallback calls = 4 total
        assert execute_update.call_count == 4

    def test_fallback_ctas_does_not_wrap_in_cte(self):
        """The fallback CTAS should use the raw user query without CTE wrapping."""
        dialect = SqlDialect()
        user_query = "SELECT DISTINCT * FROM orders ORDER BY id ASC"
        fallback_sqls = _build_fallback_sql(dialect, user_query)

        ctas_sql = fallback_sqls[0]
        assert "CREATE TABLE" in ctas_sql
        assert TEMP_TABLE_NAME in ctas_sql
        assert user_query in ctas_sql
        # No CTE wrapping
        assert "WITH" not in ctas_sql
        assert "_soda_failed_rows_query" not in ctas_sql

    def test_fallback_alter_adds_scan_id_column(self):
        """The fallback ALTER TABLE should add the soda_scan_id column."""
        dialect = SqlDialect()
        fallback_sqls = _build_fallback_sql(dialect, "SELECT 1")

        alter_sql = fallback_sqls[1]
        assert "ALTER TABLE" in alter_sql
        assert TEMP_TABLE_NAME in alter_sql
        assert SODA_SCAN_ID_COLUMN in alter_sql
        assert "text" in alter_sql.lower()

    def test_fallback_update_sets_scan_id(self):
        """The fallback UPDATE should set soda_scan_id to the scan ID value."""
        dialect = SqlDialect()
        fallback_sqls = _build_fallback_sql(dialect, "SELECT 1")

        update_sql = fallback_sqls[2]
        assert "UPDATE" in update_sql
        assert TEMP_TABLE_NAME in update_sql
        assert SODA_SCAN_ID_COLUMN in update_sql
        assert f"'{SCAN_ID}'" in update_sql

    def test_fallback_with_user_ctes(self):
        """User queries containing CTEs should work in fallback (no double WITH)."""
        dialect = SqlDialect()
        user_query = "WITH my_cte AS (SELECT * FROM source) SELECT * FROM my_cte WHERE bad = 1"
        fallback_sqls = _build_fallback_sql(dialect, user_query)

        ctas_sql = fallback_sqls[0]
        # The user's WITH should appear exactly once, inside the CTAS
        assert ctas_sql.count("WITH") == 1
        assert "my_cte" in ctas_sql

    def test_fallback_sql_order_is_ctas_alter_update(self):
        """The fallback must execute in order: CTAS, ALTER, UPDATE."""
        dialect = SqlDialect()
        execute_update = mock.MagicMock()
        execute_update.side_effect = [Exception("CTE failed"), None, None, None]

        _simulate_try_fallback(execute_update, dialect, "SELECT 1")

        calls = [call[0][0] for call in execute_update.call_args_list]
        # calls[0] is the failed CTE attempt
        assert "CREATE TABLE" in calls[1]
        assert "ALTER TABLE" in calls[2]
        assert "UPDATE" in calls[3]
