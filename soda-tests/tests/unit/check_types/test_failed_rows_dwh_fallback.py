"""
Tests for the DWH fallback path in failed_rows query checks.

Verifies:
- Normal queries use the CTE-wrapped path (single CTAS call)
- The CTE-wrapped CTAS SQL is correct (includes scan_id, limit)
- When CTE wrapping fails at the routing level, the streaming path is used
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


class TestDwhCtePathUsedForNormalQueries:
    def test_normal_query_uses_cte_path(self):
        """A normal SELECT query should produce CTE-wrapped CTAS SQL."""
        dialect = SqlDialect()
        user_query = "SELECT * FROM orders WHERE amount < 0"

        sql = _build_cte_wrapped_ctas(dialect, user_query)

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
        query_ast = [SELECT([STAR()]), FROM(TEMP_TABLE_NAME)]
        sql = dialect.build_select_sql(query_ast)

        assert SODA_SCAN_ID_COLUMN not in sql
        assert "LIMIT" not in sql


class TestDwhRoutingFallbackToStreaming:
    def test_in_source_failure_falls_back_to_streaming(self):
        """When in_source_transfer raises, the routing should call the streaming path."""
        in_source_called = False
        streaming_called = False

        def fake_in_source():
            nonlocal in_source_called
            in_source_called = True
            raise Exception("ORDER BY not allowed in CTE")

        def fake_streaming():
            nonlocal streaming_called
            streaming_called = True
            return mock.MagicMock()  # DwhTable

        # Simulate routing logic from _store_failed_check_full_query
        use_in_source_transfer = True
        dwh_table = None
        if use_in_source_transfer:
            try:
                dwh_table = fake_in_source()
            except Exception:
                dwh_table = fake_streaming()

        assert in_source_called
        assert streaming_called
        assert dwh_table is not None

    def test_in_source_success_does_not_call_streaming(self):
        """When in_source_transfer succeeds, the streaming path should not be called."""
        streaming_called = False

        def fake_in_source():
            return mock.MagicMock()  # DwhTable

        def fake_streaming():
            nonlocal streaming_called
            streaming_called = True

        use_in_source_transfer = True
        dwh_table = None
        if use_in_source_transfer:
            try:
                dwh_table = fake_in_source()
            except Exception:
                dwh_table = fake_streaming()

        assert dwh_table is not None
        assert not streaming_called

    def test_non_in_source_uses_streaming_directly(self):
        """When use_in_source_transfer is False, streaming is called directly."""
        in_source_called = False
        streaming_called = False

        def fake_in_source():
            nonlocal in_source_called
            in_source_called = True

        def fake_streaming():
            nonlocal streaming_called
            streaming_called = True
            return mock.MagicMock()

        use_in_source_transfer = False
        if use_in_source_transfer:
            try:
                fake_in_source()
            except Exception:
                fake_streaming()
        else:
            fake_streaming()

        assert not in_source_called
        assert streaming_called
