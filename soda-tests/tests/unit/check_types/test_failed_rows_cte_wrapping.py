"""
Tests for the CTE wrapping fix in failed_rows checks.

Verifies that:
- FailedRowsCountQuery does NOT wrap user queries in CTEs
- User queries with ORDER BY are preserved as-is
- User queries containing CTEs are preserved as-is
- Row count is derived from len(result.rows), not SELECT COUNT(*)
"""

from unittest import mock

from soda_core.common.data_source_results import QueryResult
from soda_core.contracts.impl.check_types.failed_rows_check import FailedRowsCountQuery


def _make_mock_data_source_impl(rows: list[tuple]):
    """Create a mock DataSourceImpl that returns the given rows from execute_query."""
    data_source_impl = mock.MagicMock()
    data_source_impl.execute_query.return_value = QueryResult(
        rows=rows,
        columns=(("col1",),),
    )
    return data_source_impl


def _make_mock_metric():
    metric = mock.MagicMock()
    metric.id = "test_metric_id"
    metric.type = "failed_rows_count"
    return metric


class TestFailedRowsCountQueryNoCteWrapping:
    def test_user_query_not_wrapped_in_cte(self):
        """The exact user query should be passed through as SQL, not wrapped in a CTE."""
        user_query = "SELECT * FROM orders WHERE status = 'invalid' ORDER BY id ASC"
        data_source_impl = _make_mock_data_source_impl(rows=[])

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query=user_query,
        )

        assert query.sql == user_query
        assert "WITH" not in query.sql

    def test_user_query_with_order_by_preserved(self):
        """ORDER BY in user queries must not be stripped or wrapped (SQL Server breaks otherwise)."""
        user_query = "SELECT DISTINCT * FROM customers ORDER BY id ASC"
        data_source_impl = _make_mock_data_source_impl(rows=[])

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query=user_query,
        )

        assert query.sql == user_query

    def test_user_query_with_ctes_preserved(self):
        """User queries that already contain CTEs must not be double-wrapped."""
        user_query = (
            "WITH active_users AS (SELECT * FROM users WHERE active = 1) "
            "SELECT * FROM active_users WHERE age < 0"
        )
        data_source_impl = _make_mock_data_source_impl(rows=[])

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query=user_query,
        )

        assert query.sql == user_query

    def test_count_via_len_rows(self):
        """Row count should come from len(result.rows), not from a SQL COUNT(*)."""
        rows = [(1, "bad"), (2, "worse"), (3, "terrible")]
        data_source_impl = _make_mock_data_source_impl(rows=rows)

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE status = 'invalid'",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 3

    def test_count_zero_rows(self):
        """Zero failed rows should produce a measurement with value 0."""
        data_source_impl = _make_mock_data_source_impl(rows=[])

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE 1=0",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 0

    def test_none_query_result_returns_zero(self):
        """If execute_query returns None, metric value should be 0."""
        data_source_impl = mock.MagicMock()
        data_source_impl.execute_query.return_value = None

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 0

    def test_execute_query_error_returns_empty(self):
        """On SQL execution error, execute() should return an empty list (not raise)."""
        data_source_impl = mock.MagicMock()
        data_source_impl.execute_query.side_effect = Exception("connection lost")

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM missing_table",
        )
        measurements = query.execute()

        assert measurements == []
