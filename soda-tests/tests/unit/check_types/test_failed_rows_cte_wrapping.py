"""
Tests for the CTE wrapping fix in failed_rows checks.

Verifies that:
- FailedRowsCountQuery tries CTE-wrapped COUNT(*) first (efficient path)
- On CTE failure, falls back to row-by-row streaming count
- User queries with ORDER BY or CTEs are handled via the fallback
"""

from unittest import mock

from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import SqlDialect
from soda_core.contracts.impl.check_types.failed_rows_check import FailedRowsCountQuery


def _make_mock_data_source_impl_cte_success(count: int):
    """Mock where CTE-wrapped COUNT(*) succeeds."""
    data_source_impl = mock.MagicMock()
    data_source_impl.sql_dialect = SqlDialect()
    data_source_impl.execute_query.return_value = QueryResult(
        rows=[(count,)],
        columns=(("count",),),
    )
    return data_source_impl


def _make_mock_data_source_impl_cte_fails(rows: list[tuple]):
    """Mock where CTE-wrapped COUNT(*) fails, fallback streams given rows."""
    data_source_impl = mock.MagicMock()
    data_source_impl.sql_dialect = SqlDialect()
    data_source_impl.execute_query.side_effect = Exception("ORDER BY not allowed in CTE")
    description = (("col1",),)

    def fake_execute_one_by_one(sql, row_callback, log_query=True, row_limit=None):
        for row in rows:
            row_callback(row, description)
        return description

    data_source_impl.execute_query_one_by_one.side_effect = fake_execute_one_by_one
    return data_source_impl


def _make_mock_metric():
    metric = mock.MagicMock()
    metric.id = "test_metric_id"
    metric.type = "failed_rows_count"
    return metric


class TestFailedRowsCountQueryCteSuccess:
    def test_cte_count_used_when_successful(self):
        """When CTE wrapping succeeds, the count comes from SELECT COUNT(*)."""
        data_source_impl = _make_mock_data_source_impl_cte_success(count=5)

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE status = 'invalid'",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 5
        data_source_impl.execute_query.assert_called_once()
        data_source_impl.execute_query_one_by_one.assert_not_called()

    def test_cte_sql_contains_count_and_with(self):
        """The CTE-wrapped SQL should use WITH and COUNT(*)."""
        data_source_impl = _make_mock_data_source_impl_cte_success(count=0)

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE status = 'invalid'",
        )

        assert "WITH" in query.sql
        assert "COUNT" in query.sql

    def test_cte_count_zero_rows(self):
        """Zero failed rows via CTE path should produce value 0."""
        data_source_impl = _make_mock_data_source_impl_cte_success(count=0)

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE 1=0",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 0


class TestFailedRowsCountQueryFallback:
    def test_fallback_on_cte_failure(self):
        """When CTE wrapping fails, fall back to streaming row count."""
        rows = [(1, "bad"), (2, "worse"), (3, "terrible")]
        data_source_impl = _make_mock_data_source_impl_cte_fails(rows=rows)

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE status = 'invalid' ORDER BY id ASC",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 3
        data_source_impl.execute_query_one_by_one.assert_called_once()

    def test_fallback_zero_rows(self):
        """Fallback path with zero rows should produce value 0."""
        data_source_impl = _make_mock_data_source_impl_cte_fails(rows=[])

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM orders WHERE 1=0 ORDER BY id",
        )
        measurements = query.execute()

        assert len(measurements) == 1
        assert measurements[0].value == 0

    def test_fallback_streams_raw_user_query(self):
        """The fallback should execute the original user query, not the CTE-wrapped one."""
        user_query = "SELECT * FROM orders ORDER BY id ASC"
        data_source_impl = _make_mock_data_source_impl_cte_fails(rows=[])

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query=user_query,
        )
        query.execute()

        call_args = data_source_impl.execute_query_one_by_one.call_args
        assert call_args.kwargs["sql"] == user_query

    def test_both_paths_fail_returns_empty(self):
        """If both CTE and streaming fail, execute() returns an empty list."""
        data_source_impl = mock.MagicMock()
        data_source_impl.sql_dialect = SqlDialect()
        data_source_impl.execute_query.side_effect = Exception("CTE failed")
        data_source_impl.execute_query_one_by_one.side_effect = Exception("connection lost")

        query = FailedRowsCountQuery(
            data_source_impl=data_source_impl,
            metrics=[_make_mock_metric()],
            failed_rows_query="SELECT * FROM missing_table",
        )
        measurements = query.execute()

        assert measurements == []
