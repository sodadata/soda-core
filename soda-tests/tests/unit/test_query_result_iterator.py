"""Unit tests for ``execute_query_iterate`` and its ``streaming`` option.

The postgres tests drive a fake server-side cursor that logs each fetch, so they can
check that rows come from the database a batch at a time as the caller reads them.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import psycopg
import pytest
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResultIterator
from soda_core.common.exceptions import SodaCoreException
from soda_postgres.common.data_sources.postgres_data_source_connection import PostgresDataSourceConnection


class _FakeCursor:
    def __init__(self, rows: list[tuple], description=(("col", None),)):
        self._rows = list(rows)
        self.description = description
        self.rowcount = len(rows)
        self.fetches: list[int] = []
        self.executed: list[str] = []
        self.closed = False

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size):
        self.fetches.append(size)
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class _PlainConnection(DataSourceConnection):
    """The base connection, on a fake DB-API connection."""

    def __init__(self, cursor: _FakeCursor):
        self.name = "plain"
        self.connection = MagicMock()
        self.connection.cursor.return_value = cursor

    def _create_connection(self, config):  # pragma: no cover - never called
        raise AssertionError


class TestQueryResultIterator:
    def test_reads_the_cursor_by_default(self):
        iterator = QueryResultIterator(_FakeCursor([(1,), (2,)]), format_row=tuple)

        assert list(iterator) == [(1,), (2,)]

    def test_reads_the_rows_it_is_given_and_formats_them(self):
        cursor = _FakeCursor([("from the cursor",)])

        iterator = QueryResultIterator(cursor, format_row=lambda row: (row[0] * 10,), rows=iter([(1,), (2,)]))

        assert list(iterator) == [(10,), (20,)]
        assert cursor.fetchone() == ("from the cursor",)

    def test_batches_split_the_rows_and_keep_the_last_short_one(self):
        iterator = QueryResultIterator(_FakeCursor([(1,), (2,), (3,), (4,), (5,)]), format_row=tuple)

        assert list(iterator.batches(2)) == [[(1,), (2,)], [(3,), (4,)], [(5,)]]

    def test_batches_read_on_from_where_iteration_stopped(self):
        iterator = QueryResultIterator(_FakeCursor([(1,), (2,), (3,)]), format_row=tuple)

        assert next(iterator) == (1,)
        assert list(iterator.batches(10)) == [[(2,), (3,)]]
        assert list(iterator) == []

    def test_batches_of_an_empty_result_are_none(self):
        iterator = QueryResultIterator(_FakeCursor([]), format_row=tuple)

        assert list(iterator.batches(10)) == []

    @pytest.mark.parametrize("max_rows", [0, -1])
    def test_batches_refuse_a_size_below_one_before_reading(self, max_rows):
        iterator = QueryResultIterator(_FakeCursor([(1,)]), format_row=tuple)

        with pytest.raises(ValueError):
            iterator.batches(max_rows)
        assert list(iterator) == [(1,)]


class TestExecuteQueryIterate:
    def test_without_streaming_reads_a_plain_cursor_and_closes_it(self):
        cursor = _FakeCursor([(1,), (2,)])
        connection = _PlainConnection(cursor)

        with connection.execute_query_iterate("SELECT 1") as rows:
            assert list(rows) == [(1,), (2,)]

        assert cursor.executed == ["SELECT 1"]
        assert cursor.closed

    def test_streaming_on_a_connection_that_cannot_stream_raises_before_running_the_query(self):
        cursor = _FakeCursor([(1,)])
        connection = _PlainConnection(cursor)

        with pytest.raises(NotImplementedError):
            with connection.execute_query_iterate("SELECT 1", streaming=True):
                pass  # pragma: no cover - never entered

        assert cursor.executed == []


def _postgres(cursor: _FakeCursor, autocommit: bool = False) -> PostgresDataSourceConnection:
    connection = PostgresDataSourceConnection.__new__(PostgresDataSourceConnection)
    connection.name = "postgres"
    connection.connection = MagicMock()
    connection.connection.autocommit = autocommit
    connection.connection.cursor.return_value = cursor
    return connection


class TestPostgresStreaming:
    def test_reads_through_a_named_cursor_that_is_not_held(self):
        cursor = _FakeCursor([(1,), (2,)], description=(("id", 23),))
        connection = _postgres(cursor)

        with connection.execute_query_iterate("SELECT id FROM t", streaming=True) as rows:
            assert rows.columns == {"id": 23}
            assert list(rows) == [(1,), (2,)]

        cursor_kwargs = connection.connection.cursor.call_args.kwargs
        assert cursor_kwargs["name"].startswith("soda_stream_")
        assert "withhold" not in cursor_kwargs
        assert cursor.executed == ["SELECT id FROM t"]
        assert cursor.closed

    def test_fetches_a_probe_row_then_growing_batches_as_the_caller_reads(self):
        cursor = _FakeCursor([(i,) for i in range(30)])
        connection = _postgres(cursor)

        with connection.execute_query_iterate("SELECT 1", streaming=True) as rows:
            assert cursor.fetches == []
            assert next(rows) == (0,)
            assert cursor.fetches == [1]
            assert next(rows) == (1,)
            assert cursor.fetches == [1, 4]
            assert [len(batch) for batch in rows.batches(10)] == [10, 10, 8]

        # The last fetch finds the result used up.
        assert cursor.fetches == [1, 4, 16, 64, 256]

    def test_stopping_early_fetches_no_more_and_closes_the_cursor(self):
        cursor = _FakeCursor([(i,) for i in range(30)])
        connection = _postgres(cursor)

        with connection.execute_query_iterate("SELECT 1", streaming=True) as rows:
            next(rows)

        assert cursor.fetches == [1]
        assert cursor.closed

    def test_autocommit_connection_raises_instead_of_buffering(self):
        cursor = _FakeCursor([(1,)])
        connection = _postgres(cursor, autocommit=True)

        with pytest.raises(SodaCoreException, match="autocommit"):
            with connection.execute_query_iterate("SELECT 1", streaming=True):
                pass  # pragma: no cover - never entered

        connection.connection.cursor.assert_not_called()

    def test_a_failed_fetch_rolls_back_and_raises(self):
        cursor = _FakeCursor([(1,)])
        cursor.fetchmany = MagicMock(side_effect=psycopg.errors.QueryCanceled("canceled"))
        connection = _postgres(cursor)

        with pytest.raises(psycopg.errors.QueryCanceled):
            with connection.execute_query_iterate("SELECT 1", streaming=True) as rows:
                list(rows)

        connection.connection.rollback.assert_called_once()
        assert cursor.closed

    def test_an_error_of_the_caller_does_not_roll_back(self):
        cursor = _FakeCursor([(1,)])
        connection = _postgres(cursor)

        with pytest.raises(KeyError):
            with connection.execute_query_iterate("SELECT 1", streaming=True):
                raise KeyError("caller")

        connection.connection.rollback.assert_not_called()
        assert cursor.closed
