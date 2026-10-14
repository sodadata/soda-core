"""Unit tests for ``SnowflakeDataSourceConnection._execute_query_one_by_one_streaming``.

The fake result batches log when they are opened, so the tests can check what the
cursor's own fetch does not give: a batch is opened only after the previous one is
used up, and a row limit never opens a later batch.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from snowflake.connector.errors import OperationalError
from soda_core.common.data_source_connection import memory_optimized_driver_settings
from soda_snowflake.common.data_sources.snowflake_data_source_connection import SnowflakeDataSourceConnection


@pytest.fixture(autouse=True)
def _enable_memory_optimized_driver(monkeypatch):
    monkeypatch.setenv("MEMORY_OPTIMIZED_DRIVER_ENABLED", "true")


class _FakeResultBatch:
    def __init__(self, batch_index: int, rows: list, events: list):
        self._batch_index = batch_index
        self._rows = rows
        self._events = events
        self.create_iter_kwargs: list[dict] = []

    def create_iter(self, **kwargs):
        self.create_iter_kwargs.append(kwargs)
        self._events.append(("open batch", self._batch_index))
        return iter(self._rows)


class _FakeSnowflakeCursor:
    def __init__(self, rows_per_batch: list[list], description=(("COL",),), rowcount=None):
        self.events: list = []
        self.result_batches = [_FakeResultBatch(index, rows, self.events) for index, rows in enumerate(rows_per_batch)]
        self.description = description
        self.rowcount = sum(len(rows) for rows in rows_per_batch) if rowcount is None else rowcount
        self.closed = False

    def execute(self, sql):
        """No-op stub: rows are served by the result batches."""

    def get_result_batches(self):
        return self.result_batches

    def fetchone(self):
        # The buffered base fetch reads here; an empty result ends its loop.
        return None

    def close(self):
        self.closed = True


def _make_connection(cursor: _FakeSnowflakeCursor) -> SnowflakeDataSourceConnection:
    connection = SnowflakeDataSourceConnection.__new__(SnowflakeDataSourceConnection)
    connection.connection = MagicMock()
    connection.connection.cursor.return_value = cursor
    return connection


def _stream(cursor: _FakeSnowflakeCursor, row_limit=None):
    def record_row(row, description):
        cursor.events.append(("row", row))

    return _make_connection(cursor).execute_query_one_by_one_prefer_streaming(
        "SELECT 1", record_row, row_limit=row_limit
    )


class TestOneBatchAtATime:
    def test_opens_a_batch_only_after_the_previous_one_is_used_up(self):
        cursor = _FakeSnowflakeCursor([[(1,), (2,)], [(3,), (4,)], [(5,)]])

        _stream(cursor)

        assert cursor.events == [
            ("open batch", 0),
            ("row", (1,)),
            ("row", (2,)),
            ("open batch", 1),
            ("row", (3,)),
            ("row", (4,)),
            ("open batch", 2),
            ("row", (5,)),
        ]

    def test_row_limit_at_the_end_of_a_batch_never_opens_the_next_one(self):
        cursor = _FakeSnowflakeCursor([[(1,), (2,)], [(3,), (4,)], [(5,)]])

        _stream(cursor, row_limit=2)

        assert cursor.events == [("open batch", 0), ("row", (1,)), ("row", (2,))]

    def test_row_limit_inside_a_batch_never_opens_later_batches(self):
        cursor = _FakeSnowflakeCursor([[(1,), (2,)], [(3,), (4,)], [(5,)]])

        _stream(cursor, row_limit=3)

        assert cursor.events == [("open batch", 0), ("row", (1,)), ("row", (2,)), ("open batch", 1), ("row", (3,))]

    def test_zero_row_limit_opens_no_batch(self):
        cursor = _FakeSnowflakeCursor([[(1,)]])

        _stream(cursor, row_limit=0)

        assert cursor.events == []

    def test_returns_the_cursor_description_and_closes_the_cursor(self):
        description = (("ID",), ("NAME",))
        cursor = _FakeSnowflakeCursor([[(1, "a")], [(2, "b")]], description=description)
        connection = _make_connection(cursor)
        descriptions_passed_to_callback = []

        returned_description = connection.execute_query_one_by_one_prefer_streaming(
            "SELECT 1", lambda row, row_description: descriptions_passed_to_callback.append(row_description)
        )

        assert returned_description == description
        assert descriptions_passed_to_callback == [description, description]
        assert cursor.closed

    def test_every_batch_gets_the_connection_like_the_cursors_own_fetch(self):
        # The cursor's own fetch calls create_iter(connection=...) too, so the rows
        # keep the Python types of the buffered path.
        cursor = _FakeSnowflakeCursor([[(1,)], [(2,)]])
        connection = _make_connection(cursor)

        connection.execute_query_one_by_one_prefer_streaming("SELECT 1", lambda row, description: None)

        assert [batch.create_iter_kwargs for batch in cursor.result_batches] == [
            [{"connection": connection.connection}],
            [{"connection": connection.connection}],
        ]


class TestReadErrors:
    def test_conversion_error_handed_over_as_a_row_is_raised(self):
        cursor = _FakeSnowflakeCursor([[(1,), ValueError("cannot convert"), (3,)]])

        with pytest.raises(ValueError, match="cannot convert"):
            _stream(cursor)

        assert cursor.events == [("open batch", 0), ("row", (1,))]
        assert cursor.closed

    def test_result_ending_before_the_reported_row_count_raises(self):
        cursor = _FakeSnowflakeCursor([[(1,)], [(2,)]], rowcount=3)

        with pytest.raises(OperationalError, match="produced 2 row"):
            _stream(cursor)

        assert cursor.closed

    def test_stopping_at_the_row_limit_is_not_a_short_result(self):
        cursor = _FakeSnowflakeCursor([[(1,), (2,)], [(3,)]], rowcount=3)

        _stream(cursor, row_limit=2)

        assert cursor.events == [("open batch", 0), ("row", (1,)), ("row", (2,))]

    def test_cursor_is_closed_when_the_callback_raises(self):
        cursor = _FakeSnowflakeCursor([[(1,)]])

        def failing_callback(row, description):
            raise RuntimeError("insert failed")

        with pytest.raises(RuntimeError, match="insert failed"):
            _make_connection(cursor).execute_query_one_by_one_prefer_streaming("SELECT 1", failing_callback)

        assert cursor.closed


class TestDispatch:
    def test_snowflake_supports_streaming_fetch(self):
        assert _make_connection(_FakeSnowflakeCursor([])).supports_streaming_fetch() is True

    def test_disabled_driver_uses_the_buffered_fetch(self, monkeypatch):
        monkeypatch.delenv("MEMORY_OPTIMIZED_DRIVER_ENABLED", raising=False)
        memory_optimized_driver_settings.reset()
        cursor = _FakeSnowflakeCursor([[(1,)]])

        _stream(cursor)

        assert cursor.events == []
