"""Unit tests for the execute_query_one_by_one / _memory_optimized split.

Contract: the base ``execute_query_one_by_one`` is the proven buffered
implementation; ``execute_query_one_by_one_memory_optimized`` is what the
DWH failed-rows flows call, and it DEFAULTS to the base implementation for
adapters without (or not needing) a low-memory fetch. Postgres overrides
the optimized variant with the server-side named cursor and keeps its
pre-existing rollback-on-error wrapper as the base.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from soda_core.common.data_source_connection import DataSourceConnection
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)


class _StubConnection(DataSourceConnection):
    def __init__(self):
        # Bypass DataSourceConnection.__init__ — only the method under test matters.
        self.calls = []

    def _create_connection(self, config):  # pragma: no cover - never called
        return MagicMock()

    def execute_query_one_by_one(self, sql, row_callback, log_query=True, row_limit=None):
        self.calls.append((sql, row_callback, log_query, row_limit))
        return (("col",),)


class TestDefaultDelegation:
    def test_memory_optimized_defaults_to_base_implementation(self):
        connection = _StubConnection()
        callback = lambda row, description: None

        description = connection.execute_query_one_by_one_memory_optimized(
            "SELECT 1", callback, log_query=False, row_limit=7
        )

        assert connection.calls == [("SELECT 1", callback, False, 7)]
        assert description == (("col",),)


def _make_postgres_connection(autocommit: bool) -> PostgresDataSourceConnection:
    pg = PostgresDataSourceConnection.__new__(PostgresDataSourceConnection)
    pg.connection = MagicMock()
    pg.connection.autocommit = autocommit
    return pg


def _make_plain_cursor(description=(("col",),)):
    cursor = MagicMock()
    cursor.description = description
    cursor.fetchone.return_value = None  # empty result set ends the loop
    return cursor


class TestPostgresMemoryOptimized:
    def test_uses_server_side_named_cursor(self):
        pg = _make_postgres_connection(autocommit=False)
        named_cursor = _make_plain_cursor()
        pg.connection.cursor.return_value.__enter__.return_value = named_cursor

        description = pg.execute_query_one_by_one_memory_optimized("SELECT 1", lambda r, d: None)

        cursor_kwargs = pg.connection.cursor.call_args.kwargs
        assert cursor_kwargs.get("name", "").startswith("soda_stream_")
        assert cursor_kwargs.get("withhold") is True
        assert description == (("col",),)

    def test_autocommit_falls_back_to_buffered_base(self):
        pg = _make_postgres_connection(autocommit=True)
        plain_cursor = _make_plain_cursor()
        pg.connection.cursor.return_value = plain_cursor

        description = pg.execute_query_one_by_one_memory_optimized("SELECT 1", lambda r, d: None)

        # Base path: a plain client-side cursor — no name, no withhold.
        assert pg.connection.cursor.call_args.kwargs == {}
        assert description == (("col",),)

    def test_base_method_is_the_buffered_implementation(self):
        # The restored pre-branch behavior: plain cursor, no server-side
        # streaming — regardless of autocommit.
        pg = _make_postgres_connection(autocommit=False)
        plain_cursor = _make_plain_cursor()
        pg.connection.cursor.return_value = plain_cursor

        rows_seen = []
        pg.execute_query_one_by_one("SELECT 1", lambda row, description: rows_seen.append(row))

        assert pg.connection.cursor.call_args.kwargs == {}  # no named cursor
        assert rows_seen == []
