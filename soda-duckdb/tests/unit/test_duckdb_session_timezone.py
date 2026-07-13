"""Unit tests for ``DuckDBDataSourceConnection._fetch_session_timezone`` and the
``DuckDBCursor`` context-manager protocol.

DuckDB ships a custom cursor wrapper (``DuckDBCursor``) that delegates most
attribute access to the underlying connection via ``__getattr__``. Python looks
up dunder methods on the *type*, not the instance, so ``__getattr__`` does not
satisfy the ``with`` statement's protocol — the class itself must define
``__enter__`` / ``__exit__``. CI surfaced this when the
``_fetch_session_timezone`` implementation switched to ``with cursor`` syntax.

These tests use an in-memory DuckDB connection (no credentials required) so the
end-to-end ``with self.connection.cursor() as cursor:`` flow runs against a real
connection, catching this class of failure before CI does.
"""

from __future__ import annotations

from datetime import tzinfo

import duckdb
from soda_duckdb.common.data_sources.duckdb_data_source import (
    DuckDBCursor,
    DuckDBDataSourceConnectionWrapper,
)


class TestDuckDBCursorContextManager:
    """Pin the context-manager contract on the wrapper class itself.

    Without ``__enter__`` / ``__exit__`` defined directly on ``DuckDBCursor``,
    ``with cursor`` raises ``TypeError`` even though attribute delegation
    appears to expose the underlying connection — Python's dunder lookup
    bypasses ``__getattr__``.
    """

    def test_cursor_supports_with_statement(self) -> None:
        # In-memory connection: no credentials, no lifecycle, fully synchronous.
        raw = duckdb.connect(":memory:")
        cursor = DuckDBCursor(raw)
        with cursor as ctx:
            ctx.execute("SELECT 1")
            row = ctx.fetchone()
        assert row == (1,)

    def test_with_statement_returns_self(self) -> None:
        raw = duckdb.connect(":memory:")
        cursor = DuckDBCursor(raw)
        with cursor as ctx:
            assert ctx is cursor

    def test_close_remains_a_noop(self) -> None:
        # DuckDB cursors share state with the connection; ``close()`` is a no-op
        # to avoid breaking subsequent queries on the same connection. The
        # context-manager exit must not regress that.
        raw = duckdb.connect(":memory:")
        cursor = DuckDBCursor(raw)
        with cursor as ctx:
            ctx.execute("SELECT 1")
            ctx.fetchone()
        # After the with-block, the underlying connection must still be usable.
        cursor.execute("SELECT 2")
        assert cursor.fetchone() == (2,)


class TestDuckDBFetchSessionTimezone:
    """Drive ``_fetch_session_timezone`` end-to-end against an in-memory DuckDB.

    DuckDB inherits its session timezone from the host OS by default, so the
    specific value depends on the runner — but the *shape* of the return
    (a usable ``tzinfo``) is what this layer is responsible for.
    """

    def _make_connection_with_real_duckdb(self):
        # Build a DuckDBDataSourceConnection skeleton with a real in-memory
        # DuckDB connection so the with-cursor path runs end-to-end. The
        # adapter doesn't need any other state to satisfy
        # ``_fetch_session_timezone``.
        from soda_duckdb.common.data_sources.duckdb_data_source import (
            DuckDBDataSourceConnection,
        )

        instance = DuckDBDataSourceConnection.__new__(DuckDBDataSourceConnection)
        raw = duckdb.connect(":memory:")
        instance.connection = DuckDBDataSourceConnectionWrapper(raw)
        return instance

    def test_fetch_returns_tzinfo(self) -> None:
        instance = self._make_connection_with_real_duckdb()

        result = instance._fetch_session_timezone()

        assert isinstance(result, tzinfo)
        # The tzinfo must be usable to localize a naive datetime — that's the
        # invariant the value-mapper layer relies on.
        from datetime import datetime, timezone

        naive = datetime(2024, 6, 15, 12, 0, 0)
        aware = naive.replace(tzinfo=result)
        aware.astimezone(timezone.utc)  # would raise if tzinfo is malformed

    def test_with_cursor_context_manager_works_end_to_end(self) -> None:
        # The point of this test is to catch the exact regression CI flagged:
        # ``with self.connection.cursor() as cursor`` raising TypeError because
        # DuckDBCursor doesn't expose ``__exit__``. If ``DuckDBCursor`` is ever
        # refactored to drop the explicit dunders, this test fails immediately
        # rather than the failure surfacing in an integration run.
        instance = self._make_connection_with_real_duckdb()

        # Exercise the same pattern the real method uses.
        with instance.connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('TimeZone')")
            row = cursor.fetchone()

        assert row is not None
        assert isinstance(row[0], str)
