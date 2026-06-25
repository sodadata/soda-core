"""Unit tests for the execute_query_one_by_one / preferring-streaming split.

Contract: the base ``execute_query_one_by_one`` is the proven buffered
implementation; ``execute_query_one_by_one_prefer_streaming`` is what the
DWH failed-rows flows call. It dispatches to the streaming impl only when the
driver is enabled AND the adapter advertises ``supports_streaming_fetch()``;
otherwise it DEFAULTS to the base buffered fetch (adapters without a low-memory
fetch). Postgres advertises the capability and overrides
``_execute_query_one_by_one_streaming`` with the server-side named cursor, keeping
its pre-existing rollback-on-error wrapper as the base.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    memory_optimized_driver_settings,
)
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)


@pytest.fixture(autouse=True)
def _enable_memory_optimized_driver(monkeypatch):
    """The memory-optimized fetch driver (e.g. postgres' server-side streaming
    cursor) is DISABLED by default in production (perf — env-gated via
    MEMORY_OPTIMIZED_DRIVER_ENABLED). These tests cover that implementation, so
    enable it per-test."""
    monkeypatch.setenv("MEMORY_OPTIMIZED_DRIVER_ENABLED", "true")


class _StubConnection(DataSourceConnection):
    def __init__(self):
        # Bypass DataSourceConnection.__init__ — only the method under test matters.
        self.calls = []

    def _create_connection(self, config):  # pragma: no cover - never called
        return MagicMock()

    def execute_query_one_by_one(self, sql, row_callback, log_query=True, row_limit=None):
        self.calls.append((sql, row_callback, log_query, row_limit))
        return (("col",),)


class _CapableButUnimplemented(DataSourceConnection):
    """Advertises the streaming capability but never overrides the impl — the
    exact misconfiguration the base ``NotImplementedError`` guards against."""

    def __init__(self):
        # Bypass DataSourceConnection.__init__ — only the dispatch matters.
        pass

    def _create_connection(self, config):  # pragma: no cover - never called
        return MagicMock()

    def supports_streaming_fetch(self):
        return True


class TestDispatchGating:
    def test_unsupported_adapter_buffers_even_when_driver_enabled(self):
        # Driver ON (autouse fixture) but the adapter advertises no streaming
        # capability, so the call must fall through to the buffered base fetch —
        # and the streaming impl (which would raise) is never reached.
        connection = _StubConnection()
        callback = lambda row, description: None

        description = connection.execute_query_one_by_one_prefer_streaming(
            "SELECT 1", callback, log_query=False, row_limit=7
        )

        assert connection.calls == [("SELECT 1", callback, False, 7)]
        assert description == (("col",),)

    def test_disabled_driver_buffers_even_for_capable_adapter(self, monkeypatch):
        # Driver OFF → even postgres (a capable adapter) takes the buffered path
        # (a plain client-side cursor, no server-side named cursor).
        monkeypatch.delenv("MEMORY_OPTIMIZED_DRIVER_ENABLED", raising=False)
        memory_optimized_driver_settings.reset()
        pg = _make_postgres_connection(autocommit=False)
        plain_cursor = _make_plain_cursor()
        pg.connection.cursor.return_value = plain_cursor

        pg.execute_query_one_by_one_prefer_streaming("SELECT 1", lambda r, d: None)

        assert pg.connection.cursor.call_args.kwargs == {}  # no name → buffered

    def test_base_supports_streaming_fetch_is_false(self):
        assert _StubConnection().supports_streaming_fetch() is False

    def test_postgres_supports_streaming_fetch_is_true(self):
        assert _make_postgres_connection(autocommit=False).supports_streaming_fetch() is True

    def test_base_streaming_impl_raises_not_implemented(self):
        # The base impl must fail loudly rather than silently buffer — the buffered
        # fallback now lives in the router gate, not here.
        with pytest.raises(NotImplementedError):
            _StubConnection()._execute_query_one_by_one_streaming("SELECT 1", lambda r, d: None)

    def test_capable_adapter_without_impl_raises(self):
        # Advertises supports_streaming_fetch() == True but never overrides the impl:
        # the dispatch must surface the misconfiguration, not quietly buffer.
        with pytest.raises(NotImplementedError):
            _CapableButUnimplemented().execute_query_one_by_one_prefer_streaming("SELECT 1", lambda r, d: None)


def _make_postgres_connection(autocommit: bool) -> PostgresDataSourceConnection:
    pg = PostgresDataSourceConnection.__new__(PostgresDataSourceConnection)
    pg.connection = MagicMock()
    pg.connection.autocommit = autocommit
    # Safeguard against runaway loops: the buffered base fetch loops on
    # `while cursor.fetchone()`, and a bare MagicMock.fetchone() returns a
    # truthy Mock endlessly. Bound the DEFAULT (non-context-manager) cursor's
    # fetches to None/[] so any buffered path terminates immediately. Tests that
    # need rows override these, or feed the server-side path via __enter__.
    default_cursor = pg.connection.cursor.return_value
    default_cursor.fetchone.return_value = None
    default_cursor.fetchmany.return_value = []
    return pg


def _make_plain_cursor(description=(("col",),)):
    cursor = MagicMock()
    cursor.description = description
    cursor.fetchone.return_value = None  # empty result set ends the loop (buffered base path)
    cursor.fetchmany.return_value = []  # empty result set ends the loop (streaming path)
    return cursor


class TestPostgresMemoryOptimized:
    def test_uses_server_side_named_cursor(self):
        pg = _make_postgres_connection(autocommit=False)
        named_cursor = _make_plain_cursor()
        pg.connection.cursor.return_value.__enter__.return_value = named_cursor

        description = pg.execute_query_one_by_one_prefer_streaming("SELECT 1", lambda r, d: None)

        cursor_kwargs = pg.connection.cursor.call_args.kwargs
        assert cursor_kwargs.get("name", "").startswith("soda_stream_")
        assert cursor_kwargs.get("withhold") is True
        assert description == (("col",),)

    def test_autocommit_falls_back_to_buffered_base(self):
        pg = _make_postgres_connection(autocommit=True)
        plain_cursor = _make_plain_cursor()
        pg.connection.cursor.return_value = plain_cursor

        description = pg.execute_query_one_by_one_prefer_streaming("SELECT 1", lambda r, d: None)

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


class _FakeStreamCursor:
    """Records the size of every fetchmany() call against a canned row list."""

    def __init__(self, rows, description=(("col",),)):
        self._rows = list(rows)
        self.description = description
        self.fetch_sizes = []

    def execute(self, sql):
        """No-op stub: rows are served by fetchmany(), not execute()."""

    def fetchmany(self, n):
        self.fetch_sizes.append(n)
        batch, self._rows = self._rows[:n], self._rows[n:]
        return batch

    def fetchone(self):
        # Safeguard: drains the same finite canned list so the buffered base
        # path (if ever taken with this cursor) terminates instead of looping.
        if not self._rows:
            return None
        row, self._rows = self._rows[0], self._rows[1:]
        return row


def _stream_rows(rows, row_limit=None):
    """Run the postgres memory-optimized path over a fake server-side cursor."""
    pg = _make_postgres_connection(autocommit=False)
    fake_cursor = _FakeStreamCursor(rows)
    pg.connection.cursor.return_value.__enter__.return_value = fake_cursor

    seen = []
    pg.execute_query_one_by_one_prefer_streaming(
        "SELECT 1", lambda row, description: seen.append(row), row_limit=row_limit
    )
    return fake_cursor.fetch_sizes, seen


class TestByteBudgetedFetchBatching:
    def test_thin_rows_ramp_up_to_row_cap(self):
        from soda_postgres.common.data_sources.postgres_data_source_connection import (
            STREAM_FETCH_MAX_BATCH_ROWS,
        )

        rows = [("x",)] * 2500
        fetch_sizes, seen = _stream_rows(rows)

        # Probe row first, then 4x growth per fetch until the 1000-row clamp:
        # tiny rows never hit the byte budget, but they also never jump
        # straight to the cap (thin-probe overshoot bound).
        assert fetch_sizes[:6] == [1, 4, 16, 64, 256, STREAM_FETCH_MAX_BATCH_ROWS]
        assert all(size == STREAM_FETCH_MAX_BATCH_ROWS for size in fetch_sizes[6:])
        assert len(seen) == 2500

    def test_fat_container_value_keeps_batch_at_one(self):
        # psycopg3 returns jsonb as parsed dicts — the sizer must see the
        # payload through the container, not just the dict header.
        rows = [({"payload": "x" * (10 * 1024 * 1024)},)] * 4
        fetch_sizes, seen = _stream_rows(rows)

        assert fetch_sizes[:4] == [1, 1, 1, 1]
        assert len(seen) == 4

    def test_fat_rows_stay_at_batch_size_one(self):
        # 10 MB strings exceed the 8 MB budget — every fetch must stay 1.
        rows = [("x" * (10 * 1024 * 1024),)] * 4
        fetch_sizes, seen = _stream_rows(rows)

        assert fetch_sizes[:4] == [1, 1, 1, 1]
        assert len(seen) == 4

    def test_late_fat_row_shrinks_batch_permanently(self):
        from soda_postgres.common.data_sources.postgres_data_source_connection import (
            STREAM_FETCH_MAX_BATCH_ROWS,
        )

        # 1500 thin rows, then a fat one, then more thin rows. The widest
        # row seen is monotonic, so once the fat row lands the batch size
        # drops to 1 and stays there.
        rows = [("x",)] * 1500 + [("y" * (10 * 1024 * 1024),)] + [("z",)] * 5
        fetch_sizes, seen = _stream_rows(rows)

        assert len(seen) == 1506
        fat_batch_index = next(
            i for i, size in enumerate(fetch_sizes) if sum(fetch_sizes[:i]) < 1501 <= sum(fetch_sizes[: i + 1])
        )
        assert all(size == 1 for size in fetch_sizes[fat_batch_index + 1 :])
        assert STREAM_FETCH_MAX_BATCH_ROWS in fetch_sizes  # thin rows did batch up first

    def test_row_limit_clamps_fetch_size(self):
        rows = [("x",)] * 100
        fetch_sizes, seen = _stream_rows(rows, row_limit=5)

        # Probe of 1, then the remainder clamped to the limit — never over-fetch.
        assert fetch_sizes == [1, 4]
        assert len(seen) == 5


class TestHeldCursorCloseAfterRollback:
    def test_close_issued_after_stream_error(self):
        import psycopg

        pg = _make_postgres_connection(autocommit=False)
        named_cursor = MagicMock()
        named_cursor.execute.side_effect = psycopg.errors.QueryCanceled("boom")
        close_cursor = MagicMock()

        # First cursor(name=..., withhold=True) → failing named cursor;
        # second plain cursor() → the best-effort CLOSE executor.
        def cursor_factory(*args, **kwargs):
            ctx = MagicMock()
            ctx.__enter__.return_value = named_cursor if kwargs.get("name") else close_cursor
            return ctx

        pg.connection.cursor.side_effect = cursor_factory

        try:
            pg.execute_query_one_by_one_prefer_streaming("SELECT 1", lambda r, d: None)
            raise AssertionError("expected the stream error to propagate")
        except psycopg.errors.QueryCanceled:
            pass

        close_statements = [call.args[0] for call in close_cursor.execute.call_args_list]
        assert len(close_statements) == 1
        assert close_statements[0].startswith('CLOSE "soda_stream_')
        pg.connection.rollback.assert_called()  # transaction cleaned before the CLOSE
