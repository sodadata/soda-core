"""Unit tests for query timing, per-connection counters, and DEBUG-formatting
guards on ``DataSourceConnection``.

Extension scans (metric monitoring, profiling) log through this engine but
today get no query timing, no row-count logging, and pay the cost of
tabulate/truncation formatting on every query even when DEBUG is off. These
tests pin: (a) exactly one DEBUG record with duration + row count per query
(rows yielded/processed at completion for the streaming variants), (b) the
public per-connection ``query_counters`` accumulator extensions read to build
scan summaries — recorded even when the query fails, and snapshot-able for
per-scan totals, (c) that the expensive row-formatting helpers are skipped
entirely above DEBUG, (d) that streamed SQL is truncated the same way
``execute_query`` truncates it, and (e) that ``log_query=False`` silences the
new completion lines too, not just the pre-execution ones.

Uses a real DuckDB connection (via ``DuckDBDataSourceImpl.from_existing_cursor``)
rather than mocks, so `cursor.fetchall()`/`fetchone()` behavior is real.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import duckdb
import pytest
from soda_core.common.data_source_connection import DataSourceConnection, QueryCounters
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

SODA_LOGGER_NAME = "soda"


@pytest.fixture
def connection() -> DataSourceConnection:
    conn = duckdb.connect(":memory:")
    conn.sql("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
    data_source_impl = DuckDBDataSourceImpl.from_existing_cursor(conn, "TEST_DS")
    data_source_impl.open_connection()
    yield data_source_impl.data_source_connection
    data_source_impl.close_connection()


def _debug_records(caplog, substring: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == SODA_LOGGER_NAME and substring in r.getMessage()]


class TestQueryCounters:
    """Task 2: a small, dumb, public per-connection counters object."""

    def test_starts_at_zero(self, connection):
        assert connection.query_counters.query_count == 0
        assert connection.query_counters.total_duration_seconds == 0.0

    def test_execute_query_increments_counters(self, connection):
        connection.execute_query("SELECT * FROM t")

        assert connection.query_counters.query_count == 1
        assert connection.query_counters.total_duration_seconds >= 0.0

    def test_execute_update_increments_counters(self, connection):
        connection.execute_update("CREATE TABLE u AS SELECT 1")

        assert connection.query_counters.query_count == 1

    def test_execute_query_one_by_one_increments_counters(self, connection):
        connection.execute_query_one_by_one("SELECT * FROM t", lambda row, desc: None)

        assert connection.query_counters.query_count == 1

    def test_execute_query_iterate_increments_counters(self, connection):
        with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
            list(result_iterator)

        assert connection.query_counters.query_count == 1

    def test_counters_accumulate_across_queries(self, connection):
        connection.execute_query("SELECT * FROM t")
        connection.execute_query("SELECT * FROM t")

        assert connection.query_counters.query_count == 2


class TestQueryCountersSnapshot:
    """Task 4: no reset() (shared-connection footgun) — snapshot-and-subtract instead."""

    def test_snapshot_is_an_independent_copy(self, connection):
        connection.execute_query("SELECT * FROM t")
        snapshot = connection.query_counters.snapshot()
        assert snapshot is not connection.query_counters
        assert isinstance(snapshot, QueryCounters)

        connection.execute_query("SELECT * FROM t")

        # Later queries don't retroactively mutate an already-taken snapshot.
        assert snapshot.query_count == 1
        assert connection.query_counters.query_count == 2

    def test_snapshot_and_subtract_yields_per_scan_totals(self, connection):
        connection.execute_query("SELECT * FROM t")  # pre-scan activity on a shared connection
        before = connection.query_counters.snapshot()

        connection.execute_query("SELECT * FROM t")
        connection.execute_query("SELECT * FROM t")
        after = connection.query_counters.snapshot()

        assert after.query_count - before.query_count == 2
        assert after.total_duration_seconds - before.total_duration_seconds >= 0.0


class TestQueryTimingAndRowCountLogging:
    """Task 1: one DEBUG record per query carrying duration + row count."""

    def test_execute_query_logs_duration_and_row_count(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_query("SELECT * FROM t")

        records = _debug_records(caplog, "SQL query result")
        assert len(records) == 1
        message = records[0].getMessage()
        assert "3 rows" in message
        assert any(part.endswith("s") and part[:-1].replace(".", "", 1).isdigit() for part in message.split())

    def test_execute_update_logs_duration_and_row_count(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_update("DELETE FROM t WHERE id = 1")

        # First record is the pre-execution "about to run" log; the last one
        # carries the outcome (duration + affected-row count). DuckDB's cursor
        # doesn't report a reliable rowcount for DML (always -1, which
        # ``_cursor_execute_update_and_commit`` normalizes to 0) — the exact
        # count is pinned separately below with a fake cursor.
        records = _debug_records(caplog, "SQL update affected")
        assert len(records) == 1
        assert "0 rows" in records[0].getMessage()

    def test_execute_update_logs_the_cursor_reported_row_count(self, connection, caplog, monkeypatch):
        """Pins the count against a cursor.rowcount DuckDB can't produce (MINOR 8)."""
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)
        fake_cursor = MagicMock()
        fake_cursor.rowcount = 2
        fake_dbapi_connection = MagicMock()
        fake_dbapi_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(connection, "connection", fake_dbapi_connection)

        rowcount = connection.execute_update("DELETE FROM t WHERE id IN (1, 2)")

        assert rowcount == 2
        records = _debug_records(caplog, "SQL update affected 2 rows")
        assert len(records) == 1

    def test_execute_query_one_by_one_logs_rows_processed_at_completion(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_query_one_by_one("SELECT * FROM t", lambda row, desc: None)

        records = _debug_records(caplog, "processed 3 rows")
        assert len(records) == 1, f"expected one completion record, got: {[r.getMessage() for r in caplog.records]}"

    def test_execute_query_iterate_logs_rows_yielded_at_completion(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
            list(result_iterator)

        records = _debug_records(caplog, "yielded 3 rows")
        assert len(records) == 1, f"expected one completion record, got: {[r.getMessage() for r in caplog.records]}"

    def test_execute_query_iterate_logs_partial_rows_yielded_when_caller_stops_early(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
            next(result_iterator)  # only consume one of the three rows

        records = _debug_records(caplog, "yielded 1 rows")
        assert len(records) == 1, f"expected a 1-row completion record, got: {[r.getMessage() for r in caplog.records]}"


class TestCountersAndCompletionLinesRecordedEvenOnFailure:
    """Task/IMPORTANT-2: a query that ran a while then failed is exactly what a
    timing summary should surface — counters (and, where the count is known
    incrementally rather than only on success, the completion line) fire from
    ``finally``, not just on the happy path."""

    def test_execute_query_failure_still_counted(self, connection):
        with pytest.raises(Exception):
            connection.execute_query("SELECT * FROM this_table_does_not_exist")

        assert connection.query_counters.query_count == 1

    def test_execute_update_failure_still_counted(self, connection):
        with pytest.raises(Exception):
            connection.execute_update("DELETE FROM this_table_does_not_exist")

        assert connection.query_counters.query_count == 1

    def test_execute_query_one_by_one_failure_still_counted(self, connection):
        def bad_callback(row, desc):
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            connection.execute_query_one_by_one("SELECT * FROM t", bad_callback)

        assert connection.query_counters.query_count == 1

    def test_execute_query_one_by_one_completion_line_reports_partial_count_on_failure(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        def fail_on_second_row(row, desc):
            fail_on_second_row.calls += 1
            if fail_on_second_row.calls == 2:
                raise RuntimeError("boom")

        fail_on_second_row.calls = 0

        with pytest.raises(RuntimeError):
            connection.execute_query_one_by_one("SELECT * FROM t", fail_on_second_row)

        # rows_processed is tracked incrementally (unlike execute_query's row
        # count, which is only known on success), so the completion line still
        # fires with the count reached before the failure.
        records = _debug_records(caplog, "processed 2 rows")
        assert len(records) == 1, f"got: {[r.getMessage() for r in caplog.records]}"

    def test_execute_query_iterate_failure_still_counted(self, connection):
        with pytest.raises(RuntimeError):
            with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
                next(result_iterator)
                raise RuntimeError("boom")

        assert connection.query_counters.query_count == 1

    def test_execute_query_iterate_completion_line_reports_partial_count_on_failure(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        with pytest.raises(RuntimeError):
            with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
                next(result_iterator)
                raise RuntimeError("boom")

        records = _debug_records(caplog, "yielded 1 rows")
        assert len(records) == 1, f"got: {[r.getMessage() for r in caplog.records]}"


class TestLogQueryFlagSilencesCompletionLines:
    """MINOR 6: log_query=False must silence the new completion lines too, not
    just the pre-execution ones — counting still happens either way."""

    def test_execute_query_log_query_false_suppresses_all_debug_lines(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_query("SELECT * FROM t", log_query=False)

        assert _debug_records(caplog, "SQL query") == []
        assert connection.query_counters.query_count == 1

    def test_execute_update_log_query_false_suppresses_completion_line(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_update("CREATE TABLE u2 AS SELECT 1", log_query=False)

        assert _debug_records(caplog, "SQL update") == []
        assert connection.query_counters.query_count == 1

    def test_execute_query_one_by_one_log_query_false_suppresses_completion_line(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_query_one_by_one("SELECT * FROM t", lambda row, desc: None, log_query=False)

        assert _debug_records(caplog, "one-by-one") == []
        assert connection.query_counters.query_count == 1

    def test_execute_query_iterate_log_query_false_suppresses_completion_line(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        with connection.execute_query_iterate("SELECT * FROM t", log_query=False) as result_iterator:
            list(result_iterator)

        assert _debug_records(caplog, "iterate") == []
        assert connection.query_counters.query_count == 1


class TestDebugFormattingGuard:
    """Task 3: tabulate/truncate must not run unless DEBUG is enabled."""

    def test_tabulate_not_called_when_debug_disabled(self, connection, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger=SODA_LOGGER_NAME)
        tabulate_spy = MagicMock(side_effect=AssertionError("tabulate should not run above DEBUG"))
        monkeypatch.setattr("soda_core.common.data_source_connection.tabulate", tabulate_spy)

        connection.execute_query("SELECT * FROM t")  # must not raise

        tabulate_spy.assert_not_called()

    def test_truncate_rows_not_called_when_debug_disabled(self, connection, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger=SODA_LOGGER_NAME)
        truncate_spy = MagicMock(wraps=connection.truncate_rows)
        monkeypatch.setattr(connection, "truncate_rows", truncate_spy)

        connection.execute_query("SELECT * FROM t")

        truncate_spy.assert_not_called()

    def test_tabulate_called_when_debug_enabled(self, connection, monkeypatch, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)
        from tabulate import tabulate as real_tabulate

        tabulate_spy = MagicMock(wraps=real_tabulate)
        monkeypatch.setattr("soda_core.common.data_source_connection.tabulate", tabulate_spy)

        connection.execute_query("SELECT * FROM t")

        tabulate_spy.assert_called_once()

    def test_returned_rows_unaffected_by_debug_level(self, connection, caplog):
        caplog.set_level(logging.INFO, logger=SODA_LOGGER_NAME)

        result = connection.execute_query("SELECT * FROM t")

        # _format_rows() still runs (needed for the return value) even though
        # the debug-only truncate/tabulate formatting is skipped.
        assert len(result.rows) == 3

    def test_debug_formatting_does_not_crash_when_cursor_description_is_none(self, connection, monkeypatch, caplog):
        """MINOR 9: some drivers can return a None description; the DEBUG-only
        header-building must not newly crash on that."""
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)
        fake_cursor = MagicMock()
        fake_cursor.description = None
        fake_cursor.fetchall.return_value = [(1,), (2,)]
        fake_dbapi_connection = MagicMock()
        fake_dbapi_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(connection, "connection", fake_dbapi_connection)

        result = connection.execute_query("SELECT 1")  # must not raise

        assert len(result.rows) == 2


class TestSqlTruncationOnStreamingPaths:
    """Task 4: one-by-one and iterate truncate logged SQL like execute_query does."""

    def test_execute_query_one_by_one_truncates_logged_sql(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)
        long_comment = "x" * (DataSourceConnection.MAX_CHARS_PER_SQL + 500)
        sql = f"SELECT * FROM t -- {long_comment}"

        connection.execute_query_one_by_one(sql, lambda row, desc: None)

        pre_execution_records = [
            r
            for r in caplog.records
            if r.name == SODA_LOGGER_NAME and "one-by-one" in r.getMessage() and "SELECT" in r.getMessage()
        ]
        assert len(pre_execution_records) == 1
        assert len(pre_execution_records[0].getMessage()) < len(sql)
        assert sql not in pre_execution_records[0].getMessage()
        assert "TEST_DS" in pre_execution_records[0].getMessage()  # datasource name still present (IMPORTANT 5)

    def test_execute_query_iterate_truncates_logged_sql(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)
        long_comment = "x" * (DataSourceConnection.MAX_CHARS_PER_SQL + 500)
        sql = f"SELECT * FROM t -- {long_comment}"

        with connection.execute_query_iterate(sql) as result_iterator:
            list(result_iterator)

        pre_execution_records = [
            r
            for r in caplog.records
            if r.name == SODA_LOGGER_NAME and "iterate" in r.getMessage() and "SELECT" in r.getMessage()
        ]
        assert len(pre_execution_records) == 1
        assert len(pre_execution_records[0].getMessage()) < len(sql)
        assert sql not in pre_execution_records[0].getMessage()
        assert "TEST_DS" in pre_execution_records[0].getMessage()  # datasource name still present (IMPORTANT 5)
