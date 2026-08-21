"""Unit tests for query timing, per-connection counters, and DEBUG-formatting
guards on ``DataSourceConnection``.

Extension scans (metric monitoring, profiling) log through this engine but
today get no query timing, no row-count logging, and pay the cost of
tabulate/truncation formatting on every query even when DEBUG is off. These
tests pin: (a) exactly one DEBUG record with duration + row count per query
(rows yielded/processed at completion for the streaming variants), (b) the
public per-connection ``query_counters`` accumulator extensions read to build
scan summaries, (c) that the expensive row-formatting helpers are skipped
entirely above DEBUG, and (d) that streamed SQL is truncated the same way
``execute_query`` truncates it.

Uses a real DuckDB connection (via ``DuckDBDataSourceImpl.from_existing_cursor``)
rather than mocks, so `cursor.fetchall()`/`fetchone()` behavior is real.
"""

from __future__ import annotations

import logging
import re
from unittest.mock import MagicMock

import duckdb
import pytest
from soda_core.common.data_source_connection import DataSourceConnection
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


class TestQueryTimingAndRowCountLogging:
    """Task 1: one DEBUG record per query carrying duration + row count."""

    def test_execute_query_logs_duration_and_row_count(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_query("SELECT * FROM t")

        records = _debug_records(caplog, "SQL query result")
        assert len(records) == 1
        message = records[0].getMessage()
        assert re.search(r"\d+\.\d+s", message), f"no duration in: {message}"
        assert "3 rows" in message

    def test_execute_update_logs_duration_and_row_count(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_update("DELETE FROM t WHERE id = 1")

        records = _debug_records(caplog, "SQL update")
        # First record is the pre-execution "about to run" log; the last one
        # carries the outcome (duration + affected-row count). DuckDB's cursor
        # doesn't report a reliable rowcount for DML (always -1, which
        # ``_cursor_execute_update_and_commit`` normalizes to 0), so assert the
        # count is present in the message rather than pinning a specific value.
        assert len(records) >= 1
        outcome_message = records[-1].getMessage()
        assert re.search(r"\d+\.\d+s", outcome_message), f"no duration in: {outcome_message}"
        assert re.search(r"\d+ rows?", outcome_message), f"no row count in: {outcome_message}"

    def test_execute_query_one_by_one_logs_rows_processed_at_completion(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        connection.execute_query_one_by_one("SELECT * FROM t", lambda row, desc: None)

        records = _debug_records(caplog, "one-by-one")
        completion_records = [r for r in records if "3" in r.getMessage() and re.search(r"\d+\.\d+s", r.getMessage())]
        assert len(completion_records) == 1, f"expected one completion record, got: {[r.getMessage() for r in records]}"

    def test_execute_query_iterate_logs_rows_yielded_at_completion(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
            list(result_iterator)

        records = _debug_records(caplog, "iterate")
        completion_records = [r for r in records if "3" in r.getMessage() and re.search(r"\d+\.\d+s", r.getMessage())]
        assert len(completion_records) == 1, f"expected one completion record, got: {[r.getMessage() for r in records]}"

    def test_execute_query_iterate_logs_partial_rows_yielded_when_caller_stops_early(self, connection, caplog):
        caplog.set_level(logging.DEBUG, logger=SODA_LOGGER_NAME)

        with connection.execute_query_iterate("SELECT * FROM t") as result_iterator:
            next(result_iterator)  # only consume one of the three rows

        records = _debug_records(caplog, "iterate")
        completion_records = [r for r in records if "1" in r.getMessage() and re.search(r"\d+\.\d+s", r.getMessage())]
        assert (
            len(completion_records) == 1
        ), f"expected a 1-row completion record, got: {[r.getMessage() for r in records]}"


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
