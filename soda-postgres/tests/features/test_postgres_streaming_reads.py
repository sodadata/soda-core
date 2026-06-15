"""Real-postgres coverage for ``execute_query_one_by_one_memory_optimized``.

The unit tests (soda-tests/tests/unit/test_memory_optimized_query.py) drive
this path with mocks — they pin the call shape (server-side named cursor,
``withhold=True``, byte-budgeted ``fetchmany`` ramp, rollback-then-CLOSE on
error) but cannot exercise the actual postgres cursor semantics. These tests
run against a live postgres so the documented behaviours are verified for
real: the byte-budgeted stream returns every row in order across a thin/fat
mix, a ``withhold=True`` cursor survives a commit mid-iteration, the
autocommit path falls back to the buffered base, and the
rollback-before-the-cursor-is-held limitation actually trips.
"""

from __future__ import annotations

import psycopg
import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

_N_ROWS = 25
_FAT_INDEX = 12
_FAT_BYTES = 2 * 1024 * 1024  # exceeds nothing fatal, but shrinks the fetch batch


def _streaming_spec() -> TestTableSpecification:
    rows = []
    for i in range(_N_ROWS):
        payload = ("F" * _FAT_BYTES) if i == _FAT_INDEX else f"thin_{i}"
        rows.append((i, payload))
    return (
        TestTableSpecification.builder()
        .table_purpose("postgres_streaming_reads")
        .column_integer("id")
        .column_unbounded_text("payload")
        .rows(rows=rows)
        .build()
    )


def _select_sql(data_source_test_helper: DataSourceTestHelper, test_table) -> str:
    qualified_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)
    id_col = data_source_test_helper.quote_column("id")
    payload_col = data_source_test_helper.quote_column("payload")
    return f"SELECT {id_col}, {payload_col} FROM {qualified_name} ORDER BY {id_col}"


def test_streaming_returns_every_row_in_order_for_thin_fat_mix(data_source_test_helper: DataSourceTestHelper):
    """Server-side cursor + byte-budgeted fetchmany must deliver all rows, in
    order, regardless of the batch ramping up on thin rows and shrinking on the
    fat one."""
    test_table = data_source_test_helper.ensure_test_table(_streaming_spec())
    sql = _select_sql(data_source_test_helper, test_table)

    seen: list[tuple] = []
    description = data_source_test_helper.data_source_impl.execute_query_one_by_one_memory_optimized(
        sql=sql, row_callback=lambda row, desc: seen.append(row)
    )

    assert description is not None
    assert [row[0] for row in seen] == list(range(_N_ROWS))  # order preserved
    assert len(seen[_FAT_INDEX][1]) == _FAT_BYTES  # the fat payload streamed intact
    assert seen[0][1] == "thin_0"


def test_withhold_cursor_survives_commit_mid_iteration(data_source_test_helper: DataSourceTestHelper):
    """The DWH pump commits on the same connection between flushes; withhold=True
    is what keeps the source cursor alive across that commit. Commit from inside
    the row callback and assert the remaining rows still arrive."""
    test_table = data_source_test_helper.ensure_test_table(_streaming_spec())
    sql = _select_sql(data_source_test_helper, test_table)
    raw_connection = data_source_test_helper.data_source_impl.connection.connection

    seen: list[tuple] = []

    def commit_after_first_row(row: tuple, description) -> None:
        seen.append(row)
        if len(seen) == 1:
            raw_connection.commit()  # would kill a non-withhold cursor

    data_source_test_helper.data_source_impl.execute_query_one_by_one_memory_optimized(
        sql=sql, row_callback=commit_after_first_row
    )

    assert [row[0] for row in seen] == list(range(_N_ROWS))


def test_autocommit_connection_falls_back_to_buffered(data_source_test_helper: DataSourceTestHelper):
    """Server-side cursors can't be created in autocommit mode; the method must
    fall back to the buffered base implementation and still return every row."""
    test_table = data_source_test_helper.ensure_test_table(_streaming_spec())
    sql = _select_sql(data_source_test_helper, test_table)
    raw_connection = data_source_test_helper.data_source_impl.connection.connection

    raw_connection.rollback()  # no open transaction, so autocommit can be set
    raw_connection.autocommit = True
    try:
        seen: list[tuple] = []
        data_source_test_helper.data_source_impl.execute_query_one_by_one_memory_optimized(
            sql=sql, row_callback=lambda row, desc: seen.append(row)
        )
        assert {row[0] for row in seen} == set(range(_N_ROWS))
    finally:
        raw_connection.autocommit = False


def test_rollback_before_cursor_held_breaks_the_stream(data_source_test_helper: DataSourceTestHelper):
    """Documented limitation: a ROLLBACK before the withhold cursor's first
    commit destroys the not-yet-held cursor, so the next fetch raises. Verify
    the real postgres behaviour (a rollback from inside the callback, before any
    commit has held the cursor) surfaces as a psycopg error."""
    test_table = data_source_test_helper.ensure_test_table(_streaming_spec())
    sql = _select_sql(data_source_test_helper, test_table)
    raw_connection = data_source_test_helper.data_source_impl.connection.connection

    seen: list[tuple] = []

    def rollback_after_first_row(row: tuple, description) -> None:
        seen.append(row)
        if len(seen) == 1:
            raw_connection.rollback()  # destroys the not-yet-held cursor

    with pytest.raises(psycopg.errors.Error):
        data_source_test_helper.data_source_impl.execute_query_one_by_one_memory_optimized(
            sql=sql, row_callback=rollback_after_first_row
        )
