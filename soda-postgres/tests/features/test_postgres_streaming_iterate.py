"""Real-postgres coverage for ``execute_query_iterate(streaming=True)``.

The unit tests (soda-tests/tests/unit/test_query_result_iterator.py) check the call
shape with a fake cursor. These tests check what only a live postgres can show: the
named cursor returns every row in order across a thin/fat mix, it does not survive a
commit, autocommit is refused, and a failed query leaves the connection usable.
"""

from __future__ import annotations

import psycopg
import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.exceptions import SodaCoreException

_N_ROWS = 25
_FAT_INDEX = 12
_FAT_BYTES = 2 * 1024 * 1024


@pytest.fixture(autouse=True)
def _disable_memory_optimized_driver(monkeypatch):
    # The streaming read is asked for explicitly, so it must not need this toggle.
    monkeypatch.delenv("MEMORY_OPTIMIZED_DRIVER_ENABLED", raising=False)


def _spec() -> TestTableSpecification:
    rows = [(i, ("F" * _FAT_BYTES) if i == _FAT_INDEX else f"thin_{i}") for i in range(_N_ROWS)]
    return (
        TestTableSpecification.builder()
        .table_purpose("postgres_streaming_iterate")
        .column_integer("id")
        .column_unbounded_text("payload")
        .rows(rows=rows)
        .build()
    )


def _select_sql(data_source_test_helper: DataSourceTestHelper) -> str:
    test_table = data_source_test_helper.ensure_test_table(_spec())
    qualified_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)
    id_col = data_source_test_helper.quote_column("id")
    payload_col = data_source_test_helper.quote_column("payload")
    return f"SELECT {id_col}, {payload_col} FROM {qualified_name} ORDER BY {id_col}"


def test_returns_every_row_in_order_in_batches(data_source_test_helper: DataSourceTestHelper):
    sql = _select_sql(data_source_test_helper)
    data_source_impl = data_source_test_helper.data_source_impl

    with data_source_impl.execute_query_iterate(sql, streaming=True) as result:
        assert list(result.columns) == ["id", "payload"]
        batches = list(result.batches(10))

    assert [len(batch) for batch in batches] == [10, 10, 5]
    rows = [row for batch in batches for row in batch]
    assert [row[0] for row in rows] == list(range(_N_ROWS))
    assert len(rows[_FAT_INDEX][1]) == _FAT_BYTES
    assert rows[0][1] == "thin_0"


def test_a_commit_before_the_last_row_ends_the_read_with_an_error(data_source_test_helper: DataSourceTestHelper):
    sql = _select_sql(data_source_test_helper)
    data_source_impl = data_source_test_helper.data_source_impl

    with pytest.raises(psycopg.errors.Error):
        with data_source_impl.execute_query_iterate(sql, streaming=True) as result:
            next(result)
            data_source_impl.connection.commit()
            list(result)

    # The connection is usable again afterwards.
    assert data_source_impl.execute_query("SELECT 1").rows == [(1,)]


def test_autocommit_is_refused(data_source_test_helper: DataSourceTestHelper):
    sql = _select_sql(data_source_test_helper)
    data_source_impl = data_source_test_helper.data_source_impl
    raw_connection = data_source_impl.connection.connection

    raw_connection.rollback()  # no open transaction, so autocommit can be set
    raw_connection.autocommit = True
    try:
        with pytest.raises(SodaCoreException, match="autocommit"):
            with data_source_impl.execute_query_iterate(sql, streaming=True):
                pass  # pragma: no cover - never entered
    finally:
        raw_connection.autocommit = False


def test_a_failed_query_rolls_back(data_source_test_helper: DataSourceTestHelper):
    data_source_impl = data_source_test_helper.data_source_impl

    with pytest.raises(psycopg.errors.Error):
        with data_source_impl.execute_query_iterate("SELECT * FROM table_that_does_not_exist", streaming=True):
            pass  # pragma: no cover - never entered

    assert data_source_impl.execute_query("SELECT 1").rows == [(1,)]
