from __future__ import annotations

import contextlib
import logging
import os
from collections import namedtuple
from collections.abc import Iterator
from typing import Any, Callable, Optional

from helpers.snapshot_manager import SnapshotEntry, SnapshotManager, SnapshotMismatchError, SnapshotNotFoundError
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult, QueryResultIterator, UpdateResult
from soda_core.common.logging_constants import soda_logger

logger = logging.getLogger(__name__)
# Use the same logger as DataSourceConnection so that replayed SQL appears in Soda Logs.
sql_logger = soda_logger

# A picklable replacement for DBAPI cursor.description column entries.
# DB drivers like psycopg3 use C-extension Column objects that can't be pickled.
# This namedtuple preserves both attribute access (.name) and index access ([0]).
PicklableColumn = namedtuple(
    "PicklableColumn", ["name", "type_code", "display_size", "internal_size", "precision", "scale", "null_ok"]
)

# Sentinel object used as a fake DBAPI connection in replay mode.
# Satisfies the `connection is not None` check in DataSourceImpl.has_open_connection().
_SENTINEL = object()


class FakeCursor:
    """A minimal cursor-like object that replays cached rows.

    Used to construct a QueryResultIterator in replay mode without a real DB cursor.
    """

    def __init__(self, rows: list, description: Any, rowcount: int):
        self._rows = list(rows)
        self._index = 0
        self.description = description
        self.rowcount = rowcount

    def fetchone(self):
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self):
        remaining = self._rows[self._index :]
        self._index = len(self._rows)
        return remaining

    def close(self):
        pass


class SnapshotDataSourceConnection(DataSourceConnection):
    """A DataSourceConnection wrapper that records or replays SQL operations.

    In record mode: delegates to the real connection and captures all SQL + results.
    In replay mode: returns cached results without any database interaction.

    Test boundaries are detected automatically via the PYTEST_CURRENT_TEST environment
    variable, so no changes to test code or fixtures are required.
    """

    def __init__(
        self,
        real_connection: Optional[DataSourceConnection],
        snapshot_manager: SnapshotManager,
        mode: str,
    ):
        # Pass the real DBAPI connection (or sentinel) so that open_connection() in
        # DataSourceConnection.__init__ is a no-op (it skips when self.connection is not None).
        dbapi_conn = real_connection.connection if real_connection else _SENTINEL
        super().__init__(name="snapshot", connection_properties={}, connection=dbapi_conn)

        self._real: Optional[DataSourceConnection] = real_connection
        self._snapshot_manager: SnapshotManager = snapshot_manager
        self._mode: str = mode  # "record" or "replay"

        # Per-test state
        self._current_test_id: Optional[str] = None
        self._recording: list[SnapshotEntry] = []
        self._replay_data: Optional[list[SnapshotEntry]] = None
        self._replay_index: int = 0
        self._fallback_active: bool = False

    def _create_connection(self, connection_properties: dict) -> object:
        # Never called because we always pass a non-None connection to __init__.
        return None

    # -------------------------------------------------------------------------
    # Test boundary detection via PYTEST_CURRENT_TEST
    # -------------------------------------------------------------------------

    def _get_current_test_id(self) -> Optional[str]:
        """Extract the current test ID from pytest's environment variable.

        PYTEST_CURRENT_TEST format: "path/to/test.py::TestClass::test_func (setup|call|teardown)"
        We strip the phase suffix to get a stable ID across setup/call/teardown.
        """
        raw = os.environ.get("PYTEST_CURRENT_TEST", "")
        if not raw:
            return None
        # Strip the " (phase)" suffix
        return raw.rsplit(" ", 1)[0] if " " in raw else raw

    def _handle_test_boundary(self) -> None:
        """Detect when the current test changes and handle recording/replay transitions."""
        test_id = self._get_current_test_id()
        if test_id == self._current_test_id:
            return  # Same test, nothing to do

        # Finalize the previous test's recording
        self._finalize_current_test()

        # Start tracking the new test
        self._current_test_id = test_id
        if test_id is None:
            return

        if self._mode == "replay":
            self._replay_data = self._snapshot_manager.load(test_id)
            if self._replay_data is None:
                if self._real is not None:
                    logger.warning(f"SNAPSHOT: No snapshot for {test_id}, falling back to real DB")
                    self._fallback_active = True
                else:
                    snapshot_path = self._snapshot_manager._snapshot_path(test_id, "pickle")
                    raise SnapshotNotFoundError(
                        f"No snapshot found for test: {test_id}\n"
                        f"  Expected at: {snapshot_path}\n"
                        f"  To record snapshots, run: SODA_TEST_SNAPSHOT=record pytest ..."
                    )
            self._replay_index = 0
            logger.info(f"SNAPSHOT: Replaying {len(self._replay_data or [])} operations for {test_id}")
        elif self._mode == "record":
            self._recording = []
            logger.info(f"SNAPSHOT: Recording SQL for {test_id}")

    def _finalize_current_test(self) -> None:
        """Save the current test's recording (if any) and reset per-test state."""
        if self._current_test_id is not None and self._mode == "record" and self._recording:
            self._snapshot_manager.save(self._current_test_id, self._recording)
        self._recording = []
        self._replay_data = None
        self._replay_index = 0
        self._fallback_active = False

    def finalize(self) -> None:
        """Called at end of session to save the last test's recording."""
        self._finalize_current_test()
        self._current_test_id = None

    # -------------------------------------------------------------------------
    # Replay helpers
    # -------------------------------------------------------------------------

    def _activate_fallback(self) -> None:
        """Fall back to real DB for the current test.

        Re-executes all previously replayed operations against the real DB to
        set up database state (tables, inserts), then switches to passthrough
        mode for all remaining operations in this test.
        """
        if self._real is None:
            raise RuntimeError(
                "Cannot fall back to real DB — no real connection available.\n"
                "  To enable fallback, ensure the real connection is provided in replay mode."
            )

        ops_to_replay = self._replay_index
        logger.warning(
            f"SNAPSHOT: Falling back to real DB for {self._current_test_id} "
            f"(re-executing {ops_to_replay} previous operations)"
        )

        # Re-execute all previously replayed operations to set up DB state.
        # UPDATEs create tables/insert data; QUERYs are harmless but executed
        # for completeness.
        for i in range(ops_to_replay):
            entry = self._replay_data[i]
            if entry.op_type == "update":
                self._real.execute_update(entry.sql, log_query=False)
            else:
                # query, query_one_by_one, query_iterate — read-only, just run as query
                self._real.execute_query(entry.sql, log_query=False)

        self._fallback_active = True

    def _next_replay_entry(self, expected_type: str, expected_sql: str) -> SnapshotEntry:
        """Get the next entry from the replay data and verify it matches."""
        if self._replay_data is None or self._replay_index >= len(self._replay_data):
            raise SnapshotMismatchError(
                f"Snapshot exhausted for test {self._current_test_id}.\n"
                f"  No more operations in snapshot (had {len(self._replay_data or [])}).\n"
                f"  Next operation would be: {expected_type} {expected_sql[:200]}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ..."
            )
        entry = self._replay_data[self._replay_index]
        if entry.op_type != expected_type or entry.sql != expected_sql:
            raise SnapshotMismatchError(
                f"Snapshot mismatch at operation #{self._replay_index} for test {self._current_test_id}.\n"
                f"  Expected ({entry.op_type}): {entry.sql[:200]}\n"
                f"  Got      ({expected_type}): {expected_sql[:200]}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ..."
            )
        self._replay_index += 1
        return entry

    # -------------------------------------------------------------------------
    # Pickle normalization — convert non-picklable DB driver objects
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize_description(description: Any) -> Optional[tuple[PicklableColumn, ...]]:
        """Convert cursor.description to picklable namedtuples.

        DB drivers (e.g. psycopg3) use C-extension Column objects that fail to pickle.
        This converts them to plain namedtuples while preserving both .name attribute
        access and [0] index access.
        """
        if description is None:
            return None
        return tuple(
            PicklableColumn(
                name=col[0],
                type_code=col[1],
                display_size=col[2] if len(col) > 2 else None,
                internal_size=col[3] if len(col) > 3 else None,
                precision=col[4] if len(col) > 4 else None,
                scale=col[5] if len(col) > 5 else None,
                null_ok=col[6] if len(col) > 6 else None,
            )
            for col in description
        )

    @staticmethod
    def _normalize_rows(rows: list) -> list[tuple]:
        """Convert rows to plain tuples.

        DB drivers like pyodbc return Row objects (C extension) that can't be pickled.
        Converting to plain tuples ensures pickle compatibility.
        """
        return [tuple(row) for row in rows]

    @classmethod
    def _normalize_query_result(cls, result: QueryResult) -> QueryResult:
        """Create a picklable copy of a QueryResult with normalized column descriptions and rows."""
        return QueryResult(
            rows=cls._normalize_rows(result.rows),
            columns=cls._normalize_description(result.columns),
        )

    def _passthrough_or_fail(self, method_name: str, sql: str, **kwargs):
        """For session-level SQL (no test context): pass through to real connection or fail."""
        if self._real is not None:
            return getattr(self._real, method_name)(sql, **kwargs)
        raise RuntimeError(
            f"No real connection available and no test context for snapshot.\n"
            f"  Method: {method_name}, SQL: {sql[:200]}"
        )

    # -------------------------------------------------------------------------
    # SQL execution methods — the core interception points
    # -------------------------------------------------------------------------

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        self._handle_test_boundary()

        if self._current_test_id is None:
            # Session-level SQL (before/after tests) — pass through to real connection
            return self._passthrough_or_fail("execute_query", sql, log_query=log_query)

        if self._mode == "record":
            result = self._real.execute_query(sql, log_query=log_query)
            # Store a normalized copy (picklable), return original to caller
            self._recording.append(SnapshotEntry("query", sql, self._normalize_query_result(result)))
            return result
        else:  # replay
            if self._fallback_active:
                return self._real.execute_query(sql, log_query=log_query)
            try:
                entry = self._next_replay_entry("query", sql)
                if log_query:
                    sql_logger.debug(
                        f"SQL query fetchall in datasource {self.name} "
                        f"(first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}"
                    )
                return entry.result
            except SnapshotMismatchError:
                self._activate_fallback()
                return self._real.execute_query(sql, log_query=log_query)

    def execute_update(self, sql: str, log_query: bool = True) -> UpdateResult:
        self._handle_test_boundary()

        if self._current_test_id is None:
            return self._passthrough_or_fail("execute_update", sql, log_query=log_query)

        if self._mode == "record":
            result = self._real.execute_update(sql, log_query=log_query)
            # Store None as the result — execute_update return values are driver-specific
            # (e.g. psycopg3 returns the cursor itself) and rarely used by callers.
            self._recording.append(SnapshotEntry("update", sql, None))
            return result
        else:  # replay
            if self._fallback_active:
                return self._real.execute_update(sql, log_query=log_query)
            try:
                self._next_replay_entry("update", sql)
                return None
            except SnapshotMismatchError:
                self._activate_fallback()
                return self._real.execute_update(sql, log_query=log_query)

    def execute_query_one_by_one(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        self._handle_test_boundary()

        if self._current_test_id is None:
            if self._real is not None:
                return self._real.execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)
            raise RuntimeError("No real connection and no test context for snapshot")

        if self._mode == "record":
            captured_rows = []

            def capturing_callback(row, description):
                captured_rows.append(tuple(row))
                row_callback(row, description)

            description = self._real.execute_query_one_by_one(
                sql, capturing_callback, log_query=log_query, row_limit=row_limit
            )
            self._recording.append(
                SnapshotEntry("query_one_by_one", sql, (self._normalize_description(description), captured_rows))
            )
            return description
        else:  # replay
            if self._fallback_active:
                return self._real.execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)
            try:
                entry = self._next_replay_entry("query_one_by_one", sql)
                description, rows = entry.result
                rows_processed = 0
                for row in rows:
                    if row_limit is not None and rows_processed >= row_limit:
                        break
                    rows_processed += 1
                    row_callback(row, description)
                return description
            except SnapshotMismatchError:
                self._activate_fallback()
                return self._real.execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)

    @contextlib.contextmanager
    def execute_query_iterate(self, sql: str, log_query: bool = True) -> Iterator[QueryResultIterator]:
        self._handle_test_boundary()

        if self._current_test_id is None:
            if self._real is not None:
                with self._real.execute_query_iterate(sql, log_query=log_query) as iterator:
                    yield iterator
                return
            raise RuntimeError("No real connection and no test context for snapshot")

        if self._mode == "record":
            # Eagerly consume all rows from the real connection so we can cache them.
            with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                cursor_description = real_iter._cursor.description
                rows = [tuple(row) for row in real_iter]
            normalized_desc = self._normalize_description(cursor_description)
            self._recording.append(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
            # Yield a fake iterator backed by cached data
            try:
                yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)))
            finally:
                pass
        else:  # replay
            if self._fallback_active:
                with self._real.execute_query_iterate(sql, log_query=log_query) as iterator:
                    yield iterator
                return
            try:
                entry = self._next_replay_entry("query_iterate", sql)
                rows, cursor_description = entry.result
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)))
                finally:
                    pass
            except SnapshotMismatchError:
                self._activate_fallback()
                with self._real.execute_query_iterate(sql, log_query=log_query) as iterator:
                    yield iterator

    # -------------------------------------------------------------------------
    # Transaction and connection lifecycle — pass through or no-op
    # -------------------------------------------------------------------------

    def commit(self) -> None:
        if self._real is not None:
            self._real.commit()
        # In replay mode: no-op

    def rollback(self) -> None:
        if self._real is not None:
            self._real.rollback()
        # In replay mode: no-op

    def open_connection(self) -> None:
        # Connection is already set up in __init__. Do not re-open.
        pass

    def close_connection(self) -> None:
        self.finalize()
        if self._real is not None:
            self._real.close_connection()
        self.connection = None

    def format_rows(self, rows: list[tuple]) -> list[tuple]:
        if self._real is not None:
            return self._real.format_rows(rows)
        return rows
