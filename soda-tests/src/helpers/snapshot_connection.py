from __future__ import annotations

import contextlib
import logging
import os
import re
from collections import namedtuple
from collections.abc import Iterator
from typing import Any, Callable, Optional

from helpers.snapshot_manager import (
    SnapshotEntry,
    SnapshotManager,
    SnapshotMismatchError,
    SnapshotNotFoundError,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import (
    QueryResult,
    QueryResultIterator,
    UpdateResult,
)
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

# Regex matching timestamps that appear as SQL string literals.
# Matches ISO 8601 ('2026-03-16T18:24:35.151223', '2026-03-16T17:24:35+00:00')
# and Oracle-style TIMESTAMP literals (TIMESTAMP '2026-03-18 14:09:40').
_TIMESTAMP_RE = re.compile(r"(?:TIMESTAMP\s+)?'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.+\-]\S+)?'")
_TIMESTAMP_PLACEHOLDER = "'__$$__SODA_TIMESTAMP__$$__'"

# Regex matching __soda_temp_<uuid-hex> table names that use uuid4().hex.
# Case-insensitive: Snowflake (and other DBs) may uppercase identifiers.
_SODA_TEMP_RE = re.compile(r"__soda_temp_[0-9a-fA-F]{32}", re.IGNORECASE)
_SODA_TEMP_PLACEHOLDER = "__soda_temp___$$__SODA_UUID__$$__"


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
        fallback_connection_factory: Optional[Callable[[], DataSourceConnection]] = None,
        schema_placeholder: Optional[str] = None,
        real_schema_name: Optional[str] = None,
        allow_fallback: bool = False,
    ):
        # Always use _SENTINEL so that open_connection() is a no-op (skips when
        # self.connection is not None) AND so that code accessing the raw DBAPI
        # connection (e.g. _optimized_insert) fails gracefully and falls back to
        # execute_update, which the snapshot intercepts.  The snapshot's own
        # execute_query / execute_update use self._real, not self.connection.
        conn_props = real_connection.connection_properties if real_connection else {}
        super().__init__(name="snapshot", connection_properties=conn_props, connection=_SENTINEL)

        self._real: Optional[DataSourceConnection] = real_connection
        self._snapshot_manager: SnapshotManager = snapshot_manager
        self._mode: str = mode  # "record" or "replay"
        self._fallback_connection_factory: Optional[Callable[[], DataSourceConnection]] = fallback_connection_factory
        self._schema_placeholder: Optional[str] = schema_placeholder
        self._real_schema_name: Optional[str] = real_schema_name

        # Extra placeholder → real_value replacements for normalization/denormalization.
        # Callers can add entries to this dict to normalize additional dynamic values.
        self.extra_replacements: dict[str, str] = {}

        # When True, ISO 8601 timestamps in SQL are replaced with a generic placeholder
        # before comparison so that dynamic timestamps (e.g. time_created, execution_time)
        # don't cause mismatches between record and replay runs.
        self.normalize_timestamps: bool = False

        # Exact SQL → mock QueryResult mappings that bypass snapshot recording/replay.
        # These queries return the provided result directly without hitting the DB
        # or being stored in snapshots. Used for session-level queries that run
        # lazily during tests (e.g. BigQuery's SELECT @@location).
        self.passthrough_queries: dict[str, QueryResult] = {}

        # Like passthrough_queries, but only for record mode: returns the cached result
        # AND records it in the snapshot. Populated externally by DataSourceTestHelper
        # for specific queries (e.g. metadata tables query).
        self.record_mode_cached_queries: dict[str, QueryResult] = {}

        # Per-test state
        self._current_test_id: Optional[str] = None
        self._recording: list[SnapshotEntry] = []
        self._replay_data: Optional[list[SnapshotEntry]] = None
        self._replay_index: int = 0
        self._fallback_active: bool = False
        self._allow_fallback: bool = allow_fallback
        self._linked_snapshot: Optional[SnapshotDataSourceConnection] = None

    def __getattr__(self, name: str) -> Any:
        """Proxy unknown attributes to the real connection.

        Some data sources (e.g. Athena) store custom attributes on their connection
        objects (like athena_staging_dir). This ensures they remain accessible
        through the snapshot wrapper.

        If the real connection hasn't been created yet (lazy replay mode), the
        fallback factory is triggered to create it.
        """
        real = self.__dict__.get("_real")
        if real is None:
            factory = self.__dict__.get("_fallback_connection_factory")
            if factory is not None:
                real = factory()
                self._real = real
        if real is not None and hasattr(real, name):
            return getattr(real, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _execute_query_get_result_row_column_name(self, column) -> str:
        """Delegate to the real connection so data-source-specific overrides apply.

        Some drivers (SQL Server, Databricks, Athena, etc.) return plain tuples
        in cursor.description and override this method to use column[0] instead
        of column.name.  In record mode the QueryResult still carries the real
        cursor's description, so we must honour the real connection's method.
        In replay mode _real may be None but descriptions are PicklableColumn
        namedtuples which have a .name attribute, so the base fallback works.
        """
        if self._real is not None:
            return self._real._execute_query_get_result_row_column_name(column)
        return column.name

    def _create_connection(self, connection_properties: dict) -> object:
        # Never called because we always pass a non-None connection to __init__.
        return None

    @staticmethod
    def _to_quoted(s: str) -> str:
        """Convert 'a.b.c' to '"a"."b"."c"'."""
        return '"' + '"."'.join(s.split(".")) + '"'

    def _normalize_value(self, value: Any, old: str, new: str) -> Any:
        """Recursively replace real values with uniform lowercase placeholders.

        All case variants of ``old`` (upper, lower, mixed) are replaced with
        ``new.lower()`` so that stored snapshots always contain lowercase
        placeholders regardless of the database's identifier casing.

        Also handles dot-separated paths that appear as individually-quoted
        identifiers in SQL (e.g. Dremio: "a.b.c" → "a"."b"."c").
        """
        if isinstance(value, str):
            new_lower = new.lower()
            if "." in old or "." in new:
                quoted_old = self._to_quoted(old) if "." in old else '"' + old + '"'
                quoted_new_lower = (self._to_quoted(new) if "." in new else '"' + new + '"').lower()
                value = value.replace(quoted_old.upper(), quoted_new_lower)
                value = value.replace(quoted_old.lower(), quoted_new_lower)
                if quoted_old != quoted_old.upper() and quoted_old != quoted_old.lower():
                    value = value.replace(quoted_old, quoted_new_lower)
            value = value.replace(old.upper(), new_lower)
            value = value.replace(old.lower(), new_lower)
            if old != old.upper() and old != old.lower():
                value = value.replace(old, new_lower)
            return value
        if isinstance(value, PicklableColumn):
            return PicklableColumn(*(self._normalize_value(v, old, new) for v in value))
        if isinstance(value, tuple):
            return tuple(self._normalize_value(v, old, new) for v in value)
        if isinstance(value, list):
            return [self._normalize_value(v, old, new) for v in value]
        if isinstance(value, QueryResult):
            return QueryResult(
                rows=self._normalize_value(value.rows, old, new),
                columns=value.columns,
            )
        return value

    def _denormalize_value(self, value: Any, old: str, new: str) -> Any:
        """Recursively replace lowercase placeholders with real values.

        Since snapshots always store lowercase placeholders (via _normalize_value),
        this replaces ``old.lower()`` with ``new`` as-is, preserving the real
        value's natural case (e.g. uppercase for Snowflake, lowercase for Postgres).
        """
        if isinstance(value, str):
            old_lower = old.lower()
            if "." in old or "." in new:
                quoted_old_lower = (self._to_quoted(old) if "." in old else '"' + old + '"').lower()
                quoted_new = self._to_quoted(new) if "." in new else '"' + new + '"'
                value = value.replace(quoted_old_lower, quoted_new)
            value = value.replace(old_lower, new)
            return value
        if isinstance(value, PicklableColumn):
            return PicklableColumn(*(self._denormalize_value(v, old, new) for v in value))
        if isinstance(value, tuple):
            return tuple(self._denormalize_value(v, old, new) for v in value)
        if isinstance(value, list):
            return [self._denormalize_value(v, old, new) for v in value]
        if isinstance(value, QueryResult):
            return QueryResult(
                rows=self._denormalize_value(value.rows, old, new),
                columns=value.columns,
            )
        return value

    def _normalize_for_snapshot(self, entry: SnapshotEntry) -> SnapshotEntry:
        """Replace real schema name with uniform lowercase placeholder for portable snapshot storage."""
        has_primary = bool(self._schema_placeholder and self._real_schema_name)
        if not has_primary and not self.extra_replacements:
            return entry
        sql = entry.sql
        result = entry.result
        if has_primary:
            sql = self._normalize_value(sql, self._real_schema_name, self._schema_placeholder)
            result = (
                self._normalize_value(result, self._real_schema_name, self._schema_placeholder)
                if result is not None
                else None
            )
        for placeholder, real_value in self.extra_replacements.items():
            sql = self._normalize_value(sql, real_value, placeholder)
            result = self._normalize_value(result, real_value, placeholder) if result is not None else None
        return SnapshotEntry(entry.op_type, sql, result)

    def _denormalize_from_snapshot(self, entry: SnapshotEntry) -> SnapshotEntry:
        """Replace lowercase placeholders with real values when loading from snapshot."""
        has_primary = bool(self._schema_placeholder and self._real_schema_name)
        if not has_primary and not self.extra_replacements:
            return entry
        sql = entry.sql
        result = entry.result
        if has_primary:
            sql = self._denormalize_value(sql, self._schema_placeholder, self._real_schema_name)
            result = (
                self._denormalize_value(result, self._schema_placeholder, self._real_schema_name)
                if result is not None
                else None
            )
        for placeholder, real_value in self.extra_replacements.items():
            sql = self._denormalize_value(sql, placeholder, real_value)
            result = self._denormalize_value(result, placeholder, real_value) if result is not None else None
        return SnapshotEntry(entry.op_type, sql, result)

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

        # Finalize the previous test's recording.
        # Log but don't re-raise unconsumed-operation errors to prevent cascade failures:
        # if a test fails mid-replay, its unconsumed entries would otherwise propagate as
        # errors to every subsequent test (each loading replay data but failing before
        # consuming it, leaving unconsumed entries for the next test, and so on).
        # The original test already fails on its own SnapshotMismatchError.
        try:
            self._finalize_current_test()
        except SnapshotMismatchError as e:
            logger.warning(f"SNAPSHOT: {e}")

        # Start tracking the new test
        self._current_test_id = test_id
        if test_id is None:
            return

        if self._mode == "replay":
            raw = self._snapshot_manager.load(test_id)
            # Store raw (normalized) data; denormalization happens lazily in
            # _next_replay_entry so that extra_replacements added after load are applied.
            self._replay_data = raw if raw else None
            if self._replay_data is None:
                if self._real is not None or self._fallback_connection_factory is not None:
                    if self._real is None:
                        self._real = self._fallback_connection_factory()
                    snapshot_path = self._snapshot_manager._snapshot_path(test_id, "pickle")
                    logger.info(
                        f"SNAPSHOT: Auto-recording for {test_id}\n"
                        f"  Reason: No snapshot found (expected at: {snapshot_path})\n"
                        f"  Running all operations against real DB. A new snapshot will be recorded."
                    )
                    self._fallback_active = True
                    self._recording = []
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

    def _record_entry(self, entry: SnapshotEntry) -> None:
        """Append an entry to the recording, normalizing with current replacements.

        Normalizing at capture time (rather than finalization) ensures that
        extra_replacements values active when the SQL was executed are used.
        This matters when values like scan_id change mid-test.
        """
        self._recording.append(self._normalize_for_snapshot(entry))

    def _finalize_current_test(self) -> None:
        """Save the current test's recording (if any) and reset per-test state."""
        if self._current_test_id is not None and self._recording:
            # Entries are already normalized at capture time via _record_entry.
            self._snapshot_manager.save(self._current_test_id, self._recording)

        # Check for unconsumed snapshot entries before resetting state.
        # We capture the error first and always reset state to prevent cascade failures
        # where one test's unconsumed operations block all subsequent tests.
        unconsumed_error = None
        if (
            self._mode == "replay"
            and not self._fallback_active
            and self._replay_data is not None
            and self._replay_index < len(self._replay_data)
        ):
            remaining = len(self._replay_data) - self._replay_index
            next_entry = self._replay_data[self._replay_index]
            unconsumed_error = SnapshotMismatchError(
                f"Snapshot has {remaining} unconsumed operation(s) for test {self._current_test_id}.\n"
                f"  Next unconsumed ({next_entry.op_type}): {next_entry.sql[:200]}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ..."
            )

        # Always reset per-test state
        self._recording = []
        self._replay_data = None
        self._replay_index = 0
        self._fallback_active = False

        if unconsumed_error:
            raise unconsumed_error

    def finalize(self) -> None:
        """Called at end of session to save the last test's recording."""
        self._finalize_current_test()
        self._current_test_id = None

    # -------------------------------------------------------------------------
    # Replay helpers
    # -------------------------------------------------------------------------

    def _ensure_real_connection(self) -> None:
        """Re-establish the real connection if it was closed (e.g. by close_connection)."""
        if self._real is None:
            if self._fallback_connection_factory is not None:
                self._real = self._fallback_connection_factory()
            else:
                raise RuntimeError(
                    "Cannot use real DB — no real connection available.\n"
                    "  The connection was closed and no factory exists to re-create it."
                )

    def _activate_fallback(self, reason: str = "") -> None:
        """Fall back to real DB for the current test.

        Re-executes all previously replayed operations against the real DB to
        set up database state (tables, inserts), then switches to passthrough
        mode for all remaining operations in this test.
        """
        if not self._allow_fallback:
            raise SnapshotMismatchError(
                f"Snapshot mismatch for test {self._current_test_id} and fallback is disabled.\n"
                f"  Reason: {reason}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ...\n"
                f"  To enable fallback, set: SODA_TEST_SNAPSHOT_FALLBACK=true"
            )
        if self._real is None:
            if self._fallback_connection_factory is not None:
                self._real = self._fallback_connection_factory()
            else:
                raise RuntimeError(
                    "Cannot fall back to real DB — no real connection available.\n"
                    "  To enable fallback, ensure the real connection is provided in replay mode."
                )

        # Cascade fallback to linked snapshot first (insource mode).
        # The linked source snapshot must re-execute its operations to create
        # real tables before the DWH snapshot can re-execute CTAS statements.
        if self._linked_snapshot is not None:
            linked = self._linked_snapshot
            if not linked._fallback_active and linked._mode == "replay":
                linked._activate_fallback(reason=f"Cascaded from linked snapshot fallback ({reason})")

        ops_to_replay = self._replay_index
        logger.warning(
            f"SNAPSHOT: Falling back to real DB for {self._current_test_id}\n"
            f"  Reason: {reason}\n"
            f"  Re-executing {ops_to_replay} previous operations, then continuing against real DB.\n"
            f"  The snapshot will be re-recorded with the new SQL."
        )

        # Re-execute all previously replayed operations to set up DB state
        # and record fresh results so the snapshot can be updated.
        self._recording = []
        for i in range(ops_to_replay):
            entry = self._denormalize_from_snapshot(self._replay_data[i])
            if entry.op_type == "update":
                self._real.execute_update(entry.sql, log_query=False)
                self._record_entry(SnapshotEntry("update", entry.sql, None))
            else:
                result = self._real.execute_query(entry.sql, log_query=False)
                self._record_entry(SnapshotEntry(entry.op_type, entry.sql, self._normalize_query_result(result)))

        self._fallback_active = True

    @staticmethod
    def _normalize_dynamic_values(sql: str) -> str:
        """Replace dynamic values (timestamps, temp table UUIDs) with placeholders.

        Used for SQL comparison so that per-run values don't cause mismatches.
        """
        sql = _TIMESTAMP_RE.sub(_TIMESTAMP_PLACEHOLDER, sql)
        sql = _SODA_TEMP_RE.sub(_SODA_TEMP_PLACEHOLDER, sql)
        return sql

    def _sql_matches(self, stored_sql: str, incoming_sql: str) -> bool:
        """Compare two SQL strings, optionally normalizing dynamic values first."""
        if self.normalize_timestamps:
            return self._normalize_dynamic_values(stored_sql) == self._normalize_dynamic_values(incoming_sql)
        return stored_sql == incoming_sql

    def _next_replay_entry(self, expected_type: str, expected_sql: str) -> SnapshotEntry:
        """Get the next entry from the replay data and verify it matches.

        Comparison is done at the normalized level: both the stored snapshot SQL
        (already has placeholders) and the incoming SQL (normalized with current
        run's real values → placeholders) are compared. This avoids case mismatches
        from databases that uppercase identifiers (e.g. Snowflake uppercases hex
        scan IDs in table names, but denormalization can only produce one case).
        """
        if self._replay_data is None or self._replay_index >= len(self._replay_data):
            raise SnapshotMismatchError(
                f"Snapshot exhausted for test {self._current_test_id}.\n"
                f"  No more operations in snapshot (had {len(self._replay_data or [])}).\n"
                f"  Next operation would be: {expected_type} {expected_sql[:200]}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ..."
            )
        stored_entry = self._replay_data[self._replay_index]
        # Normalize the incoming SQL the same way snapshots are stored (real values → placeholders).
        incoming_normalized = self._normalize_for_snapshot(SnapshotEntry(expected_type, expected_sql, None))
        if stored_entry.op_type != incoming_normalized.op_type or not self._sql_matches(
            stored_entry.sql, incoming_normalized.sql
        ):
            # Show denormalized forms in error for readability
            denormalized = self._denormalize_from_snapshot(stored_entry)
            raise SnapshotMismatchError(
                f"Snapshot mismatch at operation #{self._replay_index} for test {self._current_test_id}.\n"
                f"  Expected ({denormalized.op_type}): {denormalized.sql[:200]}\n"
                f"  Got      ({expected_type}): {expected_sql[:200]}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ..."
            )
        # Return denormalized entry so callers get usable result data
        entry = self._denormalize_from_snapshot(stored_entry)
        self._replay_index += 1
        return entry

    # -------------------------------------------------------------------------
    # Pickle normalization — convert non-picklable DB driver objects
    # -------------------------------------------------------------------------

    @staticmethod
    def _safe_pickle_value(value: Any) -> Any:
        """Convert a value to a pickle-safe form.

        Some DB drivers use C-extension types (e.g. DuckDB's DuckDBPyType) that
        can't be pickled. Convert them to strings as a fallback.
        """
        import pickle

        try:
            pickle.dumps(value)
            return value
        except (TypeError, pickle.PicklingError):
            return str(value)

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
                type_code=SnapshotDataSourceConnection._safe_pickle_value(col[1]),
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

    def _get_passthrough_result(self, sql: str) -> Optional[QueryResult]:
        """Return a mock result if this SQL is a registered passthrough query, else None."""
        return self.passthrough_queries.get(sql.strip())

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        self._handle_test_boundary()

        if self._current_test_id is None:
            # Finalized or session-level SQL — pass through to real connection
            return self._passthrough_or_fail("execute_query", sql, log_query=log_query)

        # Passthrough queries return a mock result, bypassing snapshot entirely
        passthrough_result = self._get_passthrough_result(sql)
        if passthrough_result is not None:
            return passthrough_result

        if self._mode == "record":
            # Check record-mode cache before hitting the real DB
            cached = self.record_mode_cached_queries.get(sql.strip())
            if cached is not None:
                self._record_entry(SnapshotEntry("query", sql, self._normalize_query_result(cached)))
                return cached
            result = self._real.execute_query(sql, log_query=log_query)
            # Store a normalized copy (picklable), return original to caller
            self._record_entry(SnapshotEntry("query", sql, self._normalize_query_result(result)))
            return result
        else:  # replay
            if self._fallback_active:
                self._ensure_real_connection()
                result = self._real.execute_query(sql, log_query=log_query)
                self._record_entry(SnapshotEntry("query", sql, self._normalize_query_result(result)))
                return result
            try:
                entry = self._next_replay_entry("query", sql)
                if log_query:
                    sql_logger.debug(f"SNAPSHOT: replaying logs for query:")
                    sql_logger.debug(
                        f"SQL query fetchall in datasource {self.name} "
                        f"(first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}"
                    )
                return entry.result
            except SnapshotMismatchError as e:
                self._activate_fallback(reason=str(e))
                result = self._real.execute_query(sql, log_query=log_query)
                self._record_entry(SnapshotEntry("query", sql, self._normalize_query_result(result)))
                return result

    def execute_update(self, sql: str, log_query: bool = True) -> UpdateResult:
        self._handle_test_boundary()

        if self._current_test_id is None:
            return self._passthrough_or_fail("execute_update", sql, log_query=log_query)

        if self._mode == "record":
            result = self._real.execute_update(sql, log_query=log_query)
            # Store None as the result — execute_update return values are driver-specific
            # (e.g. psycopg3 returns the cursor itself) and rarely used by callers.
            self._record_entry(SnapshotEntry("update", sql, None))
            return result
        else:  # replay
            if self._fallback_active:
                self._ensure_real_connection()
                result = self._real.execute_update(sql, log_query=log_query)
                self._record_entry(SnapshotEntry("update", sql, None))
                return result
            try:
                self._next_replay_entry("update", sql)
                sql_logger.debug(f"SNAPSHOT: captured update: {sql[:50]}...")
                return None
            except SnapshotMismatchError as e:
                self._activate_fallback(reason=str(e))
                result = self._real.execute_update(sql, log_query=log_query)
                self._record_entry(SnapshotEntry("update", sql, None))
                return result

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
            self._record_entry(
                SnapshotEntry("query_one_by_one", sql, (self._normalize_description(description), captured_rows))
            )
            return description
        else:  # replay
            if self._fallback_active:
                self._ensure_real_connection()
                captured_rows = []

                def capturing_callback(row, description):
                    captured_rows.append(tuple(row))
                    row_callback(row, description)

                description = self._real.execute_query_one_by_one(
                    sql, capturing_callback, log_query=log_query, row_limit=row_limit
                )
                self._record_entry(
                    SnapshotEntry("query_one_by_one", sql, (self._normalize_description(description), captured_rows))
                )
                return description
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
            except SnapshotMismatchError as e:
                self._activate_fallback(reason=str(e))
                captured_rows = []

                def capturing_callback_fb(row, description):
                    captured_rows.append(tuple(row))
                    row_callback(row, description)

                description = self._real.execute_query_one_by_one(
                    sql, capturing_callback_fb, log_query=log_query, row_limit=row_limit
                )
                self._record_entry(
                    SnapshotEntry("query_one_by_one", sql, (self._normalize_description(description), captured_rows))
                )
                return description

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
            self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
            # Yield a fake iterator backed by cached data
            try:
                yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)))
            finally:
                pass
        else:  # replay
            if self._fallback_active:
                self._ensure_real_connection()
                with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                    cursor_description = real_iter._cursor.description
                    rows = [tuple(row) for row in real_iter]
                normalized_desc = self._normalize_description(cursor_description)
                self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)))
                finally:
                    pass
                return
            try:
                entry = self._next_replay_entry("query_iterate", sql)
                rows, cursor_description = entry.result
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)))
                finally:
                    pass
            except SnapshotMismatchError as e:
                self._activate_fallback(reason=str(e))
                with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                    cursor_description = real_iter._cursor.description
                    rows = [tuple(row) for row in real_iter]
                normalized_desc = self._normalize_description(cursor_description)
                self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)))
                finally:
                    pass

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
