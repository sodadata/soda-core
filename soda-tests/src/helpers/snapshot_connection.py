from __future__ import annotations

import contextlib
import logging
import os
import re
import weakref
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
from soda_core.common.data_source_results import QueryResult, QueryResultIterator
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
_SODA_TEMP_RE = re.compile(r"__soda_temp_[0-9a-f]{32}", re.IGNORECASE)
_SODA_TEMP_PLACEHOLDER = "__soda_temp___$$__SODA_UUID__$$__"

# Process-local registry of (test_id → first fallback reason) recorded as soon as
# _activate_fallback fires. Consumed by snapshot_pytest_plugin to surface fallback
# events in pytest output (custom progress char + end-of-session summary). Module
# scope (rather than class state) so the plugin can read it without a connection
# instance. With pytest-xdist each worker has its own copy — the plugin aggregates
# on the master via the test-report path so summaries still come out correct.
_FALLBACK_TEST_RECORD: dict[str, str] = {}

# Process-local registry of (test_id → reason) populated when a snapshot replay
# raises SnapshotReplayError under the rerun model. The pytest plugin
# (`pytest_runtest_protocol`) reads this map after the first test attempt to
# decide whether a re-run against the real DB is needed.
_PENDING_RERUN: dict[str, str] = {}

# Weak registry of every live SnapshotDataSourceConnection in this process.
# The rerun plugin walks this set to mark all wrappers for passthrough on
# re-run (primary, DWH, recon secondaries) without needing to know about them
# individually. Weakref so wrappers GC normally when their helper goes away.
_LIVE_SNAPSHOT_WRAPPERS: "weakref.WeakSet[SnapshotDataSourceConnection]" = weakref.WeakSet()


def get_fallback_test_record() -> dict[str, str]:
    """Return the (test_id → reason) map of tests that triggered fallback in this process."""
    return _FALLBACK_TEST_RECORD


def reset_fallback_test_record() -> None:
    """Clear the fallback registry. Intended for unit tests that need a clean slate."""
    _FALLBACK_TEST_RECORD.clear()


def get_pending_rerun_record() -> dict[str, str]:
    """Return (test_id → reason) for tests that asked the plugin to re-run."""
    return _PENDING_RERUN


def reset_pending_rerun_record() -> None:
    """Clear the pending-rerun registry. Used by the plugin between phases."""
    _PENDING_RERUN.clear()


def get_live_snapshot_wrappers() -> "weakref.WeakSet[SnapshotDataSourceConnection]":
    """Return the weak set of all live SnapshotDataSourceConnection instances."""
    return _LIVE_SNAPSHOT_WRAPPERS


def is_rerun_mode_enabled() -> bool:
    """Whether the rerun-on-mismatch model is active for this process.

    Off by default during migration. When false, the legacy partial-replay
    fallback path runs unchanged. When true, replay errors bubble out so the
    pytest plugin can re-run the test against the real DB.
    """
    return os.getenv("SODA_TEST_SNAPSHOT_RERUN", "").lower() == "true"


# When the rerun plugin is between the failed first attempt and the re-run
# of ``test_id``, this dict holds (test_id → record_flag). Any wrapper
# constructed during the re-run (e.g. a DWH or recon-secondary that didn't
# exist on the first attempt) checks this at __init__ time and pre-arms its
# own passthrough flags so its first SQL op routes to the real DB.
#
# Stored as a dict rather than a single (test_id, record_flag) tuple so the
# plugin can handle the edge where a nested DwhTestSetup is created in test
# teardown that should not re-arm: we look up by test_id exactly. With
# pytest-xdist each worker process has its own copy.
_RERUN_IN_PROGRESS: dict[str, bool] = {}


def begin_rerun_in_progress(test_id: str, *, record: bool) -> None:
    """Plugin hook: mark a rerun-in-progress for ``test_id``.

    Any wrapper constructed after this call (e.g. DWH interceptor's snapshot
    connection created inside the rerun's test body) auto-enters passthrough
    for the same test_id, so the rerun doesn't see "fresh" snapshots loaded.
    """
    _RERUN_IN_PROGRESS[test_id] = record


def end_rerun_in_progress(test_id: str) -> None:
    """Plugin hook: clear the rerun-in-progress marker for ``test_id``."""
    _RERUN_IN_PROGRESS.pop(test_id, None)


def get_rerun_in_progress_record() -> dict[str, bool]:
    """Return the currently-active (test_id → record_flag) rerun map."""
    return _RERUN_IN_PROGRESS


class _SnapshotSequence:
    """Per-test-scope monotonic counter shared across related snapshot connections.

    Lets a source snapshot and its DWH dependent tag every recorded op with a
    sequence number that orders ops across BOTH streams (not just within one
    snapshot). On replay-time fallback we use this to compute precisely how
    many dependent ops happened before the upstream's mismatch point in the
    original recording, so the dependent's real DB can be materialised to the
    exact state the upstream's subsequent live ops expect to see.

    Reset semantics: ``transition_to_test`` resets the counter the FIRST
    time a new test_id is observed. Subsequent calls for the same test_id
    are no-ops, so two snapshots both calling ``transition_to_test`` at
    their own ``_handle_test_boundary`` only reset once per test.
    """

    def __init__(self) -> None:
        self.counter: int = 0
        self.current_test_id: Optional[str] = None

    def next(self) -> int:
        self.counter += 1
        return self.counter

    def observe(self, seq: int) -> None:
        """Advance the counter past ``seq`` if it's ahead. Used in replay
        mode so that any subsequent record (e.g. during fallback recovery)
        starts numbering after the highest stored seq we've seen."""
        if seq > self.counter:
            self.counter = seq

    def transition_to_test(self, test_id: Optional[str]) -> None:
        if test_id != self.current_test_id:
            self.counter = 0
            self.current_test_id = test_id


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
        # No-op: FakeCursor has no real resources to release.
        pass


class _SnapshotDbapiProxy:
    """Proxy installed as ``self.connection`` on a SnapshotDataSourceConnection.

    Production code that bypasses the high-level execute_query family (notably the
    rows_diff reconciliation check, which does ``data_source.connection.connection.cursor()``)
    reaches the raw DBAPI through this proxy. The only DBAPI surface exposed is
    ``cursor()``. Every other attribute raises AttributeError so the existing
    production fallback paths (e.g. ``_optimized_insert`` falling back to
    ``execute_update``) keep working unchanged.
    """

    def __init__(self, owner: "SnapshotDataSourceConnection"):
        self._owner = owner

    def cursor(self) -> "_SnapshotCursor":
        return _SnapshotCursor(owner=self._owner)

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(
            f"_SnapshotDbapiProxy: '{name}' is not supported by the snapshot wrapper. "
            f"Only cursor() is exposed; use the high-level execute_query / execute_update "
            f"methods on DataSourceConnection so the operation is recorded."
        )


class _SnapshotCursor:
    """Records or replays raw DBAPI cursor operations at the ``execute()`` boundary.

    The supported surface mirrors what rows_diff needs and nothing else:
    ``execute(sql)`` (single argument, no params), ``fetchmany(size)``,
    ``description``, ``close()``. Any other attribute raises AttributeError so
    callers that need more functionality keep falling back to the high-level
    snapshot-aware methods.

    Recording is eager: at ``execute()`` time the full result set is drained via
    ``fetchall()`` on the real cursor and stored in the snapshot. Subsequent
    ``fetchmany()`` calls are served from the in-memory buffer. For test
    workloads (recon datasets are small) this is simpler and avoids interleaving
    record/replay state across cursor calls.

    When the owner has a ``_primary_snapshot`` (i.e. it is a secondary
    connection wrapping a ``create_additional_connection()`` result), test
    boundary detection, recording, and replay all delegate to the primary so
    interleaved cursor executes share a single ordered snapshot stream.
    """

    _OP_TYPE = "cursor_execute"

    def __init__(self, owner: "SnapshotDataSourceConnection"):
        self._owner = owner
        self._real_cursor: Optional[Any] = None
        self._rows: Optional[list[tuple]] = None
        self._row_index: int = 0
        self._description: Any = None
        self._closed: bool = False

    def _stream(self) -> "SnapshotDataSourceConnection":
        """Return the snapshot connection that owns the per-test recording stream."""
        return self._owner._primary_snapshot or self._owner

    def execute(self, sql: str, params: Any = None) -> None:
        if params is not None:
            raise AttributeError(
                "_SnapshotCursor: parameterized execute is not supported. Pass a fully-formatted SQL string."
            )
        stream = self._stream()
        stream._handle_test_boundary()

        # Rerun-mode passthrough: stream + owner (which may be a secondary
        # linked to the primary) are both flagged. Route to real cursor and
        # only record if the plugin asked us to.
        if self._owner._passthrough_for_rerun or stream._passthrough_for_rerun:
            if self._owner._real is None:
                self._owner._ensure_real_connection()
            if self._real_cursor is None:
                self._real_cursor = self._owner._real.connection.cursor()
            self._real_cursor.execute(sql)
            self._description = self._real_cursor.description
            self._rows = [tuple(row) for row in self._real_cursor.fetchall()]
            self._row_index = 0
            if stream._passthrough_record_for_rerun:
                normalized_desc = SnapshotDataSourceConnection._normalize_description(self._description)
                stream._record_entry(SnapshotEntry(self._OP_TYPE, sql, (list(self._rows), normalized_desc)))
            return

        # Record mode OR replay-with-active-fallback: run against the real DB and capture.
        if self._owner._mode == "record" or stream._fallback_active:
            # When the cursor's owner is a secondary snapshot (linked via
            # primary_snapshot), its own `_real` is independent of the primary's
            # and must be ensured separately — otherwise stream._activate_fallback()
            # would only open the primary's real connection, leaving the secondary's
            # _real as None and crashing on .connection.cursor().
            if self._owner._real is None:
                self._owner._ensure_real_connection()
            if self._real_cursor is None:
                self._real_cursor = self._owner._real.connection.cursor()
            self._real_cursor.execute(sql)
            self._description = self._real_cursor.description
            self._rows = [tuple(row) for row in self._real_cursor.fetchall()]
            self._row_index = 0
            normalized_desc = SnapshotDataSourceConnection._normalize_description(self._description)
            stream._record_entry(SnapshotEntry(self._OP_TYPE, sql, (list(self._rows), normalized_desc)))
            return

        # Pure replay
        try:
            entry = stream._next_replay_entry(self._OP_TYPE, sql)
        except SnapshotMismatchError as e:
            if is_rerun_mode_enabled():
                stream._handle_replay_error_for_rerun(e)
            stream._activate_fallback(reason=str(e))
            # Same reasoning as above: ensure the owner's own _real, not just
            # the primary's via the stream's activate_fallback.
            if self._owner._real is None:
                self._owner._ensure_real_connection()
            if self._real_cursor is None:
                self._real_cursor = self._owner._real.connection.cursor()
            self._real_cursor.execute(sql)
            self._description = self._real_cursor.description
            self._rows = [tuple(row) for row in self._real_cursor.fetchall()]
            self._row_index = 0
            normalized_desc = SnapshotDataSourceConnection._normalize_description(self._description)
            stream._record_entry(SnapshotEntry(self._OP_TYPE, sql, (list(self._rows), normalized_desc)))
            return

        rows, description = entry.result
        self._rows = list(rows)
        self._description = description
        self._row_index = 0

    def fetchmany(self, size: Optional[int] = None) -> list[tuple]:
        if self._rows is None:
            return []
        if size is None:
            size = 1  # DBAPI default arraysize
        start = self._row_index
        end = min(start + size, len(self._rows))
        self._row_index = end
        return self._rows[start:end]

    @property
    def description(self) -> Any:
        return self._description

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._real_cursor is not None:
            try:
                self._real_cursor.close()
            except Exception:
                logger.warning("Failed to close real cursor in _SnapshotCursor", exc_info=True)
            self._real_cursor = None

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(
            f"_SnapshotCursor: '{name}' is not supported by the snapshot wrapper. "
            f"Only execute(sql), fetchmany(size), description, close() are exposed."
        )


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
        primary_snapshot: Optional["SnapshotDataSourceConnection"] = None,
    ):
        # Pass _SENTINEL during super().__init__ so the base open_connection()
        # is a no-op (skips when self.connection is not None). Immediately after,
        # we replace self.connection with a _SnapshotDbapiProxy so that callers
        # which reach for the raw DBAPI (e.g. rows_diff: connection.connection.cursor())
        # get a recording/replaying cursor instead of an AttributeError.
        # The snapshot's own execute_query / execute_update still use self._real,
        # not self.connection.
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
        self._fallback_is_auto_record: bool = False  # True when fallback is for a missing snapshot (new recording)
        self._allow_fallback: bool = allow_fallback
        # _linked_snapshot models an *upstream* dependency for cascade-on-fallback:
        # when this snapshot falls back, its linked snapshot must fall back FIRST
        # (because this snapshot's re-execution depends on the linked one's state
        # already being on the real DB). Used by DWH→source insource mode.
        self._linked_snapshot: Optional[SnapshotDataSourceConnection] = None
        # _dependent_snapshots is the *inverse* relationship for cascade-on-fallback:
        # when this snapshot falls back, every dependent must ALSO fall back
        # (because their writes would otherwise stay in replay land and never
        # reach the real DB that subsequent code reads back from). Used to
        # propagate a source snapshot's fallback to its DWH snapshot.
        self._dependent_snapshots: list[SnapshotDataSourceConnection] = []
        # _upstream_snapshot is the pull-side check for the same dependency.
        # If this snapshot's upstream is already in fallback when this snapshot
        # starts processing a new test (via _handle_test_boundary), this
        # snapshot must also enter fallback. Covers the case where the source
        # fell back before the dependent DWH was opened, so the push-cascade
        # couldn't reach it then.
        self._upstream_snapshot: Optional[SnapshotDataSourceConnection] = None
        self._finalized: bool = False

        # Data-source-specific cleanup hook used by _activate_fallback when a
        # CREATE TABLE re-execution fails because connection_factory already
        # created the table. The hook receives (real_connection, table_name)
        # and is responsible for resetting the table to an empty state so the
        # subsequent INSERT lands clean. Default behaviour (None) emits a plain
        # DROP TABLE — that works for Postgres, BigQuery, Snowflake, SqlServer,
        # Databricks, AND Athena (whose AthenaDataSource.execute_update
        # intercepts DROP TABLE to also clean up the underlying S3 files).
        # DataSourceTestHelper.reset_table_for_fallback_retry is wired here by
        # _start_test_session_replay; subclasses can override that method for
        # any data source that needs extra steps.
        self._fallback_reset_table_callback: Optional[Callable[[DataSourceConnection, str], None]] = None

        # When set, this is a secondary snapshot wrapper that shares the primary's
        # per-test recording stream. Used to wrap connections returned by
        # DataSourceImpl.create_additional_connection() so that interleaved cursor
        # executes (e.g. rows_diff's merge-join) appear in one ordered snapshot.
        self._primary_snapshot: Optional["SnapshotDataSourceConnection"] = primary_snapshot

        # Cross-connection monotonic sequence counter shared with related
        # snapshots (source + DWH dependent + secondaries) so every recorded
        # op gets a position in a single test-scoped order. Used to make
        # cascade-on-fallback precise: when upstream mismatches at seq=S,
        # the cascade replays only the dependent ops with seq<S on its real
        # DB. ``None`` here means "no precise cascade" — backward compatible
        # for old snapshots whose entries lack ``seq``.
        self._sequence: Optional[_SnapshotSequence] = None

        # When fallback fires due to a snapshot mismatch, ``_mismatch_seq``
        # holds the recorded ``seq`` of the entry the test failed to match.
        # Cascade-to-dependents reads it to compute precise cut-offs.
        self._mismatch_seq: Optional[int] = None

        # Per-test override flag set by the pytest rerun plugin between
        # attempts. When True for the current test_id, this wrapper bypasses
        # replay/record state entirely and routes every op straight to the
        # real connection — recording fresh results too if
        # ``_passthrough_record_for_rerun`` is also True. Cleared at the next
        # test boundary.
        self._passthrough_for_rerun: bool = False
        self._passthrough_record_for_rerun: bool = False
        self._partial_consumption_warned: bool = False

        # If a rerun is in progress for the current pytest test, pre-arm
        # passthrough on this wrapper so its first SQL op goes straight to
        # the real DB. Covers wrappers constructed inside the rerun's test
        # body (DwhSnapshotInterceptor's DWH wrapper, recon secondaries
        # created via patched create_additional_connection, etc.).
        if _RERUN_IN_PROGRESS:
            current = os.environ.get("PYTEST_CURRENT_TEST", "")
            current_test_id = current.rsplit(" ", 1)[0] if " " in current else current
            if current_test_id in _RERUN_IN_PROGRESS:
                self._passthrough_for_rerun = True
                self._passthrough_record_for_rerun = _RERUN_IN_PROGRESS[current_test_id]

        # Install the DBAPI proxy in place of the sentinel passed to super().__init__.
        # Production code that does data_source.connection.connection.cursor() will
        # now receive a recording/replaying _SnapshotCursor.
        self.connection = _SnapshotDbapiProxy(owner=self)

        # Register in the process-wide live-wrappers set so the rerun plugin
        # can find every wrapper (primary, DWH, recon secondaries) and mark
        # them in lockstep when a test asks to re-run.
        _LIVE_SNAPSHOT_WRAPPERS.add(self)

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

    def _fetch_session_timezone(self):
        """Record / replay the session timezone via the SnapshotManager sidecar.

        Vendor-specific ``_fetch_session_timezone`` implementations call
        ``self.connection.cursor()`` directly, bypassing the snapshot's
        ``execute_query`` / ``execute_update`` interception. So the regular
        record/replay layer cannot capture them — we wire session-TZ recording
        explicitly through the SnapshotManager's sidecar file.

        Behavior:
        * **Record mode** (or auto-record fallback during replay) — delegate to
          the real connection's ``_fetch_session_timezone`` and persist the
          result via ``snapshot_manager.save_session_timezone``. Subsequent
          calls within the same connection lifetime are cached by the base
          class so the real query runs once.
        * **Replay mode with a parseable sidecar** — load the recorded string
          and route through ``parse_session_timezone`` so an IANA name like
          ``"America/Los_Angeles"`` returns a real DST-aware ``ZoneInfo``.
        * **Replay mode with an unparseable or missing sidecar** — fall back
          to the real connection (auto-record on success). If the real
          connection is also unreachable, raise — DON'T silently default to
          UTC. The caller can't tell UTC-by-record from UTC-by-fabrication, so
          fabricating UTC here would corrupt the engine's wallclock
          canonicalization for any non-UTC source. The upstream
          ``get_session_timezone`` wrapper catches the raised exception,
          produces a single warning + UTC fallback, and the failure is
          visible in the operator's logs rather than embedded in wrong data.
        """
        from soda_core.common.data_source_connection import parse_session_timezone

        # Record mode: the real connection is the source of truth. Persist the
        # query result so subsequent replay runs find it.
        if self._mode == "record" or self._fallback_is_auto_record:
            return self._fetch_and_record_real_session_timezone(reason="record mode")

        # Replay mode.
        recorded = self._snapshot_manager.load_session_timezone()
        if recorded is not None:
            try:
                return parse_session_timezone(recorded)
            except ValueError as exc:
                logger.warning(
                    f"SNAPSHOT: recorded session timezone {recorded!r} is no longer "
                    f"parseable ({exc!r}); falling back to the real connection. "
                    "Re-record to refresh the sidecar."
                )
                # fall through to live-fetch path below

        # No (or unparseable) sidecar — query the real connection. The real
        # connection is the only authoritative source of session TZ; we never
        # fabricate a default here.
        return self._fetch_and_record_real_session_timezone(
            reason="replay sidecar missing or unparseable",
        )

    def _fetch_and_record_real_session_timezone(self, *, reason: str):
        """Query the real connection's session TZ and persist into the sidecar.

        Raises ``RuntimeError`` (caught by the public ``get_session_timezone``
        wrapper, which logs one warning and falls back to UTC) when no real
        connection is available — never silently defaults.
        """
        real = self._real
        if real is None and self._fallback_connection_factory is not None:
            real = self._fallback_connection_factory()
            self._real = real
        if real is None:
            raise RuntimeError(
                f"SnapshotDataSourceConnection cannot determine session timezone "
                f"({reason}): no real connection is attached and no fallback factory "
                "is configured."
            )
        tz = real._fetch_session_timezone()
        try:
            self._snapshot_manager.save_session_timezone(tz)
        except Exception as exc:
            # Sidecar persistence is an optimization; the value is already
            # determined for the current run. Don't let a write failure take
            # down the test.
            logger.warning(f"SNAPSHOT: failed to persist session timezone: {exc!r}")
        return tz

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
        return SnapshotEntry(entry.op_type, sql, result, seq=entry.seq)

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
        return SnapshotEntry(entry.op_type, sql, result, seq=entry.seq)

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
        if self._primary_snapshot is not None:
            # Secondary wrapper: delegate to primary, then mirror the test id so
            # inherited methods (execute_query, etc.) that gate on _current_test_id
            # see the same value as the primary.
            self._primary_snapshot._handle_test_boundary()
            self._current_test_id = self._primary_snapshot._current_test_id
            return
        if self._finalized:
            return
        test_id = self._get_current_test_id()
        # Always notify the shared sequence of the current test so the
        # counter resets at most once per test, regardless of which
        # snapshot in the cohort observes the boundary first. This is
        # idempotent for the same test_id, so calling it on every
        # _handle_test_boundary (including the early-return cases below)
        # is safe.
        if self._sequence is not None:
            self._sequence.transition_to_test(test_id)
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
        self._mismatch_seq = None
        if test_id is None:
            return

        # Rerun-mode passthrough: the pytest plugin has marked this wrapper
        # for a re-run of ``test_id`` against the real DB. Skip the snapshot
        # load entirely and record fresh ops into ``_recording`` if asked.
        if self._passthrough_for_rerun:
            self._replay_data = None
            self._replay_index = 0
            self._recording = []
            return

        if self._mode == "replay":
            raw = self._snapshot_manager.load(test_id)
            # Store raw (normalized) data; denormalization happens lazily in
            # _next_replay_entry so that extra_replacements added after load are applied.
            self._replay_data = raw if raw else None
            if self._replay_data is None:
                snapshot_path = self._snapshot_manager._snapshot_path(test_id, "pickle")
                if not self._allow_fallback:
                    raise SnapshotNotFoundError(
                        f"No snapshot found for test: {test_id}\n"
                        f"  Expected at: {snapshot_path}\n"
                        f"  To record snapshots, run: SODA_TEST_SNAPSHOT=record pytest ...\n"
                        f"  To enable fallback, set: SODA_TEST_SNAPSHOT_FALLBACK=true"
                    )
                # Rerun model: a missing snapshot is the same signal as a
                # mismatch — surface it, let the plugin re-run the test in
                # passthrough+record mode. No partial state synthesised here.
                if is_rerun_mode_enabled():
                    reason = f"No snapshot found at {snapshot_path}"
                    _PENDING_RERUN.setdefault(test_id, reason)
                    raise SnapshotNotFoundError(
                        f"No snapshot found for test: {test_id}\n"
                        f"  Expected at: {snapshot_path}\n"
                        f"  Rerun will record a fresh snapshot."
                    )
                if self._real is not None or self._fallback_connection_factory is not None:
                    if self._real is None:
                        self._real = self._fallback_connection_factory()
                    logger.info(
                        f"SNAPSHOT: Auto-recording for {test_id}\n"
                        f"  Reason: No snapshot found (expected at: {snapshot_path})\n"
                        f"  Running all operations against real DB. A new snapshot will be recorded."
                    )
                    self._fallback_active = True
                    self._fallback_is_auto_record = True
                    self._recording = []
                else:
                    raise SnapshotNotFoundError(
                        f"No snapshot found for test: {test_id}\n"
                        f"  Expected at: {snapshot_path}\n"
                        f"  To record snapshots, run: SODA_TEST_SNAPSHOT=record pytest ..."
                    )
            self._replay_index = 0
            # If the loaded snapshot has seq numbers, prime the shared counter
            # so any new ops added by fallback recovery continue past the
            # highest stored value (avoids seq-collisions when the snapshot
            # is partially re-recorded after a mismatch).
            if self._sequence is not None and self._replay_data:
                max_seq = max((e.seq for e in self._replay_data if e.seq is not None), default=0)
                self._sequence.observe(max_seq)
            logger.info(f"SNAPSHOT: Replaying {len(self._replay_data or [])} operations for {test_id}")
            # NOTE: no pull-side cascade here. If our upstream is already in
            # fallback, we deliberately stay in pure replay until our own
            # snapshot mismatches. Pre-activating fallback at the boundary
            # (where _replay_index = 0) would either leave the real DB
            # without recorded setup or — if we tried to materialize it
            # here — leak persistent state into the shared real DWH that
            # subsequent tests would observe (DwhTestSetup skips
            # delete_dwh_objects in replay mode). If our SQL has drifted in
            # the same way as upstream's, we'll hit our own mismatch and
            # run normal partial recovery with the correct _replay_index.
        elif self._mode == "record":
            self._recording = []
            logger.info(f"SNAPSHOT: Recording SQL for {test_id}")

    def _record_entry(self, entry: SnapshotEntry) -> None:
        """Append an entry to the recording, normalizing with current replacements.

        Normalizing at capture time (rather than finalization) ensures that
        extra_replacements values active when the SQL was executed are used.
        This matters when values like scan_id change mid-test.
        """
        if self._primary_snapshot is not None:
            # Secondary wrapper: delegate so all entries land in the primary's
            # ordered recording stream.
            self._primary_snapshot._record_entry(entry)
            return
        # Tag the entry with the next position in the shared cross-connection
        # order. Done here (rather than at execute_query / execute_update
        # call sites) so every code path that funnels through _record_entry
        # — record mode, fallback re-execution, auto-record — gets
        # consistent sequence numbering.
        if entry.seq is None and self._sequence is not None:
            entry.seq = self._sequence.next()
        self._recording.append(self._normalize_for_snapshot(entry))

    def _finalize_current_test(self) -> None:
        """Save the current test's recording (if any) and reset per-test state."""
        if self._current_test_id is not None and self._recording:
            # Rerun-mode passthrough+record: the plugin decides whether to
            # save (via ``finalize_test_rerun``) based on test outcome and
            # the SODA_TEST_SNAPSHOT_RERECORD env var. Auto-saving here
            # would persist recordings from failing re-runs, which we never
            # want.
            if self._passthrough_record_for_rerun:
                pass
            else:
                # In record mode, always save.  In replay+fallback for a missing
                # snapshot, save too (creating a new snapshot, not overwriting).
                # For mismatch fallback, only save when SODA_TEST_SNAPSHOT_RERECORD=true
                # — otherwise fallback re-recording silently overwrites the
                # nightly-recorded snapshot, which can corrupt it.
                should_save = (
                    self._mode == "record"
                    or self._fallback_is_auto_record
                    or os.getenv("SODA_TEST_SNAPSHOT_RERECORD", "").lower() == "true"
                )
                if should_save:
                    self._snapshot_manager.save(self._current_test_id, self._recording)
                elif self._fallback_active:
                    logger.info(
                        f"SNAPSHOT: Skipping re-record for {self._current_test_id} "
                        f"(set SODA_TEST_SNAPSHOT_RERECORD=true to overwrite)"
                    )

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
        self._fallback_is_auto_record = False

        if unconsumed_error:
            raise unconsumed_error

    def finalize(self) -> None:
        """Called at end of session to save the last test's recording."""
        self._finalize_current_test()
        self._current_test_id = None
        self._finalized = True

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

    def _activate_fallback(self, reason: str = "", ops_to_replay_override: Optional[int] = None) -> None:
        """Fall back to real DB for the current test.

        Re-executes previously replayed operations against the real DB to
        set up database state (tables, inserts), then switches to passthrough
        mode for all remaining operations in this test.

        ``ops_to_replay_override``: when set, re-execute exactly this many
        prefix entries of ``_replay_data`` instead of using the default
        ``_replay_index``. Used by the cascade path so that a dependent
        snapshot can materialise the prefix of its recording that ran
        BEFORE the upstream's mismatch point in the original recording —
        even if the dependent has consumed fewer (or more) ops in the
        current replay than the recording shows it had by that point.
        """
        if not self._allow_fallback:
            raise SnapshotMismatchError(
                f"Snapshot mismatch for test {self._current_test_id} and fallback is disabled.\n"
                f"  Reason: {reason}\n"
                f"  To re-record, run: SODA_TEST_SNAPSHOT=record pytest ...\n"
                f"  To enable fallback, set: SODA_TEST_SNAPSHOT_FALLBACK=true"
            )
        # Record this fallback against the active test for pytest reporting.
        # Only the FIRST reason wins per test (subsequent cascades from a
        # dependent snapshot are derivative). Skip when there is no current
        # test id (e.g. session-level fallback during teardown).
        if self._current_test_id and self._current_test_id not in _FALLBACK_TEST_RECORD:
            _FALLBACK_TEST_RECORD[self._current_test_id] = reason or "unspecified"
        # Call the factory on every fallback so it can both lazily open the
        # real connection (first call) and top up any test tables that this
        # test ensured but earlier fallbacks didn't yet materialize in the
        # real DB. The factory is responsible for being idempotent across
        # repeated invocations.
        if self._fallback_connection_factory is not None:
            self._real = self._fallback_connection_factory()
        elif self._real is None:
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

        if ops_to_replay_override is not None:
            ops_to_replay = max(ops_to_replay_override, self._replay_index)
        else:
            ops_to_replay = self._replay_index
        logger.warning(
            f"SNAPSHOT: Falling back to real DB for {self._current_test_id}\n"
            f"  Reason: {reason}\n"
            f"  Re-executing {ops_to_replay} previous operations, then continuing against real DB.\n"
            f"  The snapshot will be re-recorded with the new SQL."
        )

        # Re-execute all previously replayed operations to set up DB state
        # and record fresh results so the snapshot can be updated.
        # CREATE statements may fail with "already exists" when connection_factory
        # already created tables/schemas (via _ensured_test_tables) before this
        # re-execution runs. Only those errors are suppressed; other update
        # failures (INSERT, DROP, etc.) are re-raised.
        #
        # When a CREATE TABLE fails because the connection_factory already created
        # and populated the table, the subsequent INSERT in the snapshot ops would
        # double the data. To avoid that, on CREATE failure we DROP the existing
        # table and retry the CREATE — that's portable across Postgres, BigQuery,
        # Snowflake, SqlServer, Databricks, AND Athena/Hive (the latter doesn't
        # support DELETE FROM on non-Iceberg tables, which silently swallowed the
        # cleanup and caused data doubling). CREATE SCHEMA / CREATE VIEW failures
        # are still just suppressed since there's no INSERT after them.
        self._recording = []
        for i in range(ops_to_replay):
            entry = self._denormalize_from_snapshot(self._replay_data[i])
            if entry.op_type == "update":
                try:
                    self._real.execute_update(entry.sql, log_query=False)
                except Exception as exc:
                    if entry.sql.lstrip().upper().startswith("CREATE "):
                        # Postgres (and other DBs with strict transaction semantics)
                        # aborts the surrounding transaction on a failed DDL, so every
                        # subsequent execute_update on the same connection then fails
                        # with the original error message. Rolling back here clears
                        # that state. On DBs that don't need it, rollback is a no-op.
                        with contextlib.suppress(Exception):
                            self._real.rollback()
                        logger.warning(
                            f"SNAPSHOT: Ignoring error during fallback re-execution of CREATE op #{i}: {exc}"
                        )
                        # Reset table + retry CREATE so a subsequent INSERT lands in
                        # an empty table. If the failure was for CREATE SCHEMA / VIEW
                        # (no table name extractable), skip — those don't have
                        # data-doubling consequences and re-creating them isn't worth
                        # the risk.
                        table_name = self._extract_table_name_from_create(entry.sql)
                        if table_name:
                            self._reset_table_via_callback_or_default(table_name)
                            try:
                                self._real.execute_update(entry.sql, log_query=False)
                                logger.info(
                                    f"SNAPSHOT: Recreated {table_name} after resetting the pre-existing copy "
                                    "to prevent data doubling during fallback"
                                )
                            except Exception as recreate_exc:
                                # If recreate still fails, leave it: rollback once more
                                # so a following INSERT can attempt to run. The INSERT
                                # may still fail, and that will be surfaced as before.
                                with contextlib.suppress(Exception):
                                    self._real.rollback()
                                logger.warning(
                                    f"SNAPSHOT: Recreate of {table_name} also failed during fallback: {recreate_exc}"
                                )
                    else:
                        raise
                self._record_entry(SnapshotEntry("update", entry.sql, None))
            elif entry.op_type == _SnapshotCursor._OP_TYPE:
                # cursor_execute entries are read-only and tied to live cursor
                # state. Re-executing them isn't meaningful for state setup;
                # preserve the already-recorded entry so the new snapshot stays
                # consistent with what the test code already consumed.
                self._recording.append(self._replay_data[i])
            else:
                result = self._real.execute_query(entry.sql, log_query=False)
                self._record_entry(SnapshotEntry(entry.op_type, entry.sql, self._normalize_query_result(result)))

        self._fallback_active = True

        # Precise sequence-based cascade to dependent snapshots (e.g. a DWH
        # snapshot registered against this source snapshot). The mismatching
        # entry's recorded ``seq`` (captured in _next_replay_entry as
        # ``self._mismatch_seq``) defines a global cut-off across the
        # source+dependent recordings: dependent ops with seq < mismatch_seq
        # ran BEFORE the upstream's mismatch in the original recording, so
        # those — and only those — are the ones the dependent's real DB
        # needs to be primed with for the upstream's subsequent live ops to
        # see the state the recording captured. Re-executing dependent ops
        # with seq >= mismatch_seq would inject "future" state the test
        # hasn't yet caused, which is exactly what produced the snowflake/
        # bigquery "9 tables initial" regression and the doubling failures.
        #
        # Backward compat: if seqs are absent (old snapshots), fall back to
        # the coarser "only if the dependent has consumed something"
        # heuristic — same as before the seq mechanism existed.
        mismatch_seq = self._mismatch_seq
        for dependent in self._dependent_snapshots:
            if dependent._fallback_active or dependent._mode != "replay":
                continue
            if dependent._fallback_connection_factory is None and dependent._real is None:
                continue
            # Ensure the dependent's _replay_data is loaded for the current
            # test. If the dependent hasn't been touched yet,
            # _handle_test_boundary lazily loads its snapshot so we can
            # compute the seq-based cut-off. (The pull-side cascade was
            # removed earlier; this call now only loads data, it does NOT
            # auto-activate fallback.)
            try:
                dependent._handle_test_boundary()
            except SnapshotNotFoundError:
                # No snapshot for this test on the dependent; nothing to
                # materialise. Skip.
                continue
            if dependent._fallback_active:
                # Auto-record path activated fallback during boundary
                # handling (missing snapshot, factory available). Nothing
                # for the cascade to add.
                continue
            if dependent._replay_data is None:
                continue
            dep_ops_to_replay = self._count_dependent_ops_before(dependent, mismatch_seq)
            if dep_ops_to_replay == 0 and dependent._replay_index == 0:
                # Nothing to materialise and dependent hasn't been used —
                # safest to leave it in pure replay.
                continue
            dependent._activate_fallback(
                reason=f"Cascaded from upstream snapshot fallback ({reason})",
                ops_to_replay_override=dep_ops_to_replay,
            )

    @staticmethod
    def _count_dependent_ops_before(dependent: "SnapshotDataSourceConnection", mismatch_seq: Optional[int]) -> int:
        """Return the count of dependent ops whose recorded seq < mismatch_seq.

        Used by the cascade path to compute exactly how many dependent ops
        ran before the upstream's mismatch point in the original recording.
        Special cases:
        - Entries without ``seq`` (legacy snapshots) fall back to the prefix
          the test has already consumed (``_replay_index``), since precise
          ordering info is unavailable.
        - ``mismatch_seq=None`` with seq-tagged entries means "no upper
          bound" (upstream ran past end of its recording) — replay the
          dependent's entire recording.
        """
        data = dependent._replay_data or []
        has_seqs = any(e.seq is not None for e in data)
        if not has_seqs:
            # Legacy entries: no precise info → coarse heuristic.
            return dependent._replay_index
        if mismatch_seq is None:
            # Upstream had no upper bound; with seqs present we can safely
            # materialise everything the dependent recorded.
            return len(data)
        # Seqs are monotonic by construction at record time, so the first
        # entry with seq >= mismatch_seq marks the cut-off.
        for i, entry in enumerate(data):
            if entry.seq is None or entry.seq >= mismatch_seq:
                return i
        return len(data)

    def _reset_table_via_callback_or_default(self, table_name: str) -> None:
        """Reset a table so a re-CREATE during fallback re-execution lands clean.

        Prefers the data-source-specific hook installed by
        DataSourceTestHelper.reset_table_for_fallback_retry; falls back to a
        plain DROP TABLE on the real connection. Both paths are wrapped in
        suppress(Exception) because the only goal is to clear the previous
        state — the retry CREATE that follows is the operation we care about,
        and the caller will already log/warn if that fails.
        """
        callback = self._fallback_reset_table_callback
        try:
            if callback is not None:
                callback(self._real, table_name)
            else:
                self._real.execute_update(f"DROP TABLE {table_name}", log_query=False)
        except Exception:
            with contextlib.suppress(Exception):
                self._real.rollback()

    @staticmethod
    def _extract_table_name_from_create(sql: str) -> Optional[str]:
        """Extract the table name from a CREATE TABLE statement.

        Returns None for non-TABLE CREATE statements (CREATE SCHEMA, CREATE VIEW, etc.).
        Handles optional TEMP/TEMPORARY and EXTERNAL keywords (the latter is
        emitted by Athena/Hive), optional IF NOT EXISTS clause, and the various
        identifier-quoting styles used by Soda's supported datasources:
        unquoted, "double-quoted" (Postgres/Snowflake/Oracle/Athena),
        [bracket-quoted] (SqlServer/Synapse/Fabric), and `backtick-quoted`
        (BigQuery/Databricks/MySQL/Athena). Returns the fully qualified name as written.
        """
        # Use [^\s(]+ rather than a reluctant \S+? — the character class
        # explicitly stops at whitespace or "(", which avoids the lazy
        # quantifier and is equivalent for all CREATE TABLE shapes Soda emits
        # ("CREATE TABLE <name> (...)" or "CREATE TABLE <name>(...)").
        match = re.match(
            r"\s*CREATE\s+(?:TEMP(?:ORARY)?\s+|EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
            sql,
            re.IGNORECASE,
        )
        return match.group(1) if match else None

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
        if self._primary_snapshot is not None:
            # Secondary wrapper: delegate so all cursor executes are matched against
            # the primary's single ordered replay stream.
            return self._primary_snapshot._next_replay_entry(expected_type, expected_sql)
        if self._replay_data is None or self._replay_index >= len(self._replay_data):
            # No upper bound on the cascade — the upstream ran past the end
            # of its recording, so dependents should replay everything they
            # have. ``None`` mismatch_seq means "no cap" in the cascade.
            self._mismatch_seq = None
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
            # Capture the recorded seq of the mismatching entry so the
            # cascade (in _activate_fallback) can replay only the dependent
            # ops that happened BEFORE this point in the original recording.
            self._mismatch_seq = stored_entry.seq
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

    def _handle_replay_error_for_rerun(self, exc: SnapshotReplayError) -> None:
        """Stash the test id + reason and re-raise so the pytest plugin can catch.

        Only called in rerun mode. Populates ``_PENDING_RERUN`` once per test
        (first reason wins, like the existing fallback registry) and re-raises
        the exception so it bubbles out of the test body.
        """
        test_id = self._current_test_id
        if test_id is not None:
            _PENDING_RERUN.setdefault(test_id, str(exc))
        raise exc

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        self._handle_test_boundary()

        if self._current_test_id is None:
            # Finalized or session-level SQL — pass through to real connection
            return self._passthrough_or_fail("execute_query", sql, log_query=log_query)

        # Passthrough queries return a mock result, bypassing snapshot entirely
        passthrough_result = self._get_passthrough_result(sql)
        if passthrough_result is not None:
            return passthrough_result

        if self._passthrough_for_rerun:
            self._ensure_real_connection()
            result = self._real.execute_query(sql, log_query=log_query)
            if self._passthrough_record_for_rerun:
                self._record_entry(SnapshotEntry("query", sql, self._normalize_query_result(result)))
            return result

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
                    sql_logger.debug("SNAPSHOT: replaying logs for query:")
                    sql_logger.debug(
                        f"SQL query fetchall in datasource {self.name} "
                        f"(first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}"
                    )
                return entry.result
            except SnapshotMismatchError as e:
                if is_rerun_mode_enabled():
                    self._handle_replay_error_for_rerun(e)
                self._activate_fallback(reason=str(e))
                result = self._real.execute_query(sql, log_query=log_query)
                self._record_entry(SnapshotEntry("query", sql, self._normalize_query_result(result)))
                return result

    def execute_update(self, sql: str, log_query: bool = True) -> int:
        self._handle_test_boundary()

        if self._current_test_id is None:
            return self._passthrough_or_fail("execute_update", sql, log_query=log_query)

        if self._passthrough_for_rerun:
            self._ensure_real_connection()
            result = self._real.execute_update(sql, log_query=log_query)
            if self._passthrough_record_for_rerun:
                self._record_entry(SnapshotEntry("update", sql, None))
            return result

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
                return 0
            except SnapshotMismatchError as e:
                if is_rerun_mode_enabled():
                    self._handle_replay_error_for_rerun(e)
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

        if self._passthrough_for_rerun:
            self._ensure_real_connection()
            captured_rows = []

            def passthrough_callback(row, description):
                captured_rows.append(tuple(row))
                row_callback(row, description)

            description = self._real.execute_query_one_by_one(
                sql, passthrough_callback, log_query=log_query, row_limit=row_limit
            )
            if self._passthrough_record_for_rerun:
                self._record_entry(
                    SnapshotEntry("query_one_by_one", sql, (self._normalize_description(description), captured_rows))
                )
            return description

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
                if is_rerun_mode_enabled():
                    self._handle_replay_error_for_rerun(e)
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

        if self._passthrough_for_rerun:
            self._ensure_real_connection()
            with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                cursor_description = real_iter._cursor.description
                rows = [tuple(row) for row in real_iter]
            if self._passthrough_record_for_rerun:
                normalized_desc = self._normalize_description(cursor_description)
                self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
            try:
                yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
            finally:
                logger.debug("SNAPSHOT: rerun-passthrough query_iterate generator closed")
            return

        if self._mode == "record":
            # Eagerly consume all rows from the real connection so we can cache them.
            with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                cursor_description = real_iter._cursor.description
                rows = [tuple(row) for row in real_iter]
            normalized_desc = self._normalize_description(cursor_description)
            self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
            # Yield a fake iterator backed by cached data
            try:
                yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
            finally:
                logger.debug("SNAPSHOT: record query_iterate generator closed")
        else:  # replay
            if self._fallback_active:
                self._ensure_real_connection()
                with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                    cursor_description = real_iter._cursor.description
                    rows = [tuple(row) for row in real_iter]
                normalized_desc = self._normalize_description(cursor_description)
                self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
                finally:
                    logger.debug("SNAPSHOT: fallback query_iterate generator closed")
                return
            try:
                entry = self._next_replay_entry("query_iterate", sql)
                rows, cursor_description = entry.result
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
                finally:
                    logger.debug("SNAPSHOT: replay query_iterate generator closed")
            except SnapshotMismatchError as e:
                if is_rerun_mode_enabled():
                    self._handle_replay_error_for_rerun(e)
                self._activate_fallback(reason=str(e))
                with self._real.execute_query_iterate(sql, log_query=log_query) as real_iter:
                    cursor_description = real_iter._cursor.description
                    rows = [tuple(row) for row in real_iter]
                normalized_desc = self._normalize_description(cursor_description)
                self._record_entry(SnapshotEntry("query_iterate", sql, (rows, normalized_desc)))
                try:
                    yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
                finally:
                    logger.debug("SNAPSHOT: mismatch-fallback query_iterate generator closed")

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

    # -------------------------------------------------------------------------
    # Rerun-on-mismatch hooks (used by snapshot_pytest_plugin between attempts)
    # -------------------------------------------------------------------------

    def mark_test_for_rerun(self, test_id: str, *, record: bool) -> None:
        """Switch this wrapper into passthrough mode for ``test_id``.

        Called by the pytest plugin between the failed first attempt and the
        re-run. Drops any partially-consumed replay state for the named test,
        ensures the real connection is open, and sets the per-test passthrough
        flag. The next ``_handle_test_boundary`` for ``test_id`` will refuse
        to load a snapshot and route every op to ``_real`` instead.

        ``record=True`` additionally captures results so a successful re-run
        can produce a fresh snapshot (only persisted when
        ``SODA_TEST_SNAPSHOT_RERECORD=true`` — saving is decided in the
        plugin / ``_finalize_current_test``).
        """
        if self._primary_snapshot is not None:
            # Secondaries delegate: the primary already owns the recording
            # stream and the test-id state.
            self._primary_snapshot.mark_test_for_rerun(test_id, record=record)
            self._passthrough_for_rerun = True
            self._passthrough_record_for_rerun = record
            return
        # Open the real connection if it isn't already — the rerun needs to
        # talk to the live DB.
        self._ensure_real_connection()
        self._passthrough_for_rerun = True
        self._passthrough_record_for_rerun = record
        # If we already started consuming a snapshot for this test, surface
        # that so the rerun output makes the partial-consumption obvious.
        if self._replay_data is not None and self._replay_index > 0 and not self._partial_consumption_warned:
            logger.warning(
                f"SNAPSHOT: re-run for {test_id} discards {self._replay_index} "
                f"already-consumed snapshot operation(s); the test will be "
                "re-executed end-to-end against the real DB."
            )
            self._partial_consumption_warned = True
        # Drop any partial replay state so the rerun starts cleanly.
        self._replay_data = None
        self._replay_index = 0
        self._recording = []
        # Reset the test-id so _handle_test_boundary observes the rerun as a
        # fresh transition and walks through the passthrough branch.
        self._current_test_id = None

    def clear_test_rerun_marker(self) -> None:
        """Clear the per-test passthrough flags. Called once the rerun is done."""
        self._passthrough_for_rerun = False
        self._passthrough_record_for_rerun = False
        self._partial_consumption_warned = False

    def finalize_test_rerun(self, test_id: str, *, success: bool) -> None:
        """Save the rerun's recording when the rerun passed and rerecord is on.

        Called by the pytest plugin after the second attempt completes. Saves
        the fresh recording only if (a) the test passed end-to-end and (b)
        ``SODA_TEST_SNAPSHOT_RERECORD=true``. Always clears the passthrough
        flags so the next test starts clean.

        Secondaries delegate to the primary which owns the recording.
        """
        if self._primary_snapshot is not None:
            self._primary_snapshot.finalize_test_rerun(test_id, success=success)
            self.clear_test_rerun_marker()
            return
        rerecord = os.getenv("SODA_TEST_SNAPSHOT_RERECORD", "").lower() == "true"
        if (
            success
            and rerecord
            and self._passthrough_record_for_rerun
            and self._recording
            and self._current_test_id == test_id
        ):
            self._snapshot_manager.save(test_id, self._recording)
            logger.info(
                f"SNAPSHOT: Saved fresh recording from successful rerun of {test_id} "
                f"({len(self._recording)} operations)"
            )
        elif self._passthrough_record_for_rerun and not success:
            logger.info(f"SNAPSHOT: Discarding rerun recording for {test_id} (test did not pass)")
        # Always drop the per-test buffer + flags so the next test is clean.
        self._recording = []
        self.clear_test_rerun_marker()
