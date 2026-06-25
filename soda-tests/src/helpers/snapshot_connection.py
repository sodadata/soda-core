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
# Matches ISO 8601 ('2026-03-16T18:24:35.151223', '2026-03-16T17:24:35+00:00'),
# Oracle-style TIMESTAMP literals (TIMESTAMP '2026-03-18 14:09:40'), and the
# DB2 dashed-dotted format (TIMESTAMP '2026-03-18-14.09.40.123456') where the
# date-time separator is a dash and H/M/S are dot-separated.
_TIMESTAMP_RE = re.compile(r"(?:TIMESTAMP\s+)?'\d{4}-\d{2}-\d{2}[T\- ]\d{2}[:.]\d{2}[:.]\d{2}(?:[.+\-]\S+)?'")
_TIMESTAMP_PLACEHOLDER = "'__$$__SODA_TIMESTAMP__$$__'"

# Regex matching __soda_temp_<uuid-hex> table names that use uuid4().hex.
# Case-insensitive: Snowflake (and other DBs) may uppercase identifiers.
_SODA_TEMP_RE = re.compile(r"__soda_temp_[0-9a-f]{32}", re.IGNORECASE)
_SODA_TEMP_PLACEHOLDER = "__soda_temp___$$__SODA_UUID__$$__"

# Process-local registry of (test_id → reason) populated when a snapshot replay
# raises SnapshotReplayError. The pytest plugin reads this map after the first
# test attempt to decide whether a rerun against the real DB is needed.
_PENDING_RERUN: dict[str, str] = {}

# Weak registry of every live SnapshotDataSourceConnection in this process.
# The rerun plugin walks this set to swap every wrapper (primary + DWH) out
# for its real connection between the failed first attempt and the rerun.
# Weakref so wrappers GC normally when their helper goes away.
_LIVE_SNAPSHOT_WRAPPERS: "weakref.WeakSet[SnapshotDataSourceConnection]" = weakref.WeakSet()

# When the rerun plugin is between the failed first attempt and the rerun of
# ``test_id``, this set holds the active test id. Any wrapping path that
# would otherwise install a snapshot wrapper (DWH interceptor's
# patched_open_connection, patched create_additional_connection) checks this
# and bypasses, returning a plain real connection — so the rerun behaves
# identically to SODA_TEST_SNAPSHOT=off.
_RERUN_IN_PROGRESS: set[str] = set()

# Set to True by snapshot_pytest_plugin.pytest_configure so that
# _finalize_current_test can distinguish "plugin is wired up, downgrade
# unconsumed-error to a queued rerun" from "no plugin, raise as before".
_PYTEST_PLUGIN_ACTIVE: bool = False

# Test boundary discards: when _finalize_current_test detects unconsumed
# snapshot entries for a test that's *already finished* (the next test's
# first execute_query triggered the boundary handler), the rerun queue
# entry would be keyed to the wrong test id — the plugin can no longer
# act on it. Rather than crash the next test, we downgrade those
# unconsumed-entry errors to a warning AND record them here. The plugin's
# pytest_terminal_summary surfaces a dedicated section so the SQL drift
# doesn't silently disappear across many runs.
_DISCARDED_UNCONSUMED_RECORD: dict[str, str] = {}


def set_pytest_plugin_active(active: bool) -> None:
    """Toggle the plugin-active flag. Called from snapshot_pytest_plugin."""
    global _PYTEST_PLUGIN_ACTIVE
    _PYTEST_PLUGIN_ACTIVE = active


def is_pytest_plugin_active() -> bool:
    """Whether the snapshot rerun plugin is wired up in this process."""
    return _PYTEST_PLUGIN_ACTIVE


def get_pending_rerun_record() -> dict[str, str]:
    """Return (test_id → reason) for tests that asked the plugin to rerun."""
    return _PENDING_RERUN


def reset_pending_rerun_record() -> None:
    """Clear the pending-rerun registry. Used by the plugin between phases."""
    _PENDING_RERUN.clear()


def get_live_snapshot_wrappers() -> "weakref.WeakSet[SnapshotDataSourceConnection]":
    """Return the weak set of all live SnapshotDataSourceConnection instances."""
    return _LIVE_SNAPSHOT_WRAPPERS


def get_discarded_unconsumed_record() -> dict[str, str]:
    """Return (test_id → reason) for tests whose teardown silently swallowed
    unconsumed-snapshot errors because the test boundary had already passed.
    The plugin's terminal summary reads this so the drift is visible.
    """
    return _DISCARDED_UNCONSUMED_RECORD


def reset_discarded_unconsumed_record() -> None:
    """Clear the discarded-unconsumed registry. Used by the plugin at
    session start so each session begins with a clean slate."""
    _DISCARDED_UNCONSUMED_RECORD.clear()


def is_strict_mode_enabled() -> bool:
    """Whether strict mode is active: snapshot mismatches fail the test instead of rerunning.

    Default OFF. Set ``SODA_TEST_SNAPSHOT_STRICT=true`` to verify that just-
    recorded snapshots replay correctly without falling through to the real
    DB — useful right after a nightly record run to flag any
    non-deterministic SQL that would otherwise be silently papered over by
    a successful rerun.
    """
    return os.getenv("SODA_TEST_SNAPSHOT_STRICT", "false").lower() == "true"


def begin_rerun_in_progress(test_id: str) -> None:
    """Plugin hook: mark a rerun-in-progress for ``test_id``.

    Wrapping paths (DwhSnapshotInterceptor.patched_open_connection, patched
    create_additional_connection) check this and bypass — returning a plain
    real connection so the rerun matches off-mode exactly.
    """
    _RERUN_IN_PROGRESS.add(test_id)


def end_rerun_in_progress(test_id: str) -> None:
    """Plugin hook: clear the rerun-in-progress marker for ``test_id``."""
    _RERUN_IN_PROGRESS.discard(test_id)


def is_any_rerun_in_progress() -> bool:
    """Whether the rerun plugin is between the failed first attempt and the
    rerun of any test in this process.

    Wrapping paths (DwhSnapshotInterceptor.patched_open_connection, the
    patched create_additional_connection) check this and bypass — returning
    a plain real connection so the rerun matches off-mode exactly.
    """
    return bool(_RERUN_IN_PROGRESS)


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

        # Record mode: run against the real DB and capture.
        if self._owner._mode == "record":
            if self._owner._real is None:
                self._owner._ensure_real_connection()
            if self._real_cursor is None:
                self._real_cursor = self._owner._real.connection.cursor()
            # If the drain (execute/fetchall/description) raises, close the
            # real cursor and propagate WITHOUT recording — a half-recorded
            # entry would desync the snapshot stream for the rest of the test.
            try:
                self._real_cursor.execute(sql)
                raw_description = self._real_cursor.description
                rows = [tuple(row) for row in self._real_cursor.fetchall()]
            except Exception:
                with contextlib.suppress(Exception):
                    self._real_cursor.close()
                self._real_cursor = None
                raise
            normalized_desc = SnapshotDataSourceConnection._normalize_description(raw_description)
            # Store the normalized description so .description has the same
            # shape (PicklableColumn) in record and replay mode.
            self._description = normalized_desc
            self._rows = rows
            self._row_index = 0
            stream._record_entry(SnapshotEntry(self._OP_TYPE, sql, (list(rows), normalized_desc)))
            return

        # Pure replay
        try:
            entry = stream._next_replay_entry(self._OP_TYPE, sql)
        except SnapshotMismatchError as e:
            stream._signal_rerun_or_fail(e)
            # _signal_rerun_or_fail always raises; this line is for the type checker.
            return  # pragma: no cover

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

    On a mismatch (or missing snapshot), the wrapper raises a
    ``SnapshotReplayError`` and registers the test in ``_PENDING_RERUN`` so
    the pytest plugin can rerun the test end-to-end against the real DB.
    Between attempts the plugin swaps this wrapper out of the
    ``DataSourceImpl.data_source_connection`` slot — the rerun behaves
    identically to ``SODA_TEST_SNAPSHOT=off``.

    Test boundaries are detected automatically via the PYTEST_CURRENT_TEST environment
    variable, so no changes to test code or fixtures are required.

    Invariant for the rerun swap window: between ``swap_to_real_for_rerun`` and
    ``restore_from_rerun`` the wrapper is detached from its ``DataSourceImpl``
    and ``_current_test_id`` is ``None``. Anything reading the wrapper during
    that window observes "no test in progress on this wrapper" rather than a
    stale id.
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
        # When True, a snapshot mismatch (or missing snapshot) registers a
        # rerun signal in _PENDING_RERUN before re-raising; when False, the
        # exception propagates as a plain test failure. Gated by
        # SODA_TEST_SNAPSHOT_FALLBACK=true at the helper level.
        self._allow_fallback: bool = allow_fallback
        self._finalized: bool = False

        # When set, this is a secondary snapshot wrapper that shares the primary's
        # per-test recording stream. Used to wrap connections returned by
        # DataSourceImpl.create_additional_connection() so that interleaved cursor
        # executes (e.g. rows_diff's merge-join) appear in one ordered snapshot.
        self._primary_snapshot: Optional["SnapshotDataSourceConnection"] = primary_snapshot

        # Back-reference to the DataSourceImpl that owns this wrapper. Used
        # by the pytest rerun plugin to swap the wrapper out for a real
        # DataSourceConnection during the rerun, so the rerun's test body
        # runs identically to SODA_TEST_SNAPSHOT=off mode. Set by the code
        # that installs the wrapper (start_test_session_replay/record,
        # DwhSnapshotInterceptor.patched_open_connection). ``None`` means
        # we don't know which DataSourceImpl this wrapper belongs to — the
        # plugin can't swap.
        self._data_source_impl: Optional[Any] = None

        # Install the DBAPI proxy in place of the sentinel passed to super().__init__.
        # Production code that does data_source.connection.connection.cursor() will
        # now receive a recording/replaying _SnapshotCursor.
        self.connection = _SnapshotDbapiProxy(owner=self)

        # Register in the process-wide live-wrappers set so the rerun plugin
        # can find every wrapper (primary, DWH) and swap them in lockstep
        # when a test asks to rerun.
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
        * **Record mode** — delegate to the real connection's
          ``_fetch_session_timezone`` and persist the result via
          ``snapshot_manager.save_session_timezone``. Subsequent calls within
          the same connection lifetime are cached by the base class so the
          real query runs once.
        * **Replay mode with a parseable sidecar** — load the recorded string
          and route through ``parse_session_timezone`` so an IANA name like
          ``"America/Los_Angeles"`` returns a real DST-aware ``ZoneInfo``.
        * **Replay mode with an unparseable or missing sidecar** — fall back
          to the real connection. If the real connection is also unreachable,
          raise — DON'T silently default to UTC. The upstream
          ``get_session_timezone`` wrapper catches the raised exception,
          produces a single warning + UTC fallback, and the failure is
          visible in the operator's logs rather than embedded in wrong data.
        """
        from soda_core.common.data_source_connection import parse_session_timezone

        # Record mode: the real connection is the source of truth. Persist the
        # query result so subsequent replay runs find it.
        if self._mode == "record":
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
                snapshot_path = self._snapshot_manager._snapshot_path(test_id, "pickle")
                # Missing snapshot — always register a rerun signal, regardless
                # of ``_allow_fallback``. There's literally no recording to
                # replay, so the only way to know if the test passes is to
                # run it against the real DB. The pytest plugin then decides:
                # rerun (default) or hard fail (strict mode).
                exc = SnapshotNotFoundError(
                    f"No snapshot found for test: {test_id}\n"
                    f"  Expected at: {snapshot_path}\n"
                    f"  To record snapshots, run: SODA_TEST_SNAPSHOT=record pytest ..."
                )
                _PENDING_RERUN.setdefault(test_id, str(exc))
                raise exc
            self._replay_index = 0
            rerun_marker = " [INSIDE-RERUN]" if _RERUN_IN_PROGRESS else ""
            logger.info(
                f"SNAPSHOT: Replaying {len(self._replay_data or [])} operations for {test_id}"
                f" (wrapper id={id(self)}){rerun_marker}"
            )
        elif self._mode == "record":
            self._recording = []
            logger.info(f"SNAPSHOT: Recording SQL for {test_id} (wrapper id={id(self)})")

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
        self._recording.append(self._normalize_for_snapshot(entry))

    def _finalize_current_test(self) -> None:
        """Save the current test's recording (if any) and reset per-test state."""
        if self._current_test_id is not None and self._recording and self._mode == "record":
            self._snapshot_manager.save(self._current_test_id, self._recording)

        # Check for unconsumed snapshot entries before resetting state.
        # We capture the error first and always reset state to prevent cascade failures
        # where one test's unconsumed operations block all subsequent tests.
        unconsumed_error = None
        if self._mode == "replay" and self._replay_data is not None and self._replay_index < len(self._replay_data):
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

        if unconsumed_error:
            # Unconsumed entries at teardown mean an earlier mid-test mismatch was
            # swallowed by user code (e.g. the contract verification handler).
            # Queue the rerun signal and log a warning rather than raising —
            # raising here would block the plugin from running the rerun.
            # Only downgrade when the rerun plugin is actually wired up.
            # Without the plugin, _PENDING_RERUN is read by nobody and the
            # original test failure would silently disappear.
            if self._allow_fallback and self._current_test_id is not None and is_pytest_plugin_active():
                _PENDING_RERUN.setdefault(self._current_test_id, str(unconsumed_error))
                # Also record the drift in the discarded registry. The
                # rerun queue may not actually be acted on — when this branch
                # fires from _handle_test_boundary, the *previous* test's
                # reports are already emitted and the plugin's per-test
                # _pop_rerun_reason lookup can't match the previous test_id.
                # Recording here guarantees the drift is surfaced in the
                # terminal summary even if no rerun ever runs against it.
                _DISCARDED_UNCONSUMED_RECORD.setdefault(self._current_test_id, str(unconsumed_error))
                logger.warning(f"SNAPSHOT: {unconsumed_error}")
                return
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

    @staticmethod
    def _normalize_dynamic_values(sql: str) -> str:
        """Replace dynamic values (timestamps, temp table UUIDs) with placeholders.

        Used for SQL comparison so that per-run values don't cause mismatches.
        """
        sql = _TIMESTAMP_RE.sub(_TIMESTAMP_PLACEHOLDER, sql)
        sql = _SODA_TEMP_RE.sub(_SODA_TEMP_PLACEHOLDER, sql)
        return sql

    @staticmethod
    def _canonicalize_yaml_literals(sql: str) -> str:
        """Replace YAML-formatted single-quoted string literals with canonical JSON.

        Some columns persist structured data as a YAML dump (notably
        ``check_results.check_definition``). YAML's plain-scalar line-wrap column
        depends on content length, so the same logical value can serialize to
        different textual forms across runs when the surrounding content changes
        length (e.g. a different test-session schema name length shifts the wrap
        point). Such whitespace-only differences are semantically irrelevant —
        ``yaml.safe_load`` collapses ``\\n + indent`` to a single space and returns
        identical Python objects either way.

        For every single-quoted SQL literal that parses as **multi-line**
        structured YAML (dict or list, with at least one newline in the inner
        content), substitute its sorted-key JSON canonical form, so that
        downstream string comparison is insensitive to YAML pretty-print drift.

        Two guards constrain when the rewrite fires:

        - **Multi-line only** (``"\\n" in inner``): a YAML wrap-drift difference
          can only arise from a YAML dump that's actually wrapped, which means
          multi-line. Single-line flow-style literals like ``'[1,2,3]'`` or
          ``'{a: 1}'`` are left untouched so legitimate whitespace/ordering
          differences in flow-style payloads still surface as mismatches.

        - **JSON-serialisable parsed value**: the YAML loader happily parses
          bare ISO timestamps into ``datetime`` objects and other tagged YAML
          types that ``json.dumps`` can't natively encode. Fall back to a
          string coercion via ``default=str`` so canonicalisation never crashes
          on these — and tolerate any residual unexpected type by catching
          ``TypeError``/``ValueError`` and leaving the literal as-is.

        Uses ``ruamel.yaml`` (a declared soda-core dependency) rather than
        PyYAML, which isn't pulled in transitively by ``soda-tests`` and would
        be ``ModuleNotFoundError`` in a clean install.
        """
        import json

        from ruamel.yaml import YAML as _YAML
        from ruamel.yaml import YAMLError as _YAMLError

        _yaml_loader = _YAML(typ="safe")

        def _try_yaml_structured(content: str):
            """Return parsed dict/list if ``content`` is *multi-line* structured YAML.

            Single-line flow-style scalars (``'[1,2,3]'``, ``'{a:1}'``) and bare
            scalars are explicitly NOT eligible — the wrap-drift bug only
            manifests on YAML dumps that span multiple lines.
            """
            if "\n" not in content:
                return None
            try:
                parsed = _yaml_loader.load(content)
            except _YAMLError:
                return None
            if isinstance(parsed, (dict, list)):
                return parsed
            return None

        def _repl(match: "re.Match[str]") -> str:
            literal = match.group(0)
            # SQL single-quote escaping: '' inside a literal represents a single '
            inner = literal[1:-1].replace("''", "'")
            parsed = _try_yaml_structured(inner)
            # One-level extra unwrap: some dialects (notably BigQuery's
            # do_bulk_insert) wrap string values in an extra layer of single-quote
            # escaping, so the SQL literal carries content like ``'YAML-here'`` —
            # i.e. the structured YAML wrapped in literal single quotes. YAML
            # parses such content as a single-quoted SCALAR (folding newlines
            # into spaces), which loses the structure. Detect this pattern by
            # checking for inner content that starts AND ends with a single
            # quote, strip that layer, and retry the structured parse.
            if parsed is None and len(inner) >= 2 and inner.startswith("'") and inner.endswith("'"):
                parsed = _try_yaml_structured(inner[1:-1])
            if parsed is None:
                return literal
            try:
                # ``default=str`` coerces non-JSON-serialisable values (e.g.
                # ``datetime`` parsed from unquoted ISO timestamps) to their
                # str() form so canonicalisation produces a stable text.
                canonical = json.dumps(parsed, sort_keys=True, default=str)
            except (TypeError, ValueError):
                return literal
            return "'" + canonical.replace("'", "''") + "'"

        return re.sub(r"'(?:[^']|'')*'", _repl, sql)

    def _sql_matches(self, stored_sql: str, incoming_sql: str) -> bool:
        """Compare two SQL strings, optionally normalizing dynamic values first.

        Strict equality is the fast path. On miss, a YAML-aware fallback
        canonicalizes any single-quoted literals that embed structured YAML so
        that cosmetic line-wrap differences (e.g. in the ``check_definition``
        column) don't trigger spurious mismatches.
        """
        if self.normalize_timestamps:
            stored = self._normalize_dynamic_values(stored_sql)
            incoming = self._normalize_dynamic_values(incoming_sql)
        else:
            stored, incoming = stored_sql, incoming_sql
        if stored == incoming:
            return True
        return self._canonicalize_yaml_literals(stored) == self._canonicalize_yaml_literals(incoming)

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
            # Diagnostic dump: write the FULL Expected/Got pair to disk so it can
            # be captured as a CI artifact (the in-error truncation makes the
            # actual diff invisible for long INSERT statements).
            import os as _os

            diag_dir = _os.environ.get("SODA_TEST_SNAPSHOT_DIAG_DIR")
            if diag_dir:
                try:
                    _os.makedirs(diag_dir, exist_ok=True)
                    safe_test = (self._current_test_id or "unknown").replace("/", "_").replace(":", "_")[:200]
                    diag_path = _os.path.join(diag_dir, f"{safe_test}_op{self._replay_index}.txt")
                    with open(diag_path, "w", encoding="utf-8") as _f:
                        _f.write(f"Test: {self._current_test_id}\n")
                        _f.write(f"Operation index: {self._replay_index}\n")
                        _f.write(f"Stored op_type: {stored_entry.op_type}\n")
                        _f.write(f"Got op_type:    {expected_type}\n\n")
                        _f.write("=== Expected SQL (denormalized) ===\n")
                        _f.write(denormalized.sql)
                        _f.write("\n\n=== Got SQL (current run) ===\n")
                        _f.write(expected_sql)
                        _f.write("\n\n=== Stored SQL (normalized — as in snapshot file) ===\n")
                        _f.write(stored_entry.sql)
                        _f.write("\n\n=== Incoming SQL (normalized — what comparison sees) ===\n")
                        _f.write(incoming_normalized.sql)
                except Exception:
                    pass  # best-effort diagnostic
            raise SnapshotMismatchError(
                f"Snapshot mismatch at operation #{self._replay_index} for test {self._current_test_id}.\n"
                f"  Expected ({denormalized.op_type}): {denormalized.sql[:4000]}\n"
                f"  Got      ({expected_type}): {expected_sql[:4000]}\n"
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

    def _signal_rerun_or_fail(self, exc: Exception) -> None:
        """Register a rerun signal (if allow_fallback) and re-raise.

        The pytest plugin reads ``_PENDING_RERUN`` between attempts. If the
        wrapper's ``_allow_fallback`` is False (no SODA_TEST_SNAPSHOT_FALLBACK)
        the exception propagates as a plain test failure.
        """
        test_id = self._current_test_id
        if test_id is not None and self._allow_fallback:
            _PENDING_RERUN.setdefault(test_id, str(exc))
            logger.info(
                f"SNAPSHOT: queued rerun for {test_id} — wrapper id={id(self)} "
                f"primary_snapshot={'yes' if self._primary_snapshot else 'no'} "
                f"reason={type(exc).__name__}"
            )
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
        # replay
        try:
            entry = self._next_replay_entry("query", sql)
        except SnapshotMismatchError as e:
            self._signal_rerun_or_fail(e)
            return None  # pragma: no cover (signal always raises)
        if log_query:
            sql_logger.debug("SNAPSHOT: replaying logs for query:")
            sql_logger.debug(
                f"SQL query fetchall in datasource {self.name} "
                f"(first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}"
            )
        return entry.result

    def execute_update(self, sql: str, log_query: bool = True) -> int:
        self._handle_test_boundary()

        if self._current_test_id is None:
            return self._passthrough_or_fail("execute_update", sql, log_query=log_query)

        if self._mode == "record":
            result = self._real.execute_update(sql, log_query=log_query)
            # Store None as the result — execute_update return values are driver-specific
            # (e.g. psycopg3 returns the cursor itself) and rarely used by callers.
            self._record_entry(SnapshotEntry("update", sql, None))
            return result
        # replay
        try:
            self._next_replay_entry("update", sql)
        except SnapshotMismatchError as e:
            self._signal_rerun_or_fail(e)
            return 0  # pragma: no cover
        sql_logger.debug(f"SNAPSHOT: captured update: {sql[:50]}...")
        return 0

    def execute_query_one_by_one(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> Optional[tuple[tuple]]:
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
        # replay
        try:
            entry = self._next_replay_entry("query_one_by_one", sql)
        except SnapshotMismatchError as e:
            self._signal_rerun_or_fail(e)
            return None  # pragma: no cover
        description, rows = entry.result
        rows_processed = 0
        for row in rows:
            if row_limit is not None and rows_processed >= row_limit:
                break
            rows_processed += 1
            row_callback(row, description)
        return description

    def execute_query_one_by_one_prefer_streaming(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> Optional[tuple[tuple]]:
        # The streaming-preference routing is a real-connection concern; the
        # record/replay semantics are identical to the base one-by-one.
        # Without this explicit wrapper, __getattr__ would forward the call
        # straight to the real connection and bypass snapshot capture.
        return self.execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)

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
                yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
            finally:
                logger.debug("SNAPSHOT: record query_iterate generator closed")
            return
        # replay
        try:
            entry = self._next_replay_entry("query_iterate", sql)
        except SnapshotMismatchError as e:
            self._signal_rerun_or_fail(e)
            return  # pragma: no cover
        rows, cursor_description = entry.result
        try:
            yield QueryResultIterator(FakeCursor(rows, cursor_description, len(rows)), format_row=tuple)
        finally:
            logger.debug("SNAPSHOT: replay query_iterate generator closed")

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
    # Swap hooks (used by snapshot_pytest_plugin between attempts)
    # -------------------------------------------------------------------------

    def swap_to_real_for_rerun(self) -> bool:
        """Detach the wrapper from its DataSourceImpl, replacing it with the real connection.

        The pytest rerun plugin calls this between the failed first attempt
        and the rerun. After this swap, ``data_source_impl.data_source_connection``
        is a plain ``DataSourceConnection`` — exactly what the test would see
        in ``SODA_TEST_SNAPSHOT=off`` mode. The rerun's SQL goes straight to
        the real DB; no snapshot wrapper, no boundary handler, no recording.

        Returns ``True`` if the swap happened (caller must call
        ``restore_from_rerun`` afterwards), ``False`` if there's nothing to
        swap (secondary wrapper, missing back-ref, or no real connection
        available).
        """
        # Secondaries aren't installed on a DataSourceImpl — they're returned
        # by patched create_additional_connection for direct caller use.
        # During the rerun, the patched method bypasses entirely and returns
        # a real connection, so any pre-existing secondary is harmlessly
        # orphaned. Nothing to swap here.
        if self._primary_snapshot is not None:
            return False
        if self._data_source_impl is None:
            return False
        try:
            self._ensure_real_connection()
        except Exception:
            return False
        if self._real is None:
            return False
        # Drop any partial replay state so we don't leak it on restore.
        self._replay_data = None
        self._replay_index = 0
        # Clear the current-test pointer for the duration of the rerun so
        # that any caller still holding a wrapper reference (e.g. via
        # _LIVE_SNAPSHOT_WRAPPERS or a leaked __getattr__) observes a wrapper
        # that's clearly detached, not one still claiming to be inside the
        # test that's now running off-mode. restore_from_rerun re-attaches by
        # putting the wrapper back on the impl and clearing this again.
        self._current_test_id = None
        # Detach: data_source_impl now points at the real connection directly.
        # The wrapper instance is still alive (held by the plugin's swap list
        # + _LIVE_SNAPSHOT_WRAPPERS), but no longer in the call chain.
        self._data_source_impl.data_source_connection = self._real
        return True

    def restore_from_rerun(self) -> None:
        """Re-attach the wrapper to its DataSourceImpl after the rerun completes.

        Pair with ``swap_to_real_for_rerun``. Putting the wrapper back means
        subsequent tests (on the same session) keep replaying snapshots
        normally.
        """
        if self._data_source_impl is None:
            return
        # The data_source_impl currently points at the real connection (or
        # None if the test body explicitly closed it). Restore it to the
        # wrapper regardless — the wrapper still holds ``self._real`` and
        # will continue to use it for the next test's potential rerun.
        self._data_source_impl.data_source_connection = self
        # Reset boundary state so the next test loads a fresh snapshot.
        self._current_test_id = None
