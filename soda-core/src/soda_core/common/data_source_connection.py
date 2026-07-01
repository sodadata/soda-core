from __future__ import annotations

import contextlib
import logging
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from soda_core.common.data_source_results import QueryResult, QueryResultIterator
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger

from tabulate import tabulate


class MemoryOptimizedDriverSettings:
    """Process-level enablement for the bounded-memory streaming fetch driver.

    Enabled when EITHER the Soda Cloud override (``configure``, plumbed in by
    soda-extensions from the ``useMemoryOptimized`` feature flag) OR the env var
    ``MEMORY_OPTIMIZED_DRIVER_ENABLED`` is on; DISABLED by default.

    When disabled, every connection's ``execute_query_one_by_one_prefer_streaming``
    uses the buffered base fetch. When enabled, connections that advertise a
    low-memory fetch (``supports_streaming_fetch``, e.g. postgres' server-side
    streaming cursor) use it. Disabled by default because the streaming path trades
    throughput for bounded peak memory and regresses DWH runtime on fast /
    low-latency sources.

    Process-scoped (a single module instance) because the Soda Cloud flag is
    resolved during diagnostics-config parse, before any DWH/source connection
    exists, and connections are reused across scans. The env var is read at call
    time so it can be toggled per-process (or per-test via ``monkeypatch.setenv``).

    Operator guidance: enable this (set ``MEMORY_OPTIMIZED_DRIVER_ENABLED=true``)
    when reads of very wide source rows run out of memory — without it the source
    read buffers the whole result client-side. Only postgres sources benefit today
    (other adapters fall back to the buffered read regardless). Expect a throughput
    cost (~14% slower @100k rows) in exchange for bounded peak memory.
    """

    ENV_VAR: str = "MEMORY_OPTIMIZED_DRIVER_ENABLED"
    _TRUTHY: tuple[str, ...] = ("1", "true", "yes", "on")

    def __init__(self) -> None:
        # Override set from the Soda Cloud diagnostics-warehouse config; ORed with
        # the env toggle so either source can turn the driver on.
        self._override: bool = False
        # One-off guard so the "driver active" line is logged once per process,
        # not once per query.
        self._active_logged: bool = False

    def configure(self, enabled: Optional[bool]) -> None:
        """Set the Soda Cloud override. A falsy value leaves the env var as the
        sole control."""
        self._override = bool(enabled)

    def is_enabled(self) -> bool:
        if self._override:
            return True
        return os.getenv(self.ENV_VAR, "").strip().lower() in self._TRUTHY

    def log_active_once(self, adapter_name: str) -> None:
        if not self._active_logged:
            self._active_logged = True
            logger.info("Memory-optimized fetch driver active (%s)", adapter_name)

    def reset(self) -> None:
        """Restore defaults — for test isolation between cases."""
        self._override = False
        self._active_logged = False


# Single process-level instance. State lives here rather than in module globals so
# it is encapsulated and unit-testable (see MemoryOptimizedDriverSettings.reset).
memory_optimized_driver_settings = MemoryOptimizedDriverSettings()


# IANA names that mean "UTC, no DST, no historical offsets". When ZoneInfo returns one
# of these, we collapse to ``timezone.utc`` so adapter outputs are uniform regardless of
# whether the driver reports the literal string ``"UTC"`` or one of the IANA aliases
# below. The Etc/GMT entries are tricky — note that Etc/GMT+0 / Etc/GMT-0 are both
# zero-offset variants (Etc/GMT+N has *negative* offset N hours per POSIX inversion,
# but +0/-0 collapse to UTC). GMT0 is a System V-style zero-offset zone alias.
_UTC_ALIAS_ZONE_KEYS = frozenset(
    {
        "UTC",
        "Etc/UTC",
        "Universal",
        "Etc/Universal",
        "Zulu",
        "Etc/Zulu",
        "GMT",
        "GMT0",
        "GMT+0",
        "GMT-0",
        "Etc/GMT",
        "Etc/GMT0",
        "Etc/GMT+0",
        "Etc/GMT-0",
        "Greenwich",
        "Etc/Greenwich",
    }
)

# Driver outputs that mean "use the host's local TZ at the time of the query". Postgres
# returns ``localtime`` from ``SHOW timezone`` when the server is configured that way.
# Trino's ``current_timezone()`` can return ``system`` for a similar setup. Both should
# resolve to the engine host's actual local TZ so naive returns from the source can be
# canonicalized correctly. Without this, ``parse_session_timezone`` would reject the
# literal as unparseable and the connection wrapper would fall back to UTC, silently
# shifting wallclocks by the host's offset for every naive return from a TZ-aware column.
_HOST_LOCAL_LITERAL_VALUES = frozenset({"localtime", "system"})


def parse_session_timezone(value: Optional[str]) -> tzinfo:
    """Parse a session-timezone string returned by a database driver into a tzinfo.

    Accepts IANA names ('America/Los_Angeles'), 'UTC'/'GMT'/'Z', UTC-equivalent IANA
    aliases ('Etc/UTC', 'Universal', 'Zulu', etc.), or numeric offsets like '+00:00',
    '-08:00', '+0530', 'UTC+02:00'. Every UTC-equivalent input — the literal strings,
    the alias zones, and zero numeric offsets — is normalized to ``timezone.utc`` (the
    singleton) so adapter outputs are uniform across vendors that report UTC by name,
    by alias, or by offset.

    Empty / None / whitespace-only input is treated as "no session TZ configured" and
    returns ``timezone.utc`` silently — the conventional fallback every adapter uses
    on a missing-row query result.

    Raises ``ValueError`` on non-empty unparseable input. The single legitimate
    fallback site is ``DataSourceConnection.get_session_timezone()``, which catches
    and logs one warning before returning UTC.
    """
    if value is None:
        return timezone.utc
    # Strip outer whitespace and quotes in a single pass — drivers may quote their
    # result, and the body of those quotes may also be empty / whitespace-only.
    text = value.strip(" \r\n\t'\"")
    if not text:
        return timezone.utc
    # Case-insensitive match for the UTC literals. ZoneInfo lookup below is
    # case-sensitive (``ZoneInfo("utc")`` raises ZoneInfoNotFoundError), so we
    # have to normalize these here. ``Z`` is the ISO-8601 alias for UTC and is
    # not in the IANA database at all, so it can only be caught at this stage.
    if text.upper() in ("UTC", "GMT", "Z"):
        return timezone.utc

    if text.lower() in _HOST_LOCAL_LITERAL_VALUES:
        # Driver said "use the host local TZ". Resolve via the OS now. The result is a
        # fixed-offset tzinfo derived from the current local time — DST drift across a
        # long-lived connection is the same as for SQL Server / DB2 (out of scope per
        # the team's "DWH connections are short-lived" stance), but the wallclock at
        # canonicalization time is correct.
        return datetime.now().astimezone().tzinfo

    try:
        zi = ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        zi = None
    if zi is not None:
        # Collapse UTC-equivalent IANA aliases to ``timezone.utc`` so an adapter
        # reporting ``"Etc/UTC"`` returns the same singleton as one reporting the
        # literal ``"UTC"`` or the ``+00:00`` numeric offset.
        if zi.key in _UTC_ALIAS_ZONE_KEYS:
            return timezone.utc
        return zi

    match = re.match(r"^(?:UTC|GMT)?([+-])(\d{1,2}):?(\d{2})?$", text)
    if match:
        sign = -1 if match.group(1) == "-" else 1
        hours = int(match.group(2))
        minutes = int(match.group(3) or 0)
        # Reject components out of range explicitly. The regex's ``\d{2}`` for the
        # minutes group accepts up to ``99``, which would otherwise spill via timedelta
        # arithmetic into a higher hour bucket — e.g. ``+05:99`` becomes ``06:39``,
        # silently producing a real-but-wrong offset for what is actually junk input.
        if hours >= 24 or minutes >= 60:
            raise ValueError(f"could not parse session timezone {text!r}")
        offset = sign * timedelta(hours=hours, minutes=minutes)
        if offset == timedelta(0):
            return timezone.utc
        return timezone(offset)

    raise ValueError(f"could not parse session timezone {text!r}")


class DataSourceConnection(ABC):
    MAX_CHARS_PER_STRING = int(os.environ.get("SODA_DEBUG_PRINT_VALUE_MAX_CHARS", 256))
    MAX_ROWS = int(os.environ.get("SODA_DEBUG_PRINT_RESULT_MAX_ROWS", 20))
    MAX_CHARS_PER_SQL = int(os.environ.get("SODA_DEBUG_PRINT_SQL_MAX_CHARS", 1024))

    def __init__(
        self,
        name: str,
        connection_properties: dict,
        connection: Optional[object] = None,
    ):
        self.name: str = name
        self.connection_properties: dict = connection_properties
        self.connection: Optional[object] = connection
        self._session_timezone_cache: Optional[tzinfo] = None

        # Auto-open on creation if no connection already supplied. See DataSource.open_connection()
        self.open_connection()

    def get_session_timezone(self) -> tzinfo:
        """Return the live session timezone of this connection.

        Result is cached for the connection's lifetime. Subclasses MUST override
        :meth:`_fetch_session_timezone` to declare how their backend reports the
        session TZ. If a subclass forgets to override (or the override raises
        unexpectedly), this method catches the failure, logs a single warning,
        and falls back to UTC — but the warning makes the missing implementation
        visible rather than silently treating UTC as the global default.
        """
        if self._session_timezone_cache is None:
            try:
                self._session_timezone_cache = self._fetch_session_timezone()
            except Exception as e:
                logger.warning(f"Failed to query session timezone on '{self.name}'; defaulting to UTC: {e}")
                self._session_timezone_cache = timezone.utc
        return self._session_timezone_cache

    def _fetch_session_timezone(self) -> tzinfo:
        """Query the backend for the live session timezone and return as a ``tzinfo``.

        Subclasses MUST override. Raising ``NotImplementedError`` here (rather than
        silently returning UTC) makes the contract explicit for new adapters: a
        contributor adding a new vendor will see an immediate failure if they
        forget the override, instead of every cross-source DWH transfer through
        their adapter silently treating session TZ as UTC.

        Recommended implementation:
        * Vendors with a single source of session-TZ truth (Postgres ``SHOW
          timezone``, Snowflake ``SHOW PARAMETERS LIKE 'TIMEZONE'``, Trino /
          Databricks ``current_timezone()``, DuckDB ``current_setting('TimeZone')``,
          Oracle ``SESSIONTIMEZONE``, DB2 ``CURRENT TIMEZONE``): query, then route
          the resulting string through ``parse_session_timezone`` for normalization.
        * Vendors that are UTC-only by design (BigQuery, Athena, Dremio): return
          ``timezone.utc`` directly.
        * Vendors whose connection wraps a session that may be reconfigured by the
          caller (SparkDF's ``from_existing_session``): query the live setting
          (e.g. ``self.session.conf.get("spark.sql.session.timeZone")``) rather
          than hard-coding UTC.

        ``get_session_timezone`` (the public accessor) wraps this in a try/except
        that logs a single warning and falls back to UTC, so a runtime failure of
        an existing override (driver upgrade, permission change) won't crash the
        caller — it surfaces as a single warning + UTC.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must override _fetch_session_timezone(). "
            "See the docstring on DataSourceConnection._fetch_session_timezone for the "
            "recommended pattern. Vendors that are UTC-only by design should explicitly "
            "return timezone.utc."
        )

    def __enter__(self):
        pass

    @abstractmethod
    def _create_connection(self, connection_yaml_dict: dict) -> object:
        """
        self.connection_yaml_dict is provided as a parameter for convenience.
        Returns a new DBAPI connection based on the provided connection properties.
        The returned value will be saved in self.connection. See open() for details.
        Exceptions do not need to be handled.  They will be handled by the calling method.
        """

    def open_connection(self) -> None:
        """
        Ensures that an open connection is available.
        After this method ends, the open connection must have the database_name and optionally
        the schema_name as context as far as these are applicable for the specific data source type.
        Potentially closes and re-opens the self.connection if the existing connection has a different database
        or schema context compared to the given database_name and schema_name for the next contract.
        """
        if self.connection is None:
            try:
                logger.debug(f"'{self.name}' connection properties: {self.connection_properties}")
                self.connection = self._create_connection(self.connection_properties)
            except Exception as e:
                logger.error(msg=f"Could not connect to '{self.name}': {e}", exc_info=True)

    def close_connection(self) -> None:
        """
        Closes te connection. This method will not throw any exceptions.
        Check errors with has_errors or assert_no_errors.
        """
        if self.connection:
            try:
                # noinspection PyUnresolvedReferences
                self.connection.close()
            except Exception as e:
                logger.warning(msg=f"Could not close the DBAPI connection", exc_info=True)
            finally:
                self.connection = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            if log_query:
                logger.debug(
                    f"SQL query fetchall in datasource {self.name} (first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}"
                )

            cursor.execute(sql)
            rows = cursor.fetchall()
            formatted_rows = self._format_rows(rows)
            truncated_rows = self.truncate_rows(formatted_rows)
            headers = [self._execute_query_get_result_row_column_name(c) for c in cursor.description]
            # The tabulate can crash if the rows contain non-ASCII characters.
            # This is purely for debugging/logging purposes, so we can try/catch this.
            try:
                table_text: str = tabulate(
                    truncated_rows,
                    headers=headers,
                    tablefmt="github",
                )
            except UnicodeDecodeError as e:
                logger.debug(f"Error formatting rows. These may contain non-ASCII characters. {e}")
                table_text = "Error formatting rows. These may contain non-ASCII characters."
            except Exception as e:
                logger.debug(f"Error formatting rows. {e}")
                table_text = f"Error formatting rows. This may be due to the rows containing non-ASCII characters.\n{e}"

            logger.debug(
                f"SQL query result (max {self.MAX_ROWS} rows, {self.MAX_CHARS_PER_STRING} chars per string):\n{table_text}"
            )
            return QueryResult(rows=formatted_rows, columns=cursor.description)
        finally:
            cursor.close()

    def _format_rows(self, rows: list[tuple]) -> list[tuple]:
        return rows

    def _format_row(self, row: tuple) -> tuple:
        return row

    def truncate_sql(self, sql: str) -> str:
        """Truncate large strings in sql to a reasonable length."""
        if len(sql) > (self.MAX_CHARS_PER_SQL - 3):
            return sql[: self.MAX_CHARS_PER_SQL - 3] + "..."
        return sql

    def truncate_rows(self, rows: list[tuple]) -> list[tuple]:
        """Truncate large strings in rows to a reasonable length, and return only the first n rows."""

        def truncate_cell(cell: Any) -> Any:
            if isinstance(cell, str) and len(cell) > (self.MAX_CHARS_PER_STRING - 3):
                return cell[: self.MAX_CHARS_PER_STRING - 3] + "..."
            return cell

        if len(rows) > self.MAX_ROWS:
            rows = rows[: self.MAX_ROWS]
        rows = [tuple(truncate_cell(cell) for cell in row) for row in rows]

        return rows

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column.name

    def execute_update(self, sql: str, log_query: bool = True) -> int:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            if log_query:
                logger.debug(f"SQL update (first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}")
            return self._cursor_execute_update_and_commit(cursor, sql)

        finally:
            cursor.close()

    def _cursor_execute_update_and_commit(self, cursor: Any, sql: str) -> int:
        cursor.execute(sql)
        try:
            rowcount = cursor.rowcount
            rowcount = rowcount if isinstance(rowcount, int) and rowcount >= 0 else 0
        except Exception:
            rowcount = 0
        self.commit()
        return rowcount

    def execute_query_one_by_one(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        """
        usage: execute_query_one_by_one("SELECT ...", lambda row, description: your_handle_row(row))
        Returns the description of the query.
        """
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            if log_query:
                logger.debug(f"SQL query fetch one-by-one:\n{sql}")
            cursor.execute(sql)

            description: tuple[tuple] = cursor.description

            rows_processed: int = 0

            row = cursor.fetchone()
            while row and (row_limit is None or rows_processed < row_limit):
                rows_processed += 1
                row_callback(row, description)
                row = cursor.fetchone()

        finally:
            cursor.close()
        return description

    def supports_streaming_fetch(self) -> bool:
        """Whether this adapter implements a real bounded-memory streaming fetch
        (i.e. overrides ``_execute_query_one_by_one_streaming``). Base: ``False``;
        postgres returns ``True``. Used by
        ``execute_query_one_by_one_prefer_streaming`` to decide whether the
        streaming impl is worth dispatching to."""
        return False

    def execute_query_one_by_one_prefer_streaming(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        """Execute ``sql`` and deliver rows one-by-one to ``row_callback``,
        *preferring* a bounded-memory streaming fetch when it is both enabled and
        available — otherwise the plain buffered fetch (same external contract).

        Callers that stream potentially large result sets through Python — the
        diagnostics-warehouse failed-rows flows — use this instead of the base
        ``execute_query_one_by_one``. The name says *preferring*, not
        *guaranteeing*: streaming engages only when the driver is enabled
        (``MEMORY_OPTIMIZED_DRIVER_ENABLED`` env var or the Soda Cloud override)
        AND this adapter advertises a streaming impl (``supports_streaming_fetch``).
        Today only postgres does (server-side named cursor, byte-budgeted batches,
        bounding peak memory to ~one batch plus the largest single row); every other
        adapter — and every adapter when the driver is disabled — keeps the proven
        buffered behavior.
        """
        # Two terms, one feature: the "memory-optimized driver" is the operator-facing
        # toggle (the MEMORY_OPTIMIZED_DRIVER_ENABLED env var / Soda Cloud flag);
        # "streaming" is the fetch mechanism it prefers. So: driver enabled AND this
        # adapter can stream → stream, otherwise buffer. The "driver active" log lives
        # in the streaming impl (it fires only when streaming actually engages, not when
        # an adapter accepts the call but then falls back, e.g. postgres in autocommit).
        if memory_optimized_driver_settings.is_enabled() and self.supports_streaming_fetch():
            return self._execute_query_one_by_one_streaming(
                sql=sql, row_callback=row_callback, log_query=log_query, row_limit=row_limit
            )
        return self.execute_query_one_by_one(
            sql=sql, row_callback=row_callback, log_query=log_query, row_limit=row_limit
        )

    def _execute_query_one_by_one_streaming(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        """Bounded-memory streaming fetch. Only reached when this adapter advertises
        the capability via ``supports_streaming_fetch`` returning ``True``, so the
        base raises — advertise *and* implement (e.g. postgres' server-side cursor)."""
        raise NotImplementedError(
            f"{type(self).__name__} advertises supports_streaming_fetch() but does not implement "
            "_execute_query_one_by_one_streaming()"
        )

    @contextlib.contextmanager
    def execute_query_iterate(self, sql: str, log_query: bool = True) -> Iterator[QueryResultIterator]:
        cursor = self.connection.cursor()
        if log_query:
            logger.debug(f"SQL query iterate:\n{sql}")
        try:
            cursor.execute(sql)
            yield QueryResultIterator(cursor, self._format_row)
        finally:
            cursor.close()

    def commit(self) -> None:
        # noinspection PyUnresolvedReferences
        self.connection.commit()

    def rollback(self) -> None:
        # noinspection PyUnresolvedReferences
        self.connection.rollback()

    def disable_close_connection(self) -> None:
        self.close_connection_enabled = False

    def enable_close_connection(self) -> None:
        self.close_connection_enabled = True
