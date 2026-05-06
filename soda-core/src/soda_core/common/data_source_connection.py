from __future__ import annotations

import contextlib
import logging
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import timedelta, timezone, tzinfo
from typing import Any, Callable, Optional

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

from soda_core.common.data_source_results import QueryResult, QueryResultIterator
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger

from tabulate import tabulate


def parse_session_timezone(value: str) -> tzinfo:
    """Parse a session-timezone string returned by a database driver into a tzinfo.

    Accepts IANA names ('America/Los_Angeles'), 'UTC'/'GMT', or numeric offsets like
    '+00:00', '-08:00', '+0530', 'UTC+02:00'. Falls back to UTC if unparseable.
    """
    if not value:
        return timezone.utc
    text = value.strip().strip("'\"")
    if text.upper() in ("UTC", "GMT", "Z"):
        return timezone.utc

    if ZoneInfo is not None:
        try:
            return ZoneInfo(text)
        except (ZoneInfoNotFoundError, ValueError, OSError):
            pass

    match = re.match(r"^(?:UTC|GMT)?([+-])(\d{1,2}):?(\d{2})?$", text)
    if match:
        sign = -1 if match.group(1) == "-" else 1
        hours = int(match.group(2))
        minutes = int(match.group(3) or 0)
        return timezone(sign * timedelta(hours=hours, minutes=minutes))

    logger.warning(f"Could not parse session timezone {text!r}; defaulting to UTC")
    return timezone.utc


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

        Result is cached for the connection's lifetime. Subclasses override
        :meth:`_fetch_session_timezone` to query their backend; the default is UTC.
        """
        if self._session_timezone_cache is None:
            try:
                self._session_timezone_cache = self._fetch_session_timezone()
            except Exception as e:
                logger.warning(
                    f"Failed to query session timezone on '{self.name}'; defaulting to UTC: {e}"
                )
                self._session_timezone_cache = timezone.utc
        return self._session_timezone_cache

    def _fetch_session_timezone(self) -> tzinfo:
        return timezone.utc

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
