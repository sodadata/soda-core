from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger

from tabulate import tabulate


class DataSourceConnection(ABC):
    MAX_CHARS_PER_STRING = int(os.environ.get("SODA_DEBUG_PRINT_VALUE_MAX_CHARS", 256))
    MAX_ROWS = int(os.environ.get("SODA_DEBUG_PRINT_RESULT_MAX_ROWS", 20))
    MAX_CHARS_PER_SQL = int(os.environ.get("SODA_DEBUG_PRINT_SQL_MAX_CHARS", 1024))

    def __init__(self, name: str, connection_properties: dict, connection: Optional[object] = None):
        self.name: str = name
        self.connection_properties: dict = connection_properties
        self.connection: Optional[object] = connection

        # Auto-open on creation if no connection already supplied. See DataSource.open_connection()
        self.open_connection()

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
            formatted_rows = self.format_rows(rows)
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

            logger.debug(
                f"SQL query result (max {self.MAX_ROWS} rows, {self.MAX_CHARS_PER_STRING} chars per string):\n{table_text}"
            )
            return QueryResult(rows=formatted_rows, columns=cursor.description)
        finally:
            cursor.close()

    def format_rows(self, rows: list[tuple]) -> list[tuple]:
        return rows

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

    def execute_update(self, sql: str, log_query: bool = True) -> UpdateResult:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            if log_query:
                logger.debug(f"SQL update (first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}")
            return self._cursor_execute_update_and_commit(cursor, sql)

        finally:
            cursor.close()

    def _cursor_execute_update_and_commit(self, cursor: Any, sql: str):
        updates = cursor.execute(sql)
        self.commit()
        return updates
 
    
    def execute_query_one_by_one(
        self, sql: str, row_callback: Callable[[tuple, tuple[tuple]], None], log_query: bool = True
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

            row = cursor.fetchone()
            while row:
                row_callback(row, description)
                row = cursor.fetchone()

        finally:
            cursor.close()
        return description

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
