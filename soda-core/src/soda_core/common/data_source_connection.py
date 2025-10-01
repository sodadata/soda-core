from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional
import os

from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger

from tabulate import tabulate

SODA_DEBUG_PRINT_VALUE_MAX_CHARS = int(os.environ.get("SODA_DEBUG_PRINT_VALUE_MAX_CHARS", 256))
SODA_DEBUG_PRINT_RESULT_MAX_ROWS = int(os.environ.get("SODA_DEBUG_PRINT_RESULT_MAX_ROWS", 20))
SODA_DEBUG_PRINT_SQL_MAX_CHARS = int(os.environ.get("SODA_DEBUG_PRINT_SQL_MAX_CHARS", 1024))

class DataSourceConnection(ABC):
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

    def execute_query(self, sql: str) -> QueryResult:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            logger.debug(f"SQL query fetchall in datasource {self.name} (first {SODA_DEBUG_PRINT_SQL_MAX_CHARS} chars): \n{self.truncate_sql(sql)}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            formatted_rows = self.format_rows(rows)
            truncated_rows = self.truncate_rows(formatted_rows)
            headers = [self._execute_query_get_result_row_column_name(c) for c in cursor.description]
            table_text: str = tabulate(
                truncated_rows,
                headers=headers,
                tablefmt="github",
            )

            logger.debug(f"SQL query result (max {SODA_DEBUG_PRINT_RESULT_MAX_ROWS} rows, {SODA_DEBUG_PRINT_VALUE_MAX_CHARS} chars per string):\n{table_text}")
            return QueryResult(rows=formatted_rows, columns=cursor.description)
        finally:
            cursor.close()

    def format_rows(self, rows: list[tuple]) -> list[tuple]:
        return rows

    def truncate_sql(self, sql: str) -> str:
        """Truncate large strings in sql to a reasonable length."""
        max_chars = SODA_DEBUG_PRINT_SQL_MAX_CHARS
        if len(sql) > max_chars:
            return sql[:max_chars] + "..."
        return sql

    def truncate_rows(self, rows: list[tuple]) -> list[tuple]:
        """Truncate large strings in rows to a reasonable length, and return only the first n rows."""
        max_chars = SODA_DEBUG_PRINT_VALUE_MAX_CHARS
        max_rows = SODA_DEBUG_PRINT_RESULT_MAX_ROWS
        def truncate_cell(cell: Any) -> Any:
            if isinstance(cell, str) and len(cell) > max_chars:
                return cell[:max_chars] + "..."
            return cell
        rows = [tuple(truncate_cell(cell) for cell in row) for row in rows]
        if len(rows) > max_rows:
            rows = rows[:max_rows]
        return rows

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column.name

    def execute_update(self, sql: str) -> UpdateResult:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            logger.debug(f"SQL update: \n{sql}")
            updates = cursor.execute(sql)
            self.commit()
            return updates
        finally:
            cursor.close()

    def execute_query_one_by_one(self, sql: str, row_callback: Callable[[tuple, tuple[tuple]], None]) -> None:
        """
        usage: execute_query_one_by_one("SELECT ...", lambda row, description: your_handle_row(row))
        """
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            logger.debug(f"SQL query fetch one-by-one:\n{sql}")
            cursor.execute(sql)

            row = cursor.fetchone()
            while row:
                row_callback(row, cursor.description)
                row = cursor.fetchone()

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
