from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger

from tabulate import tabulate


class DataSourceConnection(ABC):
    def __init__(self, name: str, connection_properties: dict):
        self.name: str = name
        self.connection_properties: dict = connection_properties
        self.connection: Optional[object] = None
        # Auto-open on creation.  See DataSource.open_connection()
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
            logger.debug(f"SQL query fetchall: \n{sql}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            table_text: str = tabulate(
                rows,
                headers=[self._execute_query_get_result_row_column_name(c) for c in cursor.description],
                tablefmt="github",
            )

            logger.debug(f"SQL query result:\n{table_text}")
            return QueryResult(rows=rows, columns=cursor.description)
        finally:
            cursor.close()

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
