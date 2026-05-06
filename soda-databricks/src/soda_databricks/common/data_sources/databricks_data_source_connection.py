from __future__ import annotations

import logging
from datetime import tzinfo

from databricks import sql
from soda_core.common.data_source_connection import DataSourceConnection, parse_session_timezone
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksConnectionProperties,
)

logger: logging.Logger = soda_logger


class DatabricksDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: DatabricksConnectionProperties,
    ):
        return sql.connect(
            user_agent_entry="Soda Core",
            **config.to_connection_kwargs(),
        )

    def _fetch_session_timezone(self) -> tzinfo:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT current_timezone()")
            row = cursor.fetchone()
        return parse_session_timezone(row[0] if row else "")

    def rollback(self) -> None:
        # We do not start any transactions, Databricks default is autocommit.
        pass

    def commit(self) -> None:
        # We do not start any transactions, Databricks default is autocommit.
        pass

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]
