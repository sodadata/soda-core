from __future__ import annotations

from pathlib import Path
from typing import Union

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksConnectionProperties,
)
from databricks import sql
import logging
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


class DatabricksDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: DatabricksConnectionProperties,
    ):
        return sql.connect(
            _user_agent_entry="Soda Core",
            **config.to_connection_kwargs(),
        )

    def rollback(self) -> None:
        # Databricks does not support transactions.
        pass

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]
