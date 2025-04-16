from __future__ import annotations

from pathlib import Path
from typing import Union

from snowflake import connector
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_snowflake.model.data_source.snowflake_connection_properties import (
    SnowflakeConnectionProperties,
)
import logging
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


class SnowflakeDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: SnowflakeConnectionProperties,
    ):
        return connector.connect(
            application="Soda Core",
            **config.to_connection_kwargs(),
        )
