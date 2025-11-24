from __future__ import annotations

import logging
import time
from abc import ABC
from typing import Any, Literal, Optional, Union

import adbc_driver_flightsql.dbapi as dbapi
from pydantic import Field, IPvAnyAddress, SecretStr
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.exceptions import DataSourceConnectionException
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger

DEFAULT_PORT = 32010


class DremioConnectionProperties(DataSourceConnectionProperties, ABC):
    host: Union[str, IPvAnyAddress] = Field(..., description="Database host (hostname or IP address)")
    port: int = Field(DEFAULT_PORT, description="Database port (1-65535)", ge=1, le=65535)
    username: str = Field(..., description="Dremio username")
    password: SecretStr = Field(..., description="Dremio password")
    token: Optional[str] = Field(None, description="Dremio access token")
    use_encryption: Optional[bool] = Field(False, description="Define if encryption is used or not.")
    routing_queue: Optional[str] = Field(None, description="Identifier for the routing queue.")
    disable_certificate_verification: Optional[bool] = Field(
        False, description="If True, Dremio does not verify host certificate."
    )


class DremioDataSource(DataSourceBase, ABC):
    type: Literal["dremio"] = Field("dremio")
    connection_properties: DremioConnectionProperties = Field(
        ..., alias="connection", description="Dremio connection configuration"
    )


class DremioDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: DremioConnectionProperties,
    ):
        try:
            # Use grpc:// for unencrypted, grpc+tls:// for encrypted connections
            scheme = "grpc+tls" if config.use_encryption else "grpc"
            uri = f"{scheme}://{config.host}:{config.port}"

            db_kwargs = {
                "username": config.username,
                "password": config.password.get_secret_value(),
            }

            # Certificate verification
            if config.disable_certificate_verification:
                db_kwargs["adbc.flight.sql.client_option.tls_skip_verify"] = "true"

            # Routing queue
            if config.routing_queue:
                db_kwargs["adbc.flight.sql.client_option.routing_queue"] = config.routing_queue

            # Token authentication (if provided, use token instead of username/password)
            if config.token:
                db_kwargs["adbc.flight.sql.client_option.authorization_header"] = f"Bearer {config.token}"

            # Enable autocommit for DDL statements (CREATE TABLE, etc.) to persist
            self.connection = dbapi.connect(uri, db_kwargs=db_kwargs, autocommit=True)
            return self.connection
        except Exception as e:
            raise DataSourceConnectionException(e) from e

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]

    def _cursor_execute_update_and_commit(self, cursor: Any, sql: str):
        # ADBC DBAPI cursors use execute() for all operations (including DDL/DML)
        # Autocommit is enabled at connection level, so no explicit commit needed
        updates = cursor.execute(sql)
        time.sleep(1)  # there's a race condition
        return updates
