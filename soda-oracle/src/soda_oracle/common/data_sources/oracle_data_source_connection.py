from __future__ import annotations

import logging
from abc import ABC
from typing import Literal, Union

import oracledb
from pydantic import Field, SecretStr
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


CONTEXT_AUTHENTICATION_DESCRIPTION = "Use context authentication"


class OracleConnectionProperties(DataSourceConnectionProperties):
    user: str = Field(..., description="Database user")
    password: SecretStr = Field(..., description="Database password")


# Support two connection methods, host/port/service_name, and connectstring
class OracleHostPortService(OracleConnectionProperties):
    host: str = Field(..., description="Database host")
    port: int = Field(1521, description="Database port", ge=1, le=65535)
    service_name: str = Field(..., description="Oracle service name")


class OracleConnectionString(OracleConnectionProperties):
    connect_string: str = Field(..., description="Oracle connection string")


class OracleDataSource(DataSourceBase, ABC):
    type: Literal["oracle"] = Field("oracle")

    connection_properties: Union[
        OracleHostPortService,
        OracleConnectionString,
    ] = Field(..., alias="connection", description="Oracle connection configuration")


class OracleDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: OracleConnectionProperties,
    ):
        try:
            if isinstance(config, OracleConnectionString):
                return oracledb.connect(
                    user=config.user, 
                    password=config.password.get_secret_value(), 
                    dsn=config.connect_string
                )
            elif isinstance(config, OracleHostPortService):
                dsn = f"{config.host}:{config.port}/{config.service_name}"
                return oracledb.connect(
                    user=config.user,
                    password=config.password.get_secret_value(),
                    dsn=dsn
                )
            else:
                raise ValueError(f"Unsupported Oracle connection configuration type: {type(config)}")
        except Exception as e:
            logger.error(f"Failed to create Oracle connection: {e}")
            raise
