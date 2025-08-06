from __future__ import annotations

import logging
import struct
from abc import ABC
from datetime import datetime, timezone
from typing import Literal, Optional, Union

import pyodbc
from pydantic import Field
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SQLServerActiveDirectoryAuthentication,
    SQLServerActiveDirectoryInteractiveAuthentication,
    SQLServerActiveDirectoryPasswordAuthentication,
    SQLServerActiveDirectoryServicePrincipalAuthentication,
    SQLServerConnectionProperties,
    SQLServerDataSourceConnection,
    SQLServerPasswordAuth,
)

logger: logging.Logger = soda_logger


CONTEXT_AUTHENTICATION_DESCRIPTION = "Use context authentication"


# All of these classes are just copies of the SQLServerConnectionProperties classes, but with the Synapse type
class FabricConnectionProperties(SQLServerConnectionProperties, ABC):
    autocommit: Optional[bool] = Field(
        True, description="Whether to use autocommit"
    )  # Synapse requires autocommit to be True.


class FabricPasswordAuth(SQLServerPasswordAuth, FabricConnectionProperties):
    pass


class FabricActiveDirectoryAuthentication(SQLServerActiveDirectoryAuthentication, FabricConnectionProperties):
    pass


class FabricActiveDirectoryInteractiveAuthentication(
    SQLServerActiveDirectoryInteractiveAuthentication, FabricConnectionProperties
):
    pass


class FabricActiveDirectoryPasswordAuthentication(
    SQLServerActiveDirectoryPasswordAuthentication, FabricConnectionProperties
):
    pass


class FabricActiveDirectoryServicePrincipalAuthentication(
    SQLServerActiveDirectoryServicePrincipalAuthentication, FabricConnectionProperties
):
    pass


class FabricDataSource(DataSourceBase, ABC):
    type: Literal["fabric"] = Field("fabric")

    connection_properties: Union[
        FabricPasswordAuth,
        FabricActiveDirectoryInteractiveAuthentication,
        FabricActiveDirectoryPasswordAuthentication,
        FabricActiveDirectoryServicePrincipalAuthentication,
    ] = Field(..., alias="connection", description="Fabric connection configuration")


def handle_datetime2(dto_value):
    tup = struct.unpack("<6hI", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000)
    return datetime(
        tup[0],
        tup[1],
        tup[2],
        tup[3],
        tup[4],
        tup[5],
        tup[6] // 1000,
        timezone.utc,  # Timezone is not supported by Fabric, so we assume that all timestamps are in UTC (i.e. we do no conversion!)
    )


class FabricDataSourceConnection(SQLServerDataSourceConnection):
    def _get_autocommit_setting(self) -> bool:
        return True  # Synapse requires autocommit to be True.

    def _create_connection(self, config: SQLServerConnectionProperties):
        my_connection = super()._create_connection(config)
        my_connection.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, handle_datetime2)
        return my_connection
