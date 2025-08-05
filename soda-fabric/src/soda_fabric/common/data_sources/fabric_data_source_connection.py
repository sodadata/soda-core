from __future__ import annotations

import logging
from abc import ABC
from typing import Literal, Optional, Union

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


class FabricDataSourceConnection(SQLServerDataSourceConnection):
    def _get_autocommit_setting(self) -> bool:
        return True  # Synapse requires autocommit to be True.
