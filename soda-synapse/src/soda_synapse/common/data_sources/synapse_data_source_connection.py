from __future__ import annotations

import logging
from abc import ABC
from typing import Literal, Union

from pydantic import Field
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
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
class SynapseConnectionProperties(SQLServerConnectionProperties, ABC):
    pass


class SynapsePasswordAuth(SQLServerPasswordAuth, SynapseConnectionProperties):
    pass


class SynapseActiveDirectoryAuthentication(SQLServerActiveDirectoryAuthentication, SynapseConnectionProperties):
    pass


class SynapseActiveDirectoryInteractiveAuthentication(
    SQLServerActiveDirectoryInteractiveAuthentication, SynapseConnectionProperties
):
    pass


class SynapseActiveDirectoryPasswordAuthentication(
    SQLServerActiveDirectoryPasswordAuthentication, SynapseConnectionProperties
):
    pass


class SynapseActiveDirectoryServicePrincipalAuthentication(
    SQLServerActiveDirectoryServicePrincipalAuthentication, SynapseConnectionProperties
):
    pass


class SynapseDataSource(DataSourceBase, ABC):
    type: Literal["synapse"] = Field("synapse")

    connection_properties: Union[
        SynapsePasswordAuth,
        SynapseActiveDirectoryInteractiveAuthentication,
        SynapseActiveDirectoryPasswordAuthentication,
        SynapseActiveDirectoryServicePrincipalAuthentication,
    ] = Field(..., alias="connection", description="Synapse connection configuration")


class SynapseDataSourceConnection(SQLServerDataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)
