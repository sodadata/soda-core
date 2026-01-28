from __future__ import annotations

import logging
from abc import ABC
from typing import Literal, Union

from pydantic import Field
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerActiveDirectoryAuthentication,
    SqlServerActiveDirectoryInteractiveAuthentication,
    SqlServerActiveDirectoryPasswordAuthentication,
    SqlServerActiveDirectoryServicePrincipalAuthentication,
    SqlServerConnectionProperties,
    SqlServerDataSourceConnection,
    SqlServerPasswordAuth,
)

logger: logging.Logger = soda_logger


CONTEXT_AUTHENTICATION_DESCRIPTION = "Use context authentication"


# All of these classes are just copies of the SQLServerConnectionProperties classes, but with the Synapse type
class SynapseConnectionProperties(SqlServerConnectionProperties, ABC):
    pass


class SynapsePasswordAuth(SqlServerPasswordAuth, SynapseConnectionProperties):
    pass


class SynapseActiveDirectoryAuthentication(SqlServerActiveDirectoryAuthentication, SynapseConnectionProperties):
    pass


class SynapseActiveDirectoryInteractiveAuthentication(
    SqlServerActiveDirectoryInteractiveAuthentication, SynapseConnectionProperties
):
    pass


class SynapseActiveDirectoryPasswordAuthentication(
    SqlServerActiveDirectoryPasswordAuthentication, SynapseConnectionProperties
):
    pass


class SynapseActiveDirectoryServicePrincipalAuthentication(
    SqlServerActiveDirectoryServicePrincipalAuthentication, SynapseConnectionProperties
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


class SynapseDataSourceConnection(SqlServerDataSourceConnection):
    def _get_autocommit_setting(self) -> bool:
        return True  # Synapse requires autocommit to be True.
