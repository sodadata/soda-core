from __future__ import annotations

from abc import ABC
from ipaddress import IPv4Address, IPv6Address
from typing import Literal, Optional, Union
import trino
from pydantic import Field

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class TrinoConnectionProperties(DataSourceConnectionProperties):
    host: str = Field(..., description="Database host")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    catalog: str = Field(..., description="Database catalog")
    port: int = Field(443, description="Database port")



class TrinoDataSource(DataSourceBase, ABC):
    type: Literal["trino"] = Field("trino")

    connection_properties: TrinoConnectionProperties = Field(..., alias="connection", description="Trino connection configuration")


class TrinoDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    
    def _create_connection(
        self,
        config: TrinoConnectionProperties,
    ):
        connect_kwargs = {
            "host": config.host,
            "port": config.port,
            "catalog": config.catalog,
            "username": config.username,
            "password": config.password,
        }
        self.connection = trino.dbapi.connect(**connect_kwargs)