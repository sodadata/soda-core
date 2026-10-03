from __future__ import annotations

from abc import ABC
from typing import Literal, Optional, Union

from pydantic import Field, SecretStr
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger = soda_logger


class OdpsConnectionProperties(DataSourceConnectionProperties, ABC):
    """Connection properties for ODPS (MaxCompute)."""

    access_id: str = Field(..., description="Aliyun Access Key ID")
    secret_access_key: SecretStr = Field(..., description="Aliyun Secret Access Key")
    project: str = Field(..., description="ODPS project name")
    endpoint: str = Field(..., description="ODPS endpoint (e.g., https://service.odps.aliyun.com/api)")
    tunnel_endpoint: Optional[str] = Field(None, description="ODPS tunnel endpoint")


class OdpsDataSource(DataSourceBase, ABC):
    """ODPS (MaxCompute) data source definition."""

    type: Literal["odps"] = Field("odps")

    connection_properties: Union[OdpsConnectionProperties] = Field(
        ..., alias="connection", description="ODPS connection configuration"
    )
