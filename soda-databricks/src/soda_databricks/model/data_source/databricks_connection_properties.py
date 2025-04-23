from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Optional, Dict, Literal

from pydantic import Field, SecretStr
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class DatabricksConnectionProperties(DataSourceConnectionProperties, ABC):
    @abstractmethod
    def to_connection_kwargs(self) -> dict: ...


class DatabricksSharedConnectionProperties(DatabricksConnectionProperties, ABC):
    host: str = Field(..., description="Databricks workspace hostname (e.g. 'abc.cloud.databricks.com')")
    http_path: str = Field(..., description="HTTP path for the SQL endpoint or cluster")
    catalog: str = Field(None, description="Optional default catalog to use")
    warehouse: Optional[str] = Field(None, description="Optional warehouse")
    session_configuration: Optional[Dict[str, str]] = Field(None, description="Optional session configuration dict")

    def to_connection_kwargs(self) -> dict:
        return {
            "server_hostname": self.host,
            "http_path": self.http_path,
            "catalog": self.catalog,
            "warehouse": self.warehouse,
            "session_configuration": self.session_configuration,
        }


class DatabricksTokenAuth(DatabricksSharedConnectionProperties):
    access_token: SecretStr = Field(..., description="Personal access token")

    def to_connection_kwargs(self) -> dict:
        kwargs = super().to_connection_kwargs()
        kwargs["access_token"] = self.access_token.get_secret_value()
        return kwargs
