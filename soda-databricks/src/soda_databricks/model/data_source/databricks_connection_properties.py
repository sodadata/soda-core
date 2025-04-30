from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Optional, Dict, Literal, ClassVar

from pydantic import Field, SecretStr
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class DatabricksConnectionProperties(DataSourceConnectionProperties, ABC): ...


class DatabricksSharedConnectionProperties(DatabricksConnectionProperties, ABC):
    host: str = Field(..., description="Databricks workspace hostname (e.g. 'abc.cloud.databricks.com')")
    http_path: str = Field(..., description="HTTP path for the SQL endpoint or cluster")
    catalog: str = Field(None, description="Default catalog to use")
    warehouse: Optional[str] = Field(None, description="Optional warehouse")
    session_configuration: Optional[Dict[str, str]] = Field(None, description="Optional session configuration dict")

    field_mapping: ClassVar[Dict[str, str]] = {
        "host": "server_hostname",
    }


class DatabricksTokenAuth(DatabricksSharedConnectionProperties):
    access_token: SecretStr = Field(..., description="Personal access token")
