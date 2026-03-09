from abc import ABC
from typing import ClassVar, Dict, Optional

from pydantic import Field, SecretStr
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class DatabricksConnectionProperties(DataSourceConnectionProperties, ABC): ...


class DatabricksSharedConnectionProperties(DatabricksConnectionProperties, ABC):
    host: str = Field(
        ...,
        description="Databricks workspace hostname (e.g. 'abc.cloud.databricks.com'). If it starts with https:// or http://, it will be removed.",
    )
    http_path: str = Field(..., description="HTTP path for the SQL endpoint or cluster")
    catalog: str = Field(None, description="Default catalog to use")
    session_configuration: Optional[Dict[str, str]] = Field(None, description="Optional session configuration dict")

    field_mapping: ClassVar[Dict[str, str]] = {
        "host": "server_hostname",
    }

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        server_hostname: str = connection_kwargs["server_hostname"]
        # Check if the server_hostname starts with https:// or http:// and remove it
        prefixes = ["https://", "http://"]
        for prefix in prefixes:
            if server_hostname.startswith(prefix):
                server_hostname = server_hostname[len(prefix) :]
                break  # Stop looking for prefixes once we find one
        connection_kwargs["server_hostname"] = server_hostname
        return connection_kwargs


class DatabricksTokenAuth(DatabricksSharedConnectionProperties):
    access_token: SecretStr = Field(..., description="Personal access token")
