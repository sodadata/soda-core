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


class DatabricksOAuthM2M(DatabricksSharedConnectionProperties):
    """Databricks-managed OAuth machine-to-machine (service principal) auth.

    The token endpoint is derived from ``host`` by the Databricks SDK, so no
    ``token_url`` is required. The connection layer builds a ``credentials_provider``
    from these fields via ``oauth_service_principal`` — they must NOT be emitted as
    plain ``sql.connect`` kwargs, hence the ``to_connection_kwargs`` override below.
    """

    client_id: str = Field(..., description="Databricks OAuth service-principal client ID")
    client_secret: SecretStr = Field(..., description="Databricks OAuth service-principal client secret")

    # Consumed by the connection layer to build the credentials_provider, never passed to sql.connect.
    _credential_fields: ClassVar[tuple] = ("client_id", "client_secret")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        for field_name in self._credential_fields:
            connection_kwargs.pop(field_name, None)
        return connection_kwargs


class DatabricksAzureServicePrincipal(DatabricksSharedConnectionProperties):
    """Microsoft Entra ID (Azure AD) service-principal auth for Azure Databricks.

    Distinct from Databricks-managed OAuth M2M: the SP lives in an Entra app
    registration and the token comes from ``login.microsoftonline.com``, so a tenant
    ID is required. The connection layer builds a ``credentials_provider`` from these
    fields; they must NOT be emitted as plain ``sql.connect`` kwargs.
    """

    azure_client_id: str = Field(..., description="Entra ID (Azure AD) service-principal application/client ID")
    azure_client_secret: SecretStr = Field(..., description="Entra ID (Azure AD) service-principal client secret")
    azure_tenant_id: str = Field(..., description="Entra ID (Azure AD) directory/tenant ID")

    _credential_fields: ClassVar[tuple] = ("azure_client_id", "azure_client_secret", "azure_tenant_id")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        for field_name in self._credential_fields:
            connection_kwargs.pop(field_name, None)
        return connection_kwargs
