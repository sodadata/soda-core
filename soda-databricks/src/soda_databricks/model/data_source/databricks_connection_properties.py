from abc import ABC
from typing import Annotated, Any, ClassVar, Dict, Literal, Optional, Union

from pydantic import Discriminator, Field, SecretStr, Tag
from soda_core.model.data_source.data_source_connection_properties import DataSourceConnectionProperties

# Explicit auth_type discriminator values (Trino/Snowflake/BigQuery style). These literals
# are the cross-repo contract: they match soda-library#759 and the Cloud connection form.
AUTH_TYPE_TOKEN = "personal-access-token"
AUTH_TYPE_OAUTH_M2M = "databricks-oauth-m2m"
AUTH_TYPE_AZURE_SP = "azure-service-principal"


class DatabricksConnectionProperties(DataSourceConnectionProperties, ABC):
    ...


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
        # The auth mode discriminator is ours, never a sql.connect kwarg (the connector has its
        # own, differently-valued `auth_type` argument).
        connection_kwargs.pop("auth_type", None)
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
    # Backward compatibility: PAT is the mode when auth_type is omitted.
    auth_type: Optional[Literal["personal-access-token"]] = Field(None, description="Auth mode discriminator")
    # min_length: an empty token (e.g. an unresolved variable reference) must be rejected here —
    # the connector treats a falsy access_token as "no auth" and falls back to an interactive
    # browser login (see DatabricksDataSourceConnection._create_connection).
    access_token: SecretStr = Field(..., min_length=1, description="Personal access token")


class DatabricksOAuthM2M(DatabricksSharedConnectionProperties):
    """Databricks-managed OAuth machine-to-machine (service principal) auth.

    The token endpoint is derived from ``host`` by the Databricks SDK, so no
    ``token_url`` is required. The connection layer builds a ``credentials_provider``
    from these fields via ``oauth_service_principal`` — they must NOT be emitted as
    plain ``sql.connect`` kwargs, hence the ``to_connection_kwargs`` override below.
    """

    auth_type: Literal["databricks-oauth-m2m"] = Field(..., description="Auth mode discriminator")
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

    auth_type: Literal["azure-service-principal"] = Field(..., description="Auth mode discriminator")
    azure_client_id: str = Field(..., description="Entra ID (Azure AD) service-principal application/client ID")
    azure_client_secret: SecretStr = Field(..., description="Entra ID (Azure AD) service-principal client secret")
    azure_tenant_id: str = Field(..., description="Entra ID (Azure AD) directory/tenant ID")

    _credential_fields: ClassVar[tuple] = ("azure_client_id", "azure_client_secret", "azure_tenant_id")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        for field_name in self._credential_fields:
            connection_kwargs.pop(field_name, None)
        return connection_kwargs


def _discriminate_auth_type(value: Any) -> Optional[str]:
    """Pick the auth mode from the raw ``connection`` mapping (or an already-built model).

    An omitted ``auth_type`` means PAT, the only mode that existed before the discriminator.
    Returning an unknown tag makes pydantic report it against the expected tags.
    """
    if isinstance(value, dict):
        auth_type = value.get("auth_type")
    else:
        auth_type = getattr(value, "auth_type", None)
    return auth_type or AUTH_TYPE_TOKEN


DatabricksConnection = Annotated[
    Union[
        Annotated[DatabricksTokenAuth, Tag(AUTH_TYPE_TOKEN)],
        Annotated[DatabricksOAuthM2M, Tag(AUTH_TYPE_OAUTH_M2M)],
        Annotated[DatabricksAzureServicePrincipal, Tag(AUTH_TYPE_AZURE_SP)],
    ],
    Discriminator(_discriminate_auth_type),
]
"""The ``connection`` block of a Databricks data source: one of the auth modes, selected by
``auth_type`` (PAT when omitted). Validation errors are scoped to the selected mode only."""
