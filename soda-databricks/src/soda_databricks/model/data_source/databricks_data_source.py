import abc
from typing import Literal

from pydantic import Field, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksAzureServicePrincipal,
    DatabricksConnectionProperties,
    DatabricksOAuthM2M,
    DatabricksTokenAuth,
)

# Explicit auth_type discriminator values (Trino/Snowflake/BigQuery style).
# NOTE (provisional): these literals must match what the Backend emits in the generated
# YAML — confirm with BE before merge. See implementation-plan.md.
AUTH_TYPE_TOKEN = "personal-access-token"
AUTH_TYPE_OAUTH_M2M = "databricks-oauth-m2m"
AUTH_TYPE_AZURE_SP = "azure-service-principal"

_AUTH_TYPE_TO_PROPERTIES = {
    AUTH_TYPE_TOKEN: DatabricksTokenAuth,
    AUTH_TYPE_OAUTH_M2M: DatabricksOAuthM2M,
    AUTH_TYPE_AZURE_SP: DatabricksAzureServicePrincipal,
}


class DatabricksDataSource(DataSourceBase, abc.ABC):
    type: Literal["databricks"] = Field("databricks")
    connection_properties: DatabricksConnectionProperties = Field(
        ..., alias="connection", description="Databricks connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        # Already a resolved properties object (e.g. constructed in code) — pass through.
        if isinstance(value, DatabricksConnectionProperties):
            return value
        if not isinstance(value, dict):
            raise ValueError("Could not infer Databricks connection type from input")

        # Copy so the discriminator can be stripped without mutating caller input; also
        # prevents auth_type leaking into sql.connect kwargs (base model has extra='allow').
        value = dict(value)
        auth_type = value.pop("auth_type", None)

        if auth_type is not None:
            properties_class = _AUTH_TYPE_TO_PROPERTIES.get(auth_type)
            if properties_class is None:
                supported = ", ".join(sorted(_AUTH_TYPE_TO_PROPERTIES))
                raise ValueError(f"Unknown Databricks auth_type '{auth_type}'. Supported: {supported}")
            return properties_class(**value)

        # Backward compatibility: no explicit auth_type → infer PAT from field presence.
        if "access_token" in value:
            return DatabricksTokenAuth(**value)
        raise ValueError(
            "Could not infer Databricks connection type: provide 'auth_type' "
            f"(one of {', '.join(sorted(_AUTH_TYPE_TO_PROPERTIES))}) or 'access_token'."
        )
