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

# Explicit auth_type discriminator values (Trino/Snowflake/BigQuery style). These literals
# are the cross-repo contract: they match soda-library#759 and the Cloud connection form.
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
            if properties_class is DatabricksTokenAuth:
                cls._require_non_empty_access_token(value)
            return properties_class(**value)

        # Backward compatibility: no explicit auth_type → infer PAT from field presence.
        if "access_token" in value:
            cls._require_non_empty_access_token(value)
            return DatabricksTokenAuth(**value)
        raise ValueError(
            "Could not infer Databricks connection type: provide 'auth_type' "
            f"(one of {', '.join(sorted(_AUTH_TYPE_TO_PROPERTIES))}) or 'access_token'."
        )

    @staticmethod
    def _require_non_empty_access_token(value: dict) -> None:
        # An unresolved ${env.VAR} reference typically arrives as None or an empty string. An
        # empty token must not reach the connector: with no credentials it falls back to an
        # interactive browser login (see DatabricksDataSourceConnection._create_connection).
        access_token = value.get("access_token")
        if access_token is None or access_token == "":
            raise ValueError(
                "Databricks 'access_token' is missing or empty. If it is provided through ${env.VAR}, "
                "make sure VAR is set where the contract verification runs."
            )
