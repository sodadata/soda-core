import abc
from typing import Literal

from pydantic import Field, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_snowflake.model.data_source.snowflake_connection_properties import (
    SnowflakeConnectionProperties,
    SnowflakePasswordAuth,
    SnowflakeKeyPairAuth,
    SnowflakeJWTAuth,
    SnowflakeOAuthAuth,
    SnowflakeSSOAuth,
)


class SnowflakeDataSource(DataSourceBase, abc.ABC):
    type: Literal["snowflake"] = Field("snowflake")
    connection_properties: SnowflakeConnectionProperties = Field(
        ..., alias="connection", description="Snowflake connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if "password" in value:
            return SnowflakePasswordAuth(**value)
        elif "private_key_path" in value:
            return SnowflakeKeyPairAuth(**value)
        elif "jwt_token" in value:
            return SnowflakeJWTAuth(**value)
        elif "token" in value:
            return SnowflakeOAuthAuth(**value)
        elif value.get("authenticator") == "externalbrowser":
            return SnowflakeSSOAuth(**value)
        raise ValueError("Could not infer Snowflake connection type from input")
