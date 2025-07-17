from __future__ import annotations

import logging
from abc import ABC
from pathlib import Path
from typing import Dict, Literal, Optional

from pydantic import Field, SecretStr, field_validator
from snowflake import connector
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


class SnowflakeConnectionProperties(DataSourceConnectionProperties, ABC):
    ...


class SnowflakeSharedConnectionProperties(SnowflakeConnectionProperties, ABC):
    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Username for authentication")
    warehouse: Optional[str] = Field(None, description="Name of the warehouse to use")
    database: Optional[str] = Field(None, description="Name of the database to use")
    role: Optional[str] = Field(None, description="Role to assume after connecting")
    session_parameters: Optional[Dict[str, str]] = Field(None, description="Session-level parameters")
    host: Optional[str] = Field(None, description="Host name of the Snowflake account")


class SnowflakePasswordAuth(SnowflakeSharedConnectionProperties):
    password: SecretStr = Field(..., description="User password")


class SnowflakeKeyPairAuth(SnowflakeSharedConnectionProperties):
    private_key_path: Path = Field(..., description="Path to private key file")
    private_key_passphrase: Optional[SecretStr] = Field(None, description="Passphrase if private key is encrypted")

    def to_connection_kwargs(self) -> dict:
        # TODO
        pass


class SnowflakeJWTAuth(SnowflakeSharedConnectionProperties):
    jwt_token: SecretStr = Field(..., description="JWT token for authentication")


class SnowflakeOAuthAuth(SnowflakeSharedConnectionProperties):
    token: SecretStr = Field(..., description="OAuth access token")


class SnowflakeSSOAuth(SnowflakeSharedConnectionProperties):
    authenticator: Literal["externalbrowser"] = Field("externalbrowser", description="Use external browser SSO login")


class SnowflakeDataSource(DataSourceBase, ABC):
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


class SnowflakeDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: SnowflakeConnectionProperties,
    ):
        return connector.connect(
            application="Soda",
            **config.to_connection_kwargs(),
        )
