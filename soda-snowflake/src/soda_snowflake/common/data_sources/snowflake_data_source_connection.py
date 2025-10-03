from __future__ import annotations

import logging
from abc import ABC
from pathlib import Path
from typing import Dict, Literal, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
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
    private_key: SecretStr = Field(..., description="Private key for authentication")
    private_key_passphrase: Optional[SecretStr] = Field(None, description="Passphrase if private key is encrypted")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        connection_kwargs["private_key"] = self._decrypt(self.private_key, self.private_key_passphrase)
        return connection_kwargs

    def _decrypt(self, private_key: str, private_key_passphrase: Optional[SecretStr]) -> bytes:
        private_key_bytes = private_key.get_secret_value().encode()
        private_key_passphrase_bytes = (
            private_key_passphrase.get_secret_value().encode() if private_key_passphrase else None
        )

        p_key = serialization.load_pem_private_key(
            private_key_bytes, password=private_key_passphrase_bytes, backend=default_backend()
        )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


class SnowflakeKeyPairFileAuth(SnowflakeSharedConnectionProperties):
    private_key_path: Path = Field(..., description="Path to private key file")
    private_key_passphrase: Optional[SecretStr] = Field(None, description="Passphrase if private key is encrypted")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        connection_kwargs.update(
            {
                "private_key_file": self.private_key_path,
                "private_key_file_pwd": self.private_key_passphrase.get_secret_value(),
            }
        )
        return connection_kwargs


class SnowflakeOAuthAuth(SnowflakeSharedConnectionProperties):
    authenticator: Literal["oauth"] = Field(..., description="Use OAuth access token")
    token: SecretStr = Field(..., description="OAuth access token")


class SnowflakeClientCredentialsOAuthAuth(SnowflakeSharedConnectionProperties):
    authenticator: Literal["OAUTH_CLIENT_CREDENTIALS"] = Field(
        ..., description="Authenticator to use for OAuth Client Credentials Flow."
    )
    oauth_client_id: SecretStr = Field(..., description="Client ID for OAuth Client Credentials Flow.")
    oauth_client_secret: SecretStr = Field(..., description="Client secret for OAuth Client Credentials Flow.")
    oauth_token_request_url: SecretStr = Field(..., description="Token request URL for OAuth Client Credentials Flow.")
    oauth_scope: Optional[SecretStr] = Field(None, description="Scope for OAuth Client Credentials Flow if required.")


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
        elif "private_key" in value:
            return SnowflakeKeyPairAuth(**value)
        elif "private_key_path" in value:
            return SnowflakeKeyPairFileAuth(**value)
        elif "token" in value:
            return SnowflakeOAuthAuth(**value)
        elif value.get("authenticator") == "externalbrowser":
            return SnowflakeSSOAuth(**value)
        elif value.get("authenticator") == "OAUTH_CLIENT_CREDENTIALS":
            return SnowflakeClientCredentialsOAuthAuth(**value)
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
