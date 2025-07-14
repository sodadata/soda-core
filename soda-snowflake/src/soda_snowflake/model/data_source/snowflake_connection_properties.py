from abc import ABC
from pathlib import Path
from typing import Dict, Literal, Optional

from pydantic import Field, SecretStr
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


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
