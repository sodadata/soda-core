from __future__ import annotations

import logging
from decimal import Decimal
from typing import Literal, Optional, Union

import requests
import trino
from pydantic import BaseModel, Field, IPvAnyAddress, SecretStr, field_validator
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


class TrinoConnectionProperties(DataSourceConnectionProperties):
    host: Union[str, IPvAnyAddress] = Field(..., description="Database host (hostname or IP address)")
    catalog: str = Field(..., description="Database catalog")
    port: int = Field(443, description="Database port (1-65535)", ge=1, le=65535)
    http_scheme: Literal["https", "http"] = Field("https", description="HTTP scheme")
    http_headers: Optional[dict[str, str]] = Field(None, description="HTTP headers")
    # v3 default source was 'trino-python-client', in v4 we changed to 'soda-core'
    # this is a label that goes in Trino logs
    source: str = Field("soda-core", description="Trino-internal label for this connection")
    client_tags: Optional[list[str]] = Field(None, description="Trino-internal tags as list of strings.")
    verify: Optional[bool] = Field(True, description="Verify SSL certificate")


class TrinoUserPasswordConnectionProperties(TrinoConnectionProperties):
    # Default if auth_type not specified
    auth_type: Optional[Literal["BasicAuthentication"]] = Field(
        "BasicAuthentication", description="Authentication type"
    )
    user: str = Field(..., description="Database username")
    password: SecretStr = Field(..., description="Database password")


class TrinoJWTConnectionProperties(TrinoConnectionProperties):
    auth_type: Literal["JWTAuthentication"] = Field(description="Authentication type")
    access_token: SecretStr = Field(..., description="JWT access token")
    user: Optional[str] = Field(None, description="Database username")


class TrinoOauthPayload(BaseModel):
    token_url: str = Field(..., description="Token URL")
    client_id: str = Field(..., description="Client ID")
    client_secret: SecretStr = Field(..., description="Client secret")
    scope: Optional[str] = Field(None, description="Scope")
    grant_type: Optional[str] = Field("client_credentials", description="Grant type")


class TrinoOauthConnectionProperties(TrinoConnectionProperties):
    auth_type: Literal["OAuth2ClientCredentialsAuthentication"] = Field(description="Authentication type")
    oauth: TrinoOauthPayload = Field(..., description="OAuth configuration")
    user: Optional[str] = Field(None, description="Database username")


class TrinoNoAuthenticationConnectionProperties(TrinoConnectionProperties):
    auth_type: Literal["NoAuthentication"] = Field(description="Authentication type")


class TrinoDataSource(DataSourceBase):
    type: Literal["trino"] = Field("trino")

    connection_properties: TrinoConnectionProperties = Field(
        ..., alias="connection", description="Trino connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if isinstance(value, TrinoConnectionProperties):
            return value

        auth_type = value.get("auth_type")

        if auth_type is None or auth_type == "BasicAuthentication":
            return TrinoUserPasswordConnectionProperties(**value)
        elif auth_type == "JWTAuthentication":
            return TrinoJWTConnectionProperties(**value)
        elif auth_type == "OAuth2ClientCredentialsAuthentication":
            return TrinoOauthConnectionProperties(**value)
        elif auth_type == "NoAuthentication":
            return TrinoNoAuthenticationConnectionProperties(**value)

        raise ValueError(
            f"Unknown Trino auth_type: '{auth_type}'. "
            f"Supported values: 'BasicAuthentication', 'JWTAuthentication', "
            f"'OAuth2ClientCredentialsAuthentication', 'NoAuthentication'"
        )


class TrinoDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def format_rows(self, rows: list[tuple]) -> list[tuple]:
        return [tuple(float(v) if isinstance(v, Decimal) else v for v in row) for row in rows]

    def _create_connection(
        self,
        config: TrinoConnectionProperties,
    ):
        if isinstance(config, TrinoUserPasswordConnectionProperties):
            self.auth = trino.auth.BasicAuthentication(config.user, config.password.get_secret_value())
        elif isinstance(config, TrinoJWTConnectionProperties):
            self.auth = trino.auth.JWTAuthentication(token=config.access_token.get_secret_value())
        elif isinstance(config, TrinoOauthConnectionProperties):
            # Use OAuth to get a JWT access token
            # Note, this is a JWTAuthentication flow, not to be confused with OAuth2Authentication which launches a web browser
            token = self._exchange_oauth_for_access_token(config.oauth)
            self.auth = trino.auth.JWTAuthentication(token=token)
        elif isinstance(config, TrinoNoAuthenticationConnectionProperties):
            self.auth = None
        else:
            raise ValueError(f"Unrecognized Trino authentication type: {config.auth_type}")

        connect_kwargs = {
            "host": str(config.host),
            "port": config.port,
            "catalog": config.catalog,
            "http_scheme": config.http_scheme,
            "auth": self.auth,
            "http_headers": config.http_headers,
            "source": config.source,
            "client_tags": config.client_tags,
            "verify": config.verify,
        }

        if getattr(config, "user", None):
            connect_kwargs["user"] = config.user
        return trino.dbapi.connect(**connect_kwargs)

    def _exchange_oauth_for_access_token(self, oauth: TrinoOauthPayload) -> str:
        if not oauth:
            raise ValueError("OAuth configuration is required for OAuth2ClientCredentialsAuthentication")

        token_url = oauth.token_url
        client_id = oauth.client_id
        client_secret = oauth.client_secret.get_secret_value()
        scope = oauth.scope
        grant_type = oauth.grant_type

        payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": grant_type}
        if scope:
            payload["scope"] = scope
        response = requests.post(token_url, data=payload)
        if response.status_code != 200:
            raise ValueError(f"OAuth request failed: {response.status_code} {response.text}")

        response_json = response.json()
        expires_in = response_json.get("expires_in", 0)
        scope = response_json.get("scope", "")
        access_token = response_json.get("access_token")
        if access_token:
            logger.info(f"Obtained OAuth access token, expires in '{expires_in}' seconds, granted scopes: '{scope}'")
            return access_token
        else:
            raise ValueError(f"OAuth request did not return an access token: {response.status_code} {response.text}")
