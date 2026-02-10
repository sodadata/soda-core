from __future__ import annotations

import logging
import requests
from abc import ABC
from typing import Literal, Optional, Union

import trino
from pydantic import BaseModel, Field, IPvAnyAddress
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class TrinoConnectionProperties(DataSourceConnectionProperties):
    host: Union[str, IPvAnyAddress] = Field(..., description="Database host (hostname or IP address)")
    catalog: str = Field(..., description="Database catalog")
    port: int = Field(5432, description="Database port (1-65535)", ge=1, le=65535)
    http_scheme: Literal["https", "http"] = Field("https", description="HTTP scheme")
    http_headers: Optional[dict[str, str]] = Field(None, description="HTTP headers")
    source: str = Field("soda-core", description="Trino-internal label for this connection")
    client_tags: Optional[list[str]] = Field(None, description="Trino-internal tags as list of strings.")
    verify: Optional[bool] = Field(True, description="Verify SSL certificate")


class TrinoUserPasswordConnectionProperties(TrinoConnectionProperties):
    # Default if authType not specified
    auth_type: Optional[Literal["BasicAuthentication"]] = Field(
        "BasicAuthentication", description="Authentication type"
    )
    user: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")


class TrinoJWTConnectionProperties(TrinoConnectionProperties):
    auth_type: Literal["JWTAuthentication"] = Field(description="Authentication type")
    access_token: str = Field(..., description="JWT access token")
    user: Optional[str] = Field(None, description="Database username")


class TrinoOauthPayload(BaseModel):
    token_url: str = Field(..., description="Token URL")
    client_id: str = Field(..., description="Client ID")
    client_secret: str = Field(..., description="Client secret")
    scope: Optional[str] = Field(None, description="Scope")
    grant_type: Optional[str] = Field("client_credentials", description="Grant type")


class TrinoOauthConnectionProperties(TrinoConnectionProperties):
    auth_type: Literal["OAuth2ClientCredentialsAuthentication"] = Field(description="Authentication type")
    oauth: TrinoOauthPayload = Field(..., description="OAuth configuration")
    user: Optional[str] = Field(None, description="Database username")


class TrinoNoAuthenticationConnectionProperties(TrinoConnectionProperties):
    auth_type: Literal["NoAuthentication"] = Field(description="Authentication type")


class TrinoDataSource(DataSourceBase, ABC):
    type: Literal["trino"] = Field("trino")

    connection_properties: Union[
        TrinoUserPasswordConnectionProperties,
        TrinoJWTConnectionProperties,
        TrinoOauthConnectionProperties,
        TrinoNoAuthenticationConnectionProperties,
    ] = Field(..., alias="connection", description="Trino connection configuration")


class TrinoDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: TrinoConnectionProperties,
    ):
        if isinstance(config, TrinoUserPasswordConnectionProperties):
            self.auth = trino.auth.BasicAuthentication(config.user, config.password)
        elif isinstance(config, TrinoJWTConnectionProperties):
            self.auth = trino.auth.JWTAuthentication(token=config.access_token)
        elif isinstance(config, TrinoOauthConnectionProperties):
            # Use OAuth to get a JWT access token
            # Note, this is a JWTAuthentication flow, not to be confused with OAuth2Authentication which launches a web browser
            token = self._exchange_oauth_for_access_token(config.oauth)
            self.auth = trino.auth.JWTAuthentication(token=token)
        elif isinstance(config, TrinoNoAuthenticationConnectionProperties):
            self.auth = None
        else:
            raise ValueError(f"Unrecognized Trino authentication type: {config.authType}")

        connect_kwargs = {
            "host": config.host,
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
        client_secret = oauth.client_secret
        scope = oauth.scope
        grant_type = oauth.grant_type

        # OAuth credentials
        payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": grant_type}
        if scope:
            payload["scope"] = scope
        response = requests.post(token_url, data=payload)
        if response.status_code != 200:
            raise ValueError(f"OAuth request failed: {response.status_code} {response.text}")
        
        response_json = response.json()
        expires_in = response_json.get("expires_in", 0)
        scope = response_json.get("scope", "")
        access_token = response_json["access_token"]
        if access_token:
            logger.info(
                f"Obtained OAuth access token, expires in '{expires_in}' seconds, granted scopes: '{scope}'"
            )
            return access_token
        else:
            raise ValueError(
                f"OAuth request did not return an access token: {response.status_code} {response.text}"
            )
        