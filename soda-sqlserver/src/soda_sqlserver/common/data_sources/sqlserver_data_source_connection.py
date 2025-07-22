from __future__ import annotations

import logging
import struct
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional, Union

import pyodbc
from pydantic import Field, SecretStr
from soda_core.__version__ import SODA_CORE_VERSION
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.exceptions import DataSourceConnectionException
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


CONTEXT_AUTHENTICATION_DESCRIPTION = "Use context authentication"


class SQLServerConnectionProperties(DataSourceConnectionProperties, ABC):
    host: str = Field(..., description="Host name of the SQL Server instance")
    port: int = Field(..., description="Port number of the SQL Server instance")
    user: str = Field(..., description="Username for authentication")
    database: str = Field(..., description="Name of the database to use")

    # Optional fields
    driver: Optional[str] = Field(
        "ODBC Driver 18 for SQL Server", description="Driver name for the SQL Server instance"
    )
    trust_server_certificate: Optional[bool] = Field(False, description="Whether to trust the server certificate")
    trusted_connection: Optional[bool] = Field(False, description="Whether to use trusted connection")
    encrypt: Optional[bool] = Field(False, description="Whether to encrypt the connection")
    connection_max_retries: Optional[int] = Field(0, description="Maximum number of connection retries")
    enable_tracing: Optional[bool] = Field(False, description="Whether to enable tracing")
    login_timeout: Optional[int] = Field(0, description="Login timeout")
    scope: Optional[str] = Field(None, description="Scope for the connection")


class SQLServerPasswordAuth(SQLServerConnectionProperties):
    """SQL Server authentication using password"""

    password: SecretStr = Field(..., description="Password for authentication")
    authentication: Literal["sql"] = "sql"


class SQLServerActiveDirectoryAuthentication(SQLServerConnectionProperties):
    authentication: Literal[
        "activedirectoryinteractive", "activedirectorypassword", "activedirectoryserviceprincipal"
    ] = Field(..., description="Authentication type")


class SQLServerActiveDirectoryInteractiveAuthentication(SQLServerActiveDirectoryAuthentication):
    # Uses the "user" field from the parent class
    authentication: Literal["activedirectoryinteractive"] = "activedirectoryinteractive"


class SQLServerActiveDirectoryPasswordAuthentication(SQLServerActiveDirectoryAuthentication):
    authentication: Literal["activedirectorypassword"] = "activedirectorypassword"
    # Uses the "user" field from the parent class
    password: SecretStr = Field(..., description="Password for authentication")


class SQLServerActiveDirectoryServicePrincipalAuthentication(SQLServerActiveDirectoryAuthentication):
    authentication: Literal["activedirectoryserviceprincipal"] = "activedirectoryserviceprincipal"
    client_id: str = Field(..., description="Client ID for authentication")
    client_secret: SecretStr = Field(..., description="Client secret for authentication")


class SQLServerDataSource(DataSourceBase, ABC):
    type: Literal["sqlserver"] = Field("sqlserver")

    connection_properties: Union[
        SQLServerPasswordAuth,
        SQLServerActiveDirectoryInteractiveAuthentication,
        SQLServerActiveDirectoryPasswordAuthentication,
        SQLServerActiveDirectoryServicePrincipalAuthentication,
    ] = Field(..., alias="connection", description="SQL Server connection configuration")


def handle_datetime(dto_value):
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000)


def handle_datetimeoffset(dto_value):
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime(
        tup[0],
        tup[1],
        tup[2],
        tup[3],
        tup[4],
        tup[5],
        tup[6] // 1000,
        timezone(timedelta(hours=tup[7], minutes=tup[8])),
    )


class SQLServerDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def build_connection_string(self, config: SQLServerConnectionProperties):
        conn_params = []

        conn_params.append(f"DRIVER={{{config.driver}}}")
        conn_params.append(f"DATABASE={config.database}")

        if "\\" in config.host:
            # If there is a backslash in the host name, the host is a
            # SQL Server named instance. In this case then port number has to be omitted.
            conn_params.append(f"SERVER={config.host}")
        else:
            conn_params.append(f"SERVER={config.host},{int(config.port)}")

        if config.trusted_connection:
            conn_params.append("Trusted_Connection=YES")

        if config.trust_server_certificate:
            conn_params.append("TrustServerCertificate=YES")

        if config.encrypt:
            conn_params.append("Encrypt=YES")

        if int(config.connection_max_retries) > 0:
            conn_params.append(f"ConnectRetryCount={int(self.connection_max_retries)}")

        if config.enable_tracing:
            conn_params.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")

        if config.authentication.lower() == "sql":
            conn_params.append(f"UID={{{config.user}}}")
            conn_params.append(f"PWD={{{config.password.get_secret_value()}}}")
        elif config.authentication.lower() == "activedirectoryinteractive":
            conn_params.append("Authentication=ActiveDirectoryInteractive")
            conn_params.append(f"UID={{{config.user}}}")
        elif config.authentication.lower() == "activedirectorypassword":
            conn_params.append("Authentication=ActiveDirectoryPassword")
            conn_params.append(f"UID={{{config.user}}}")
            conn_params.append(f"PWD={{{config.password.get_secret_value()}}}")
        elif config.authentication.lower() == "activedirectoryserviceprincipal":
            conn_params.append("Authentication=ActiveDirectoryServicePrincipal")
            conn_params.append(f"UID={{{config.client_id}}}")
            conn_params.append(f"PWD={{{config.client_secret.get_secret_value()}}}")
        elif "activedirectory" in config.authentication.lower():
            conn_params.append(f"Authentication={config.authentication}")

        conn_params.append(f"APP=soda-core-fabric/{SODA_CORE_VERSION}")

        conn_str = ";".join(conn_params)

        return conn_str

    def _get_pyodbc_attrs(self) -> dict[int, bytes] | None:
        return None

    def _create_connection(
        self,
        config: SQLServerConnectionProperties,
    ):
        try:
            self.connection = pyodbc.connect(
                self.build_connection_string(config),
                attrs_before=self._get_pyodbc_attrs(),
                timeout=int(config.login_timeout),
            )

            self.connection.add_output_converter(-155, handle_datetimeoffset)
            self.connection.add_output_converter(-150, handle_datetime)
            return self.connection
        except Exception as e:
            raise DataSourceConnectionException(e) from e

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]
