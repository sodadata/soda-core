from __future__ import annotations

import logging
import random
import struct
import time
from abc import ABC
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Literal, Optional, TypeVar, Union

import pyodbc
from pydantic import Field, SecretStr
from soda_core.__version__ import SODA_CORE_VERSION
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.exceptions import DataSourceConnectionException
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


T = TypeVar("T")


CONTEXT_AUTHENTICATION_DESCRIPTION = "Use context authentication"
USER_DESCRIPTION = "Username for authentication"
DEFAULT_PORT = 1433


class SqlServerConnectionProperties(DataSourceConnectionProperties, ABC):
    host: str = Field(..., description="Host name of the SQL Server instance")
    port: int = Field(DEFAULT_PORT, description="Port number of the SQL Server instance")
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
    connection_parameters: Optional[dict[str, str]] = Field(None, description="Connection parameters")


class SqlServerPasswordAuth(SqlServerConnectionProperties):
    """SQL Server authentication using password"""

    user: str = Field(..., description=USER_DESCRIPTION)
    password: SecretStr = Field(..., description="Password for authentication")
    authentication: Literal["sql"] = "sql"


class SqlServerActiveDirectoryAuthentication(SqlServerConnectionProperties):
    authentication: Literal[
        "activedirectoryinteractive", "activedirectorypassword", "activedirectoryserviceprincipal"
    ] = Field(..., description="Authentication type")


class SqlServerActiveDirectoryInteractiveAuthentication(SqlServerActiveDirectoryAuthentication):
    user: str = Field(..., description=USER_DESCRIPTION)
    authentication: Literal["activedirectoryinteractive"] = "activedirectoryinteractive"


class SqlServerActiveDirectoryPasswordAuthentication(SqlServerActiveDirectoryAuthentication):
    authentication: Literal["activedirectorypassword"] = "activedirectorypassword"
    user: str = Field(..., description=USER_DESCRIPTION)
    password: SecretStr = Field(..., description="Password for authentication")


class SqlServerActiveDirectoryServicePrincipalAuthentication(SqlServerActiveDirectoryAuthentication):
    authentication: Literal["activedirectoryserviceprincipal"] = "activedirectoryserviceprincipal"
    client_id: str = Field(..., description="Client ID for authentication")
    client_secret: SecretStr = Field(..., description="Client secret for authentication")


class SqlServerDataSource(DataSourceBase, ABC):
    type: Literal["sqlserver"] = Field("sqlserver")

    connection_properties: Union[
        SqlServerPasswordAuth,
        SqlServerActiveDirectoryInteractiveAuthentication,
        SqlServerActiveDirectoryPasswordAuthentication,
        SqlServerActiveDirectoryServicePrincipalAuthentication,
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


class SqlServerDataSourceConnection(DataSourceConnection):
    # SQL Server resolves lock conflicts by killing one participant with error 1205
    # (SQLSTATE 40001) and rolling its transaction back; Microsoft's guidance is to
    # rerun the killed transaction. Concurrent DDL and catalog scans in the same
    # database can deadlock on the system base tables, so statements are retried a
    # few times before the error is propagated. The pause is drawn uniformly from
    # an exponentially growing window (full jitter) so that the parties of a killed
    # deadlock don't retry in lockstep and immediately deadlock again.
    DEADLOCK_MAX_ATTEMPTS: int = 3
    DEADLOCK_RETRY_BACKOFF_SECONDS: float = 0.1

    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        # Set before super().__init__(), which auto-opens the connection and
        # populates these from the live server in _create_connection.
        self.server_major_version: Optional[int] = None
        self.engine_edition: Optional[int] = None
        super().__init__(name, connection_properties)

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        execute = super().execute_query
        return self._execute_with_deadlock_retry(lambda: execute(sql, log_query))

    def execute_update(self, sql: str, log_query: bool = True) -> int:
        execute = super().execute_update
        return self._execute_with_deadlock_retry(lambda: execute(sql, log_query))

    @staticmethod
    def _is_deadlock_error(e: pyodbc.Error) -> bool:
        # pyodbc error args are (sqlstate, message); 40001 is the serialization
        # failure SQLSTATE that SQL Server raises for deadlock victims (1205).
        return bool(e.args) and e.args[0] == "40001"

    def _execute_with_deadlock_retry(self, operation: Callable[[], T]) -> T:
        """Assumes ``operation`` is a single-statement unit of work: recovery
        issues a connection-wide rollback, discarding any earlier uncommitted
        statements on this connection (SQL Server runs autocommit=False).
        Only the buffered paths (execute_query/execute_update) are wrapped;
        the callback/iterator paths (execute_query_one_by_one*,
        execute_query_iterate) may have already delivered rows when a deadlock
        strikes mid-fetch, so re-executing them would double-process rows."""
        for attempt in range(1, max(self.DEADLOCK_MAX_ATTEMPTS, 1) + 1):
            try:
                return operation()
            except pyodbc.Error as e:
                if not self._is_deadlock_error(e) or attempt == self.DEADLOCK_MAX_ATTEMPTS:
                    raise
                logger.warning(
                    f"Deadlock victim on '{self.name}' "
                    f"(attempt {attempt}/{self.DEADLOCK_MAX_ATTEMPTS}), retrying: {e}"
                )
                try:
                    self.rollback()
                except Exception as rollback_error:
                    logger.warning(f"Rollback after deadlock on '{self.name}' failed: {rollback_error}")
                backoff_cap = self.DEADLOCK_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                time.sleep(random.uniform(0, backoff_cap))

    # Normalize pyodbc.Row objects so downstream consumers see plain tuples.
    def _format_rows(self, rows: list[tuple]) -> list[tuple]:
        return [self._format_row(row) for row in rows]

    def _format_row(self, row: Any) -> tuple:
        return tuple(row)

    @staticmethod
    def build_connection_string(config: SqlServerConnectionProperties):
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

        if config.connection_max_retries is not None:
            conn_params.append(f"ConnectRetryCount={config.connection_max_retries}")

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

        if config.connection_parameters:
            for key, value in config.connection_parameters.items():
                logger.info("Adding connection parameter: %s=<redacted>", key)
                conn_params.append(f"{key}={value}")

        conn_params.append(f"APP=soda-core-fabric/{SODA_CORE_VERSION}")

        conn_str = ";".join(conn_params)

        return conn_str

    def _get_pyodbc_attrs(self) -> dict[int, bytes] | None:
        return None

    def _create_connection(
        self,
        config: SqlServerConnectionProperties,
    ):
        try:
            self.connection = pyodbc.connect(
                self.build_connection_string(config),
                attrs_before=self._get_pyodbc_attrs(),
                timeout=int(config.login_timeout),
                autocommit=self._get_autocommit_setting(),
            )

            self.connection.add_output_converter(-155, handle_datetimeoffset)
            self.connection.add_output_converter(-150, handle_datetime)
            self._detect_server_info(self.connection)
            return self.connection
        except Exception as e:
            raise DataSourceConnectionException(e) from e

    @staticmethod
    def _parse_server_major_version(dbms_version: Optional[str]) -> Optional[int]:
        """Parse the leading integer of an ODBC SQL_DBMS_VER string, e.g. '15.00.4123' -> 15."""
        if not dbms_version:
            return None
        try:
            return int(str(dbms_version).split(".")[0])
        except (ValueError, IndexError):
            return None

    def _detect_server_info(self, connection) -> None:
        """Detect raw engine facts once per connect; the data source syncs them onto
        the dialect, which derives version-dependent capabilities from them (e.g.
        APPROX_PERCENTILE_DISC needs SQL Server 2022+ or Azure SQL DB/MI).

        The product version comes from the driver's login handshake — no extra
        round-trip; EngineEdition costs one query. Detection is never fatal for an
        otherwise healthy connect: on failure a warning is logged and the fact stays
        None, which capability checks treat as "assume the newest engine".
        """
        try:
            self.server_major_version = self._parse_server_major_version(connection.getinfo(pyodbc.SQL_DBMS_VER))
        except Exception as e:
            logger.warning(f"Could not determine SQL Server product version: {e}")
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT)")
                row = cursor.fetchone()
            self.engine_edition = row[0] if row is not None else None
        except Exception as e:
            logger.warning(f"Could not determine SQL Server engine edition: {e}")

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]

    def _fetch_session_timezone(self) -> tzinfo:
        # Use SYSDATETIMEOFFSET() instead of CURRENT_TIMEZONE_ID() so the same query works
        # across the whole SQL Server family: SQL Server proper, Microsoft Fabric DW, Azure
        # Synapse Dedicated/Serverless. CURRENT_TIMEZONE_ID is unsupported in Fabric and
        # Synapse Dedicated; SYSDATETIMEOFFSET is universally supported.
        # The connection registers ``handle_datetimeoffset`` as a pyodbc output converter for
        # SQL_TYPE -155, so the returned value is a tz-aware ``datetime`` whose tzinfo is the
        # exact offset reported by the server.
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT SYSDATETIMEOFFSET()")
            row = cursor.fetchone()
        if not row or not isinstance(row[0], datetime) or row[0].tzinfo is None:
            return timezone.utc
        offset = row[0].tzinfo.utcoffset(row[0])
        # Normalize zero offset to ``timezone.utc`` (the singleton) so that adapters
        # routed through ``parse_session_timezone`` and SQL Server agree on the UTC
        # representation — adapters that report ``UTC`` / ``Etc/UTC`` resolve to the
        # same ``timezone.utc`` instance, while a raw ``timezone(timedelta(0))`` would
        # be ``==`` but not ``is`` equivalent.
        if offset == timedelta(0):
            return timezone.utc
        return timezone(offset)

    def _get_autocommit_setting(self) -> bool:
        return False  # No need to set autocommit, as it is set to False by default.
