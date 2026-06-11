from __future__ import annotations

import logging
import uuid
from abc import ABC
from datetime import timezone, tzinfo
from pathlib import Path
from typing import Callable, ClassVar, Dict, Literal, Optional, Union

import psycopg
from pydantic import Field, IPvAnyAddress, SecretStr, field_validator
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    parse_session_timezone,
)
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


class PostgresConnectionProperties(DataSourceConnectionProperties, ABC):
    field_mapping: ClassVar[Dict[str, str]] = {
        "database": "dbname",
    }


class PostgresConnectionString(PostgresConnectionProperties):
    connection_string: str = Field(..., description="Complete connection string (alternative to individual parameters)")


class PostgresConnectionPropertiesBase(PostgresConnectionProperties, ABC):
    host: Union[str, IPvAnyAddress] = Field(..., description="Database host (hostname or IP address)")
    port: int = Field(5432, description="Database port (1-65535)", ge=1, le=65535)
    database: str = Field(..., description="Database name", min_length=1, max_length=63)
    user: str = Field(..., description="Database user (1-63 characters)", min_length=1, max_length=63)

    # SSL configuration
    sslmode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = Field(
        "prefer", description="SSL mode for the connection"
    )
    sslcert: Optional[str] = Field(None, description="Path to SSL client certificate")
    sslkey: Optional[str] = Field(None, description="Path to SSL client key")
    sslrootcert: Optional[str] = Field(None, description="Path to SSL root certificate")

    # Connection options
    connection_timeout: Optional[int] = Field(None, description="Connection timeout in seconds")


class PostgresConnectionPassword(PostgresConnectionPropertiesBase):
    password: SecretStr = Field(..., description="Database password")


class PostgresConnectionPasswordFile(PostgresConnectionPropertiesBase):
    password_file: Path = Field(..., description="Path to file containing database password")


class PostgresDataSource(DataSourceBase, ABC):
    type: Literal["postgres"] = Field("postgres")
    connection_properties: PostgresConnectionProperties = Field(
        ..., alias="connection", description="Data source connection details"
    )

    @field_validator("connection_properties", mode="before")
    def infer_connection_type(cls, value):
        if "password" in value:
            return PostgresConnectionPassword(**value)
        elif "password_file" in value:
            return PostgresConnectionPasswordFile(**value)
        raise ValueError("Unknown connection structure")


class PostgresDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: PostgresConnectionProperties,
    ):
        if isinstance(config, PostgresConnectionPasswordFile):
            with open(config.password_file, "r") as f:
                config_dict = config.model_dump(exclude="password_file")
                config_dict["password"] = f.read().strip()
                config = PostgresConnectionPassword(**config_dict)
        connection_kwargs = config.to_connection_kwargs()
        connection = psycopg.connect(**connection_kwargs)
        return connection

    def _fetch_session_timezone(self) -> tzinfo:
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW timezone")
            row = cursor.fetchone()
        if not row:
            return timezone.utc
        return parse_session_timezone(row[0])

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        try:
            return super().execute_query(sql, log_query=log_query)
        except psycopg.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL query failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_update(self, sql: str, log_query: bool = True) -> int:
        try:
            return super().execute_update(sql, log_query=log_query)
        except psycopg.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL update failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_query_one_by_one(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        # Pre-memory-work behavior, restored: the buffered base
        # implementation with postgres's rollback-on-error wrapper. Callers
        # that stream large result sets use
        # execute_query_one_by_one_memory_optimized instead.
        try:
            return super().execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)
        except psycopg.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL query one-by-one failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_query_one_by_one_memory_optimized(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        """Postgres override: server-side (named) cursor that streams ONE row
        at a time from the backend instead of buffering the whole result.

        The base ``DataSourceConnection.execute_query_one_by_one`` uses
        ``self.connection.cursor()`` without a name. For psycopg3 that's a
        client-side cursor: ``execute(sql)`` materialises the ENTIRE result
        set in libpq's C-side buffer before the first ``fetchone()`` returns.
        On large result sets this drove peak RSS to ~5× the source bytes
        and OOM-killed K8s pods (see memory_management/MEMORY_ANALYSIS.md).

        Strategy: open a named (server-side) cursor and `fetchone()` per
        row. Memory footprint is bounded by the size of the single largest
        row plus libpq's per-fetch frame, regardless of result-set size.
        Throughput is intentionally traded for memory predictability.

        Caveats:
          * Server-side cursors require an open transaction. If the
            connection is in autocommit mode, fall back to the base impl
            (the buffered behaviour). Default psycopg3 connections aren't
            autocommit, so this is normally fine.
          * Server-side cursors hold a snapshot on the backend — long
            transactions can interact with vacuum / replication slots.
            For Soda's typical scan duration (seconds) this is irrelevant.
          * ``withhold=True`` keeps the cursor alive past intermediate
            ``connection.commit()`` calls — needed because the
            failed-rows DWH pump path commits at the end of every
            ``_optimized_insert`` batch on the SAME connection. Without
            this, a flush triggered mid-iteration kills the source cursor
            and the next ``fetchone()`` raises ``cursor "soda_stream_…"
            does not exist``. Postgres materialises the cursor's
            remaining unfetched rows server-side at commit time; the
            client-side memory footprint stays bounded.
        """
        if getattr(self.connection, "autocommit", False):
            # Server-side cursors can't be created in autocommit mode — fall
            # back to the buffered base implementation rather than fail.
            # self.execute_query_one_by_one is the restored postgres wrapper,
            # so the rollback-on-error semantics are preserved.
            logger.debug(
                "execute_query_one_by_one_memory_optimized: connection is in autocommit mode, "
                "falling back to buffered client-side cursor"
            )
            return self.execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)

        cursor_name = f"soda_stream_{uuid.uuid4().hex[:12]}"
        if log_query:
            logger.debug(
                f"SQL query one-by-one (server-side cursor {cursor_name}, "
                f"fetchone per row — minimise memory):\n{sql}"
            )

        try:
            with self.connection.cursor(name=cursor_name, withhold=True) as cursor:
                cursor.itersize = 1
                cursor.execute(sql)
                description: tuple[tuple] = cursor.description
                rows_processed: int = 0
                while True:
                    if row_limit is not None and rows_processed >= row_limit:
                        break
                    row = cursor.fetchone()
                    if row is None:
                        break
                    row_callback(row, description)
                    rows_processed += 1
            return description
        except psycopg.errors.Error as e:
            logger.warning(f"SQL query one-by-one failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

