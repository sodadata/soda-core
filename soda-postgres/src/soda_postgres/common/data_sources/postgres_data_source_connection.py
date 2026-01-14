from __future__ import annotations

import logging
from abc import ABC
from pathlib import Path
from typing import Callable, Literal, Optional, Union

import psycopg2
from pydantic import Field, IPvAnyAddress, SecretStr, field_validator
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


class PostgresConnectionProperties(DataSourceConnectionProperties, ABC):
    ...


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
        return psycopg2.connect(**config.to_connection_kwargs())

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        try:
            return super().execute_query(sql, log_query=log_query)
        except psycopg2.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL query failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_update(self, sql: str, log_query: bool = True) -> UpdateResult:
        try:
            return super().execute_update(sql, log_query=log_query)
        except psycopg2.errors.Error as e:  # Catch the error and roll back the transaction
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
        try:
            return super().execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)
        except psycopg2.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL query one-by-one failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e
