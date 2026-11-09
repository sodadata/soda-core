from __future__ import annotations

from abc import ABC
from typing import Literal

from pydantic import Field, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresConnectionPassword,
    PostgresConnectionPasswordFile,
    PostgresConnectionProperties,
    PostgresConnectionPropertiesBase,
    PostgresDataSourceConnection,
)


class SupabaseConnectionPropertiesBase(PostgresConnectionPropertiesBase, ABC):
    sslmode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = Field(
        "require", description="SSL mode for the connection. Supabase recommends 'require' or stricter."
    )


class SupabaseConnectionPassword(SupabaseConnectionPropertiesBase, PostgresConnectionPassword):
    pass


class SupabaseConnectionPasswordFile(SupabaseConnectionPropertiesBase, PostgresConnectionPasswordFile):
    pass


class SupabaseDataSource(DataSourceBase, ABC):
    type: Literal["supabase"] = Field("supabase")
    connection_properties: PostgresConnectionProperties = Field(
        ..., alias="connection", description="Data source connection details"
    )

    @field_validator("connection_properties", mode="before")
    def infer_connection_type(cls, value):
        if "password" in value:
            return SupabaseConnectionPassword(**value)
        elif "password_file" in value:
            return SupabaseConnectionPasswordFile(**value)
        raise ValueError("Unknown connection structure")


class SupabaseDataSourceConnection(PostgresDataSourceConnection):
    def _create_connection(self, config: PostgresConnectionProperties):
        connection = super()._create_connection(config)
        # Supabase's shared pooler in transaction mode hands each transaction to any server
        # connection, so a statement psycopg prepares after 5 runs is missing (or already
        # present) on the next one. None turns preparing off. It cannot come from the YAML,
        # because to_connection_kwargs drops None values.
        connection.prepare_threshold = None
        return connection
