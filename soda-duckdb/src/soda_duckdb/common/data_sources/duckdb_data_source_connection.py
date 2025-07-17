from abc import ABC
from typing import Dict, Literal, Optional

from duckdb import DuckDBPyConnection
from pydantic import Field, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class DuckDBConnectionProperties(DataSourceConnectionProperties, ABC):
    schema_: Optional[str] = Field(
        "main", description="Optional schema name to use for the DuckDB connection", alias="schema"
    )


class DuckDBStandardConnectionProperties(DuckDBConnectionProperties):
    database: str = Field(
        ":memory:", description="Path to the DuckDB database file or ':memory:' for in-memory database"
    )
    read_only: bool = Field(False, description="If True, the database is opened in read-only mode")
    configuration: Dict[str, str] = Field(dict(), description="Optional configuration dictionary for DuckDB")


class DuckDBExistingConnectionProperties(DuckDBConnectionProperties, arbitrary_types_allowed=True):
    duckdb_connection: Optional[DuckDBPyConnection] = Field(
        None, description="Optional existing DuckDB connection to use instead of creating a new one"
    )


class DuckDBDataSource(DataSourceBase, ABC):
    type: Literal["duckdb"] = Field("duckdb")
    connection_properties: DuckDBConnectionProperties = Field(
        ..., alias="connection", description="DuckDB connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if isinstance(value, DuckDBConnectionProperties):
            return value

        if "database" in value:
            return DuckDBStandardConnectionProperties(**value)
        elif "duckdb_connection" in value:
            return DuckDBExistingConnectionProperties(**value)
        raise ValueError("Could not infer DuckDB connection type from input")
