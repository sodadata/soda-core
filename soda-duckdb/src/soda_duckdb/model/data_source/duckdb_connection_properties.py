from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Optional, Dict, Literal, ClassVar

from pydantic import Field, SecretStr
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from duckdb import DuckDBPyConnection


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
