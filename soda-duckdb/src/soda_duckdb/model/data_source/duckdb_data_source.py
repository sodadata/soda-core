import abc
from typing import Literal

from pydantic import Field, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_duckdb.model.data_source.duckdb_connection_properties import (
    DuckDBConnectionProperties,
    DuckDBExistingConnectionProperties,
    DuckDBStandardConnectionProperties,
)


class DuckDBDataSource(DataSourceBase, abc.ABC):
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
