import abc
from typing import Dict, Literal

from pydantic import BaseModel, Field, field_validator
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class DataSourceBase(
    BaseModel,
    abc.ABC,
    frozen=True,
    extra="forbid",
    validate_by_name=True,  # Allow to use both field names and aliases when populating from dict
):
    name: str = Field(..., description="Data source name")
    type: Literal["data_source"]
    # Alias connection -> connection_properties to read that from yaml config
    connection_properties: Dict[str, DataSourceConnectionProperties] = Field(
        ..., alias="connections", description="Data source connection details"
    )

    @property
    def default_connection(self) -> DataSourceConnectionProperties:
        """Default first iteration implemetation. Use first connection in the dict for now. Empty dict is not allowed."""

        # TODO: Decide and implement the connection selection strategy. Use "default" or "primary" connection if available? etc
        return next(iter(self.connection_properties.values()))

    @classmethod
    def get_class_type(cls) -> str:
        """The only way to get the simple type like 'postgres' for plugin registry before instantiating the class"""
        return cls.model_fields["type"].default

    @field_validator("connection_properties", mode="before")
    @classmethod
    def _validate_connections(cls, value):
        if not isinstance(value, dict):
            raise ValueError("connections must be a dict")
        return {name: cls.infer_connection_type(conn) for name, conn in value.items()}
