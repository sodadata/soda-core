import abc

from pydantic import BaseModel, Field
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class DataSourceBase(BaseModel, abc.ABC):
    name: str = Field(..., description="Data source name")
    type: str = Field(..., description="Data source type")
    # Alias connection -> connection_properties to read that from yaml config
    connection_properties: DataSourceConnectionProperties = Field(
        ..., alias="connection", description="Data source connection details"
    )

    class Config:
        # Allow to use both field names and aliases when populating from dict
        validate_by_name = True
