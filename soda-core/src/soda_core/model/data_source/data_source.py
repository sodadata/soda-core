import abc
from typing import Literal, Optional

from pydantic import BaseModel, Field
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_core.model.failed_rows import FailedRowsConfigDatasource


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
    connection_properties: DataSourceConnectionProperties = Field(
        ..., alias="connection", description="Data source connection details"
    )
    failed_rows: Optional[FailedRowsConfigDatasource] = Field(..., description="Configuration for failed rows storage")

    @classmethod
    def get_class_type(cls) -> str:
        """The only way to get the simple type like 'postgres' for plugin registry before instantiating the class"""
        return cls.model_fields["type"].default
