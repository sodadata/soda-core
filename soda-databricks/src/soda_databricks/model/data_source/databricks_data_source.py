import abc
from typing import Literal

from pydantic import Field, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksConnectionProperties,
    DatabricksTokenAuth,
)


class DatabricksDataSource(DataSourceBase, abc.ABC):
    type: Literal["databricks"] = Field("databricks")

    @classmethod
    def infer_connection_type(cls, value):
        if "access_token" in value:
            return DatabricksTokenAuth(**value)
        raise ValueError("Could not infer Databricks connection type from input")
