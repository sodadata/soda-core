from abc import ABC
from typing import Literal, Optional, Union

from pydantic import Field, field_validator
from pyspark.sql import SparkSession
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class SparkDataFrameConnectionProperties(DataSourceConnectionProperties, ABC):
    schema_: Optional[str] = Field(
        "main", description="Optional schema name to use for the SparkDataFrame connection", alias="schema"
    )
    test_dir: Optional[str] = Field(None, description="The directory to use for the test")


class SparkDataFrameNewSessionProperties(SparkDataFrameConnectionProperties):
    new_session: bool = Field(True, description="Whether to create a new Spark session")


class SparkDataFrameExistingSessionProperties(SparkDataFrameConnectionProperties, arbitrary_types_allowed=True):
    spark_session: SparkSession = Field(..., description="The existing Spark session to use")


class SparkDataFrameDataSource(DataSourceBase, ABC):
    type: Literal["sparkdf"] = Field("sparkdf")
    connection_properties: Union[SparkDataFrameExistingSessionProperties, SparkDataFrameNewSessionProperties] = Field(
        ..., alias="connection", description="SparkDataFrame connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if isinstance(value, SparkDataFrameNewSessionProperties):
            return value

        if "spark_session" in value:
            return SparkDataFrameExistingSessionProperties(**value)
        elif "new_session" in value:
            return SparkDataFrameNewSessionProperties(**value)
        raise ValueError("Could not infer SparkDataFrame connection type from input")
