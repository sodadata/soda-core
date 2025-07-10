import abc
from typing import Literal, Union

from pydantic import Field
from soda_bigquery.model.data_source.bigquery_connection_properties import (
    BigQueryContextAuth,
    BigQueryJSONFileAuth,
    BigQueryJSONStringAuth,
)
from soda_core.model.data_source.data_source import DataSourceBase


class BigQueryDataSource(DataSourceBase, abc.ABC):
    type: Literal["bigquery"] = Field("bigquery")

    connection_properties: Union[
        BigQueryJSONStringAuth,
        BigQueryJSONFileAuth,
        BigQueryContextAuth,
    ] = Field(..., alias="connection", description="BigQuery connection configuration")
