import abc
from typing import Literal, Union

from pydantic import Field
from soda_core.model.data_source.data_source import DataSourceBase
from soda_postgres.model.data_source.postgres_connection_properties import (
    PostgresConnectionPassword,
    PostgresConnectionPasswordFile,
    PostgresConnectionString,
)


class PostgresDataSource(DataSourceBase, abc.ABC):
    type: Literal["postgres"]
    connection_properties: Union[
        PostgresConnectionPassword, PostgresConnectionPasswordFile, PostgresConnectionString
    ] = Field(..., alias="connection", description="Data source connection details")
