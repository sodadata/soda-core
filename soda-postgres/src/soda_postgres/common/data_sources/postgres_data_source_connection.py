from __future__ import annotations

import psycopg2
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_postgres.model.data_source.postgres_connection_properties import (
    PostgresConnectionProperties,
)


class PostgresDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: PostgresConnectionProperties,
    ):
        return psycopg2.connect(**config.to_connection_kwargs())
