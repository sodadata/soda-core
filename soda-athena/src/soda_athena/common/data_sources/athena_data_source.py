import logging

from soda_athena.common.data_sources.athena_data_source_connection import (
    AthenaDataSource as AthenaDataSourceModel,
)
from soda_athena.common.data_sources.athena_data_source_connection import (
    AthenaDataSourceConnection,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import SqlDialect

logger: logging.Logger = soda_logger


class AthenaDataSourceImpl(DataSourceImpl, model_class=AthenaDataSourceModel):
    def __init__(self, data_source_model: AthenaDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return AthenaSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return AthenaDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class AthenaSqlDialect(SqlDialect):
    pass
