import logging

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import SqlDialect
from soda_sqlserver.common.data_sources.sqlserver_data_source import (
    SQLServerDataSourceImpl,
    SQLServerSqlDialect,
)
from soda_synapse.common.data_sources.synapse_data_source_connection import (
    SynapseDataSource as SynapseDataSourceModel,
)
from soda_synapse.common.data_sources.synapse_data_source_connection import (
    SynapseDataSourceConnection,
)

logger: logging.Logger = soda_logger


class SynapseDataSourceImpl(SQLServerDataSourceImpl, model_class=SynapseDataSourceModel):
    def __init__(self, data_source_model: SynapseDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SynapseSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SynapseDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SynapseSqlDialect(SQLServerSqlDialect):
    pass
    # def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
    #     return f"""
    #     CREATE SCHEMA [{schema_name}];
    #     """
