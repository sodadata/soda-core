import logging
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import DataSourceNamespace, SodaDataTypeName
from soda_core.common.sql_ast import (
    COLUMN,
    CONCAT,
    CONCAT_WS,
    COUNT,
    DISTINCT,
    FROM,
    REGEX_LIKE,
    TUPLE,
    VALUES,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSource as TrinoDataSourceModel,
)
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSourceConnection,
)

logger: logging.Logger = soda_logger


REDSHIFT_DOUBLE_PRECISION = "double precision"
REDSHIFT_CHARACTER_VARYING = "character varying"


class TrinoDataSourceImpl(DataSourceImpl, model_class=TrinoDataSourceModel):
    def __init__(self, data_source_model: TrinoDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return TrinoSqlDialect(data_source_impl=self)

    def _create_data_source_connection(self) -> DataSourceConnection:
        return TrinoDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )
    


class TrinoSqlDialect(SqlDialect):
    pass
