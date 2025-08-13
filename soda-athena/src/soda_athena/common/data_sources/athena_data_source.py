import logging
from typing import Optional

from soda_athena.common.data_sources.athena_data_source_connection import (
    AthenaDataSource as AthenaDataSourceModel,
)
from soda_athena.common.data_sources.athena_data_source_connection import (
    AthenaDataSourceConnection,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_datatypes import DBDataType
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
    def get_contract_type_dict(self) -> dict[str, str]:
        base_contract_type_dict = super().get_contract_type_dict()
        base_contract_type_dict[DBDataType.TEXT] = "string"
        base_contract_type_dict[DBDataType.BOOLEAN] = "boolean"
        base_contract_type_dict[DBDataType.INTEGER] = "int"
        base_contract_type_dict[DBDataType.DECIMAL] = "decimal"
        base_contract_type_dict[DBDataType.DATE] = "date"
        base_contract_type_dict[DBDataType.TIME] = "date"
        base_contract_type_dict[DBDataType.TIMESTAMP] = "timestamp"
        base_contract_type_dict[DBDataType.TIMESTAMP_TZ] = "timestamp"
        return base_contract_type_dict

    def get_sql_type_dict(self) -> dict[str, str]:
        base_sql_type_dict = super().get_sql_type_dict()
        base_sql_type_dict[DBDataType.TIMESTAMP] = "timestamp(3)"
        base_sql_type_dict[DBDataType.TIMESTAMP_TZ] = "timestamp(3)"
        return base_sql_type_dict

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return identifier
