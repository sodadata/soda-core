import logging

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import INSERT_INTO, VALUES_ROW
from soda_core.common.sql_datatypes import DBDataType
from soda_core.common.sql_dialect import SqlDialect
from soda_fabric.common.data_sources.fabric_data_source_connection import (
    FabricDataSource as FabricDataSourceModel,
)
from soda_fabric.common.data_sources.fabric_data_source_connection import (
    FabricDataSourceConnection,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source import (
    SQLServerDataSourceImpl,
    SQLServerSqlDialect,
)

logger: logging.Logger = soda_logger


class FabricDataSourceImpl(SQLServerDataSourceImpl, model_class=FabricDataSourceModel):
    def __init__(self, data_source_model: FabricDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return FabricSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return FabricDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class FabricSqlDialect(SQLServerSqlDialect):
    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"DATETIMEFROMPARTS((datepart(YEAR, {timestamp_literal})), (datepart(MONTH, {timestamp_literal})), (datepart(DAY, {timestamp_literal})), 0, 0, 0, 0)"

    def _build_insert_into_values_sql(self, insert_into: INSERT_INTO) -> str:
        values_sql: str = "\n" + "\nUNION ALL ".join(
            [self._build_insert_into_values_row_sql(value) for value in insert_into.values]
        )
        return values_sql

    def _build_insert_into_values_row_sql(self, values: VALUES_ROW) -> str:
        values_sql: str = "SELECT " + ", ".join([self.literal(value) for value in values.values])
        values_sql = self.encode_string_for_sql(values_sql)
        return values_sql

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def get_contract_type_dict(self) -> dict[str, str]:
        super_dict = super().get_contract_type_dict()
        # Fabric does not support datetimeoffset (for timezones)
        super_dict[DBDataType.TIMESTAMP] = "datetime2"
        super_dict[DBDataType.TIMESTAMP_TZ] = "datetime2"
        return super_dict

    def get_sql_type_dict(self) -> dict[str, str]:
        super_dict = super().get_sql_type_dict()
        # Fabric does not support datetimeoffset (for timezones)
        # We specify the precision to 6 decimal places, this is the max precision supported by Fabric in Microsoft Fabric Data Warehouse.
        super_dict[DBDataType.TIMESTAMP] = "datetime2(6)"
        super_dict[DBDataType.TIMESTAMP_TZ] = "datetime2(6)"
        return super_dict
