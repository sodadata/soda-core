import logging
from datetime import datetime, timezone

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import CREATE_TABLE_COLUMN, INSERT_INTO, VALUES_ROW
from soda_core.common.sql_dialect import SqlDialect
from soda_fabric.common.data_sources.fabric_data_source_connection import (
    FabricDataSource as FabricDataSourceModel,
)
from soda_fabric.common.data_sources.fabric_data_source_connection import (
    FabricDataSourceConnection,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source import (
    SqlServerDataSourceImpl,
    SqlServerSqlDialect,
)

logger: logging.Logger = soda_logger


class FabricDataSourceImpl(SqlServerDataSourceImpl, model_class=FabricDataSourceModel):
    def __init__(self, data_source_model: FabricDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return FabricSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return FabricDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class FabricSqlDialect(SqlServerSqlDialect):
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

    def get_sql_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        super_dict = super().get_sql_data_type_name_by_soda_data_type_names()
        # Fabric does not support datetimeoffset (for timezones)
        # We specify the precision to 6 decimal places, this is the max precision supported by Fabric in Microsoft Fabric Data Warehouse.
        super_dict[SodaDataTypeName.TIMESTAMP] = "datetime2"
        super_dict[SodaDataTypeName.TIMESTAMP_TZ] = "datetime2"
        return super_dict

    def literal_datetime_with_tz(self, datetime: datetime):
        # Fabric does not support datetimeoffset (for timezones)
        # So we will convert the timestamp to UTC and then convert it to a string
        return f"'{datetime.astimezone(timezone.utc).isoformat()}'"

    def _build_create_table_column_type(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        assert isinstance(create_table_column.type, SqlDataType)
        if create_table_column.type.name == "datetime2" and create_table_column.type.datetime_precision is None:
            return "datetime2(6)"
        else:
            return create_table_column.type.get_sql_data_type_str_with_parameters()

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        result = super().get_data_source_data_type_name_by_soda_data_type_names()
        result[SodaDataTypeName.TIMESTAMP_TZ] = "datetime2"
        return result
