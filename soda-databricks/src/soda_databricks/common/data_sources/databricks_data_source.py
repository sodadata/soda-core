from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_dialect import SqlDialect
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
)
from soda_databricks.common.statements.databricks_metadata_columns_query import (
    DatabricksMetadataColumnsQuery,
)
from soda_databricks.model.data_source.databricks_data_source import (
    DatabricksDataSource as DatabricksDataSourceModel,
)


class DatabricksDataSourceImpl(DataSourceImpl, model_class=DatabricksDataSourceModel):
    def __init__(self, data_source_model: DatabricksDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return DatabricksSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return DatabricksDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_columns_query(self) -> DatabricksMetadataColumnsQuery:
        return DatabricksMetadataColumnsQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
        )


class DatabricksSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    def column_data_type(self) -> str:
        return self.default_casify("full_data_type")

    def supports_data_type_character_maximun_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return False

    def supports_data_type_numeric_scale(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def get_contract_type_dict(self) -> dict[str, str]:
        return {
            SodaDataTypeName.TEXT: "string",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.DECIMAL: "double",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.TIMESTAMP: "timestamp_ntz",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",
            SodaDataTypeName.BOOLEAN: "boolean",
        }
