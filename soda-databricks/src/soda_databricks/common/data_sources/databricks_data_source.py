from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_ast import CREATE_TABLE_COLUMN
from soda_core.common.sql_dialect import SqlDialect
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
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



class DatabricksSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    def get_sql_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        return {
            SodaDataTypeName.TEXT: "string",
            SodaDataTypeName.VARCHAR: "string",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.TIMESTAMP: "timestamp_ntz",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["int", "integer"],
        ]

    def column_data_type(self) -> str:
        return self.default_casify("data_type")

    def supports_data_type_character_maximun_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def column_data_type_max_length(self) -> str:
        return self.default_casify("character_maximum_length")

    def column_data_type_numeric_precision(self) -> str:
        return self.default_casify("numeric_precision")

    def column_data_type_numeric_scale(self) -> str:
        return self.default_casify("numeric_scale")

    def column_data_type_datetime_precision(self) -> str:
        return self.default_casify("datetime_precision")

    def _build_create_table_column_type(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        # Databricks will complain if string lengths or datetime precisions are passed in, so strip if they are provided
        if create_table_column.type.name == "string":
            create_table_column.type.character_maximum_length = None
        if create_table_column.type.name in ["timestamp_ntz", "timestamp"]:
            create_table_column.type.datetime_precision = None
        return super()._build_create_table_column_type(create_table_column=create_table_column)
