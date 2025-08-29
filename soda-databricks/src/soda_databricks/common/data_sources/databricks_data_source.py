from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeName
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

    def build_columns_metadata_query_str(self, dataset_prefixes: list[str], dataset_name: str) -> str:
        return super().build_columns_metadata_query_str(dataset_prefixes, dataset_name.lower())


class DatabricksSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    # def column_data_type(self) -> str:
    #     return self.default_casify("full_data_type")

    def supports_data_type_character_maximun_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return False

    def supports_data_type_numeric_scale(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        return {
            SodaDataTypeName.CHAR: "string",
            SodaDataTypeName.VARCHAR: "string",
            SodaDataTypeName.TEXT: "string",
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "int",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.FLOAT: "float",
            SodaDataTypeName.DOUBLE: "double",
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "string",   # no native TIME type in Databricks
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.VARCHAR,
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.VARCHAR,
            "tinyint": SodaDataTypeName.SMALLINT,
            "smallint": SodaDataTypeName.SMALLINT,
            "int": SodaDataTypeName.INTEGER,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "real": SodaDataTypeName.FLOAT,
            "float4": SodaDataTypeName.FLOAT,
            "double": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "float8": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp without time zone": SodaDataTypeName.TIMESTAMP,
            "timestamptz": SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "boolean": SodaDataTypeName.BOOLEAN,
            # Not supported -> will be converted to varchar
            # "binary"
            # "interval",
            # "array",
            # "map",
            # "struct"
        }

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["varchar", "char", "string"],
            ["smallint", "int2"],
            ["integer", "int", "int4"],
            ["bigint", "int8"],
            ["real", "float4", "float"],
            ["double precision", "float8", "double"],
            ["timestamp", "timestamp without time zone"],
            ["timestamptz", "timestamp with time zone"],
            ["time", "time without time zone"],
        ]
