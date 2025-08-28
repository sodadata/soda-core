from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_ast import COLUMN, COUNT, DISTINCT, TUPLE, VALUES
from soda_core.common.sql_dialect import SqlDialect
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSource as SnowflakeDataSourceModel,
)
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSourceConnection,
)


class SnowflakeDataSourceImpl(DataSourceImpl, model_class=SnowflakeDataSourceModel):
    def __init__(self, data_source_model: SnowflakeDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SnowflakeSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SnowflakeDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SnowflakeSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return " SELECT * FROM VALUES\n" + ",\n".join([self.build_expression_sql(value) for value in values.values])

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"ARRAY_CONSTRUCT{super()._build_tuple_sql(tuple)}"

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        # Implements data type synonyms
        # Each list should represent a list of synonyms
        return [
            ["varchar", "text", "string"],
            ["number", "decimal", "numeric", "int", "integer", "bigint", "smallint", "tinyint", "byteint"],
            ["float", "float4", "float8", "double", "double precision", "real"],
            ["timestamp", "datetime", "timestamp_ntz", "timestamp without time zone"],
            ["timestamp_ltz", "timestamp with local time zone"],
            ["timestamp_tz", "timestamp with time zone"],
        ]

    def get_sql_data_type_name_by_soda_data_type_names(self) -> dict:
        """
        Maps DBDataType names to data source type names.
        """
        return {
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "text",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp_tz",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return data_type_name.lower() in ["varchar", "char", "character", "text"]

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in [
            "timestamp",
            "timestamp_ntz",
            "timestamp without time zone",
            "timestamp_tz",
            "timestamp with time zone",
            "timestamp_ltz",
            "timestamp with local time zone",
        ]
