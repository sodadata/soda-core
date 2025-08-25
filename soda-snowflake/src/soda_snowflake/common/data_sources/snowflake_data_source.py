from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import COLUMN, COUNT, DISTINCT, TUPLE, VALUES
from soda_core.common.sql_dialect import (
    DataSourceDataTypes,
    DBDataType,
    SqlDataTypeMapping,
    SqlDialect,
)
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

    def default_varchar_length(self) -> Optional[int]:
        return 16777216

    def get_contract_type_dict(self) -> dict[str, str]:
        return {
            DBDataType.TEXT: "TEXT",
            DBDataType.INTEGER: "NUMBER",
            DBDataType.DECIMAL: "FLOAT",
            DBDataType.DATE: "DATE",
            DBDataType.TIME: "TIME",
            DBDataType.TIMESTAMP: "TIMESTAMP_NTZ",
            DBDataType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
            DBDataType.BOOLEAN: "BOOLEAN",
        }

    def get_canonical_data_type_mappings(self) -> dict:
        return {
            # numeric
            "decimal": "number",
            "numeric": "number",
            "int": "number",
            "integer": "number",
            "bigint": "number",
            "smallint": "number",
            "tinyint": "number",
            "byteint": "number",
            "double": "float",
            "double precision": "float",
            "real": "float",
            "float4": "float",
            "float8": "float",
            # string
            "char": "varchar",
            "character": "varchar",
            "string": "varchar",
            "text": "varchar",
            "nchar": "varchar",
            "nvarchar": "varchar",
            "nvarchar2": "varchar",
            "char varying": "varchar",
            # date & time
            "datetime": "timestamp_ntz",  # synonym in snowflake
            "timestamp": "timestamp_ntz",  # default behavior if not qualified
            # boolean
            # (no real synonyms, but sometimes bool is used informally)
            "bool": "boolean",
        }

    SNOWFLAKE_DATA_TYPES: DataSourceDataTypes = DataSourceDataTypes(
        supported_data_type_names=[
            # numeric
            "number",
            "decimal",
            "numeric",
            "int",
            "integer",
            "bigint",
            "smallint",
            "tinyint",
            "byteint",
            "float",
            "float4",
            "float8",
            "double",
            "double precision",
            "real",
            # string & binary
            "varchar",
            "char",
            "character",
            "string",
            "text",
            "nchar",
            "nvarchar",
            "nvarchar2",
            "char varying",
            "binary",
            "varbinary",
            # date & time
            "date",
            "datetime",
            "time",
            "timestamp",
            "timestamp_ltz",
            "timestamp_ntz",
            "timestamp_tz",
            # semi-structured
            "variant",
            "object",
            "array",
            # boolean
            "boolean",
            # geography
            "geography",
            # vector
            "vector",
        ],
        mappings=[
            SqlDataTypeMapping(
                supported_data_type_name="varchar",
                source_data_type_names=SqlDataTypeMapping.DEFAULT_VARCHAR_TYPES,
            ),
            SqlDataTypeMapping(
                supported_data_type_name="integer",
                source_data_type_names=SqlDataTypeMapping.DEFAULT_INTEGER_TYPES,
            ),
        ],
    )

    def get_data_source_data_types(self) -> DataSourceDataTypes:
        return self.SNOWFLAKE_DATA_TYPES
