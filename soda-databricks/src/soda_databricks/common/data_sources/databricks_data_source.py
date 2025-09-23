from logging import Logger
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import DataSourceNamespace, SodaDataTypeName
from soda_core.common.sql_ast import CREATE_TABLE_COLUMN
from soda_core.common.sql_dialect import SqlDialect
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
)
from soda_databricks.model.data_source.databricks_data_source import (
    DatabricksDataSource as DatabricksDataSourceModel,
)

logger: Logger = soda_logger


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

    SODA_DATA_TYPE_SYNONYM = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.TIMESTAMP_TZ, SodaDataTypeName.TIMESTAMP),
    )

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["int", "integer"],
        ]

    def column_data_type(self) -> str:
        return self.default_casify("data_type")

    def supports_data_type_character_maximum_length(self) -> bool:
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
            SodaDataTypeName.TIME: "string",  # no native TIME type in Databricks
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.TEXT,
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.CHAR,
            "tinyint": SodaDataTypeName.SMALLINT,
            "short": SodaDataTypeName.SMALLINT,
            "smallint": SodaDataTypeName.SMALLINT,
            "int": SodaDataTypeName.INTEGER,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "long": SodaDataTypeName.BIGINT,
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
            "timestamp_ntz": SodaDataTypeName.TIMESTAMP,  # If there is explicitly stated that the timestamp is without time zone, we consider it to be the same as TIMESTAMP
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

    def build_columns_metadata_query_str(self, table_namespace: DataSourceNamespace, table_name: str) -> str:
        # Unity catalog only stores things in lower case,
        # even though create table may have been quoted and with mixed case
        table_name_lower: str = table_name.lower()
        return super().build_columns_metadata_query_str(table_namespace, table_name_lower)

    def post_schema_create_sql(self, prefixes: list[str]) -> Optional[list[str]]:
        assert len(prefixes) == 2, f"Expected 2 prefixes, got {len(prefixes)}"
        catalog_name: str = self.quote_default(prefixes[0])
        schema_name: str = self.quote_default(prefixes[1])
        return [f"GRANT SELECT, USAGE, CREATE, MANAGE ON SCHEMA {catalog_name}.{schema_name} TO `account users`;"]
        #      f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {catalog_name}.{schema_name} TO `account users`;"]

    @classmethod
    def is_same_soda_data_type_with_synonyms(cls, expected: SodaDataTypeName, actual: SodaDataTypeName) -> bool:
        # Special case of a 1-way synonym: TEXT is allowed where TIME is expected
        if expected == SodaDataTypeName.TIME and actual == SodaDataTypeName.TEXT:
            logger.debug(
                f"In is_same_soda_data_type_with_synonyms, Expected {expected} and actual {actual} are the same"
            )
            return True
        return super().is_same_soda_data_type_with_synonyms(expected, actual)
