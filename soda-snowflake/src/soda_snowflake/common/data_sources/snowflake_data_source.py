from logging import Logger

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_ast import COLUMN, COUNT, DISTINCT, TUPLE, VALUES
from soda_core.common.sql_dialect import SqlDialect
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSource as SnowflakeDataSourceModel,
)
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSourceConnection,
)

logger: Logger = soda_logger


TIMESTAMP_WITHOUT_TIME_ZONE = "timestamp without time zone"
TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone"
TIMESTAMP_WITH_LOCAL_TIME_ZONE = "timestamp with local time zone"


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

    def metadata_casify(self, identifier: str) -> str:
        """Metadata identifiers are not uppercased for Snowflake."""
        return identifier

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
            ["timestamp", "datetime", "timestamp_ntz", TIMESTAMP_WITHOUT_TIME_ZONE],
            ["timestamp_ltz", TIMESTAMP_WITH_LOCAL_TIME_ZONE],
            ["timestamp_tz", TIMESTAMP_WITH_TIME_ZONE],
        ]

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        """
        Maps DBDataType names to data source type names.
        """
        return {
            SodaDataTypeName.CHAR: "char",  # Note: by default a char is 1 byte in Snowflake!
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "text",  # alias for varchar
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.DECIMAL: "number",  # decimal & numeric → number
            SodaDataTypeName.NUMERIC: "number",
            SodaDataTypeName.FLOAT: "float",  # float / double → float
            SodaDataTypeName.DOUBLE: "float",
            SodaDataTypeName.TIMESTAMP: "timestamp_ntz",  # default timestamp in snowflake
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp_tz",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return data_type_name.lower() in ["varchar", "char", "character", "text"]

    # TODO: test this thorough. The code here is generated using AI just to be able to test the E2E.
    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.CHAR,
            "character": SodaDataTypeName.CHAR,
            "text": SodaDataTypeName.TEXT,
            "string": SodaDataTypeName.VARCHAR,
            "smallint": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "int": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "tinyint": SodaDataTypeName.SMALLINT,
            "byteint": SodaDataTypeName.SMALLINT,
            "number": SodaDataTypeName.NUMERIC,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "float4": SodaDataTypeName.FLOAT,
            "float8": SodaDataTypeName.DOUBLE,
            "double": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "real": SodaDataTypeName.FLOAT,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp_ntz": SodaDataTypeName.TIMESTAMP,
            TIMESTAMP_WITHOUT_TIME_ZONE: SodaDataTypeName.TIMESTAMP,
            "datetime": SodaDataTypeName.TIMESTAMP,
            "timestamp_tz": SodaDataTypeName.TIMESTAMP_TZ,
            TIMESTAMP_WITH_TIME_ZONE: SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp_ltz": SodaDataTypeName.TIMESTAMP_TZ,
            TIMESTAMP_WITH_LOCAL_TIME_ZONE: SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "boolean": SodaDataTypeName.BOOLEAN,
        }

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in [
            "timestamp",
            "timestamp_ntz",
            TIMESTAMP_WITHOUT_TIME_ZONE,
            "timestamp_tz",
            TIMESTAMP_WITH_TIME_ZONE,
            "timestamp_ltz",
            TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        ]

    def get_synonyms_for_soda_data_type(self) -> list[list[SodaDataTypeName]]:
        # This function can be overloaded if required.
        # It could be that the datasource has synonyms for the data types, and we want to handle that in the mappings.
        # For an example: see the postgres implementation
        return [
            [SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR],
            [
                SodaDataTypeName.NUMERIC,
                SodaDataTypeName.DECIMAL,
                SodaDataTypeName.INTEGER,
                SodaDataTypeName.BIGINT,
                SodaDataTypeName.SMALLINT,
            ],
            [SodaDataTypeName.FLOAT, SodaDataTypeName.DOUBLE],
        ]
