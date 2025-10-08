from logging import Logger
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import CAST, CREATE_TABLE_COLUMN, REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSource as PostgresDataSourceModel,
)
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)

logger: Logger = soda_logger


PG_TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone"
PG_TIMESTAMP_WITHOUT_TIME_ZONE = "timestamp without time zone"
PG_DOUBLE_PRECISION = "double precision"


class PostgresDataSourceImpl(DataSourceImpl, model_class=PostgresDataSourceModel):
    def __init__(self, data_source_model: PostgresDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return PostgresDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class PostgresSqlDataType(SqlDataType):
    def get_sql_data_type_str_with_parameters(self) -> str:
        if isinstance(self.datetime_precision, int) and self.name == PG_TIMESTAMP_WITH_TIME_ZONE:
            return f"timestamp({self.datetime_precision}) with time zone"
        elif isinstance(self.datetime_precision, int) and self.name == PG_TIMESTAMP_WITHOUT_TIME_ZONE:
            return f"timestamp({self.datetime_precision}) without time zone"
        return super().get_sql_data_type_str_with_parameters()


class PostgresSqlDialect(SqlDialect):
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.DOUBLE, SodaDataTypeName.FLOAT),
    )

    def __init__(self):
        super().__init__()

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        return (
            f"{super().create_schema_if_not_exists_sql(prefixes, add_semicolon=False)} AUTHORIZATION CURRENT_USER"
            + (";" if add_semicolon else "")
        )

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[SodaDataTypeName, str]:
        return {
            SodaDataTypeName.CHAR: "char",
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "text",
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.NUMERIC: "numeric",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.FLOAT: "float",
            SodaDataTypeName.DOUBLE: PG_DOUBLE_PRECISION,
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamptz",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "character varying": SodaDataTypeName.VARCHAR,
            "varchar": SodaDataTypeName.VARCHAR,
            "character": SodaDataTypeName.CHAR,
            "char": SodaDataTypeName.CHAR,
            "text": SodaDataTypeName.TEXT,
            "bpchar": SodaDataTypeName.TEXT,
            "smallint": SodaDataTypeName.SMALLINT,
            "int2": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "int": SodaDataTypeName.INTEGER,
            "int4": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "int8": SodaDataTypeName.BIGINT,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "real": SodaDataTypeName.FLOAT,
            "float4": SodaDataTypeName.FLOAT,
            PG_DOUBLE_PRECISION: SodaDataTypeName.DOUBLE,
            "float8": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            PG_TIMESTAMP_WITHOUT_TIME_ZONE: SodaDataTypeName.TIMESTAMP,
            "timestamptz": SodaDataTypeName.TIMESTAMP_TZ,
            PG_TIMESTAMP_WITH_TIME_ZONE: SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "time without time zone": SodaDataTypeName.TIME,
            "boolean": SodaDataTypeName.BOOLEAN,
            "bool": SodaDataTypeName.BOOLEAN,
        }

    def _build_cast_sql(self, cast: CAST) -> str:
        to_type_text: str = (
            self.get_data_source_data_type_name_for_soda_data_type_name(cast.to_type)
            if isinstance(cast.to_type, SodaDataTypeName)
            else cast.to_type
        )
        return f"{self.build_expression_sql(cast.expression)}::{to_type_text}"

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["varchar", "character varying"],
            ["char", "character"],
            ["integer", "int", "int4"],
            ["bigint", "int8"],
            ["smallint", "int2"],
            ["real", "float4"],
            [PG_DOUBLE_PRECISION, "float8"],
            ["timestamp", PG_TIMESTAMP_WITHOUT_TIME_ZONE],
            ["decimal", "numeric"],
        ]

    def get_sql_data_type_class(self) -> type:
        return PostgresSqlDataType

    def _build_create_table_column_type(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        if create_table_column.type.name == "text":  # Do not output text with parameters!
            if create_table_column.type.character_maximum_length is not None:
                logger.warning(
                    f"Text column {create_table_column.name} has a character maximum length, but text does not support parameters! Ignoring in postgres."
                )
            return "text"
        return super()._build_create_table_column_type(create_table_column=create_table_column)
