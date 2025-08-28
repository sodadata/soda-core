from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import CAST, REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSource as PostgresDataSourceModel,
)
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)


class PostgresDataSourceImpl(DataSourceImpl, model_class=PostgresDataSourceModel):
    def __init__(self, data_source_model: PostgresDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return PostgresDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class PostgresSqlDataType(SqlDataType):
    def get_sql_data_type_str_with_parameters(self) -> str:
        if isinstance(self.datetime_precision, int) and self.name == "timestamp with time zone":
            return f"timestamp({self.datetime_precision}) with time zone"
        elif isinstance(self.datetime_precision, int) and self.name == "timestamp without time zone":
            return f"timestamp({self.datetime_precision}) without time zone"
        return super().get_sql_data_type_str_with_parameters()


class PostgresSqlDialect(SqlDialect):
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

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        return {
            SodaDataTypeName.CHAR: "char",
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "text",
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.NUMERIC: "numeric",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.FLOAT: "float",
            SodaDataTypeName.DOUBLE: "double",
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
            "smallint": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "real": SodaDataTypeName.FLOAT,
            "double precision": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp without time zone": SodaDataTypeName.TIMESTAMP,
            "timestamptz": SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "time without time zone": SodaDataTypeName.TIME,
            "boolean": SodaDataTypeName.BOOLEAN,
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
            ["double precision", "float8"],
            ["timestamp", "timestamp without time zone"],
        ]

    def get_sql_data_type_class(self) -> type:
        return PostgresSqlDataType
