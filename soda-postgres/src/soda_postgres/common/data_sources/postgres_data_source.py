from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeNames
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

    def default_varchar_length(self) -> Optional[int]:
        return 255

    def get_sql_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        return {
            SodaDataTypeNames.TEXT: "varchar",
            SodaDataTypeNames.VARCHAR: "varchar",
            SodaDataTypeNames.INTEGER: "integer",
            SodaDataTypeNames.DECIMAL: "double precision",
            SodaDataTypeNames.NUMERIC: "numeric",
            SodaDataTypeNames.DATE: "date",
            SodaDataTypeNames.TIME: "time",
            SodaDataTypeNames.TIMESTAMP: "timestamp without time zone",
            SodaDataTypeNames.TIMESTAMP_TZ: "timestamp with time zone",
            SodaDataTypeNames.BOOLEAN: "boolean",
        }

    def _build_cast_sql(self, cast: CAST) -> str:
        to_type_text: str = (
            self.get_sql_data_type_name(cast.to_type) if isinstance(cast.to_type, SodaDataTypeNames) else cast.to_type
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
        ]

    def get_data_source_type_names_by_test_type_names(self) -> dict:
        """
        Maps DBDataType names to data source type names.
        """
        return {
            SodaDataTypeNames.VARCHAR: "varchar",
            SodaDataTypeNames.TEXT: "text",
            SodaDataTypeNames.INTEGER: "integer",
            SodaDataTypeNames.DECIMAL: "decimal",
            SodaDataTypeNames.NUMERIC: "decimal",
            SodaDataTypeNames.DATE: "date",
            SodaDataTypeNames.TIME: "time",
            SodaDataTypeNames.TIMESTAMP: "timestamp",
            SodaDataTypeNames.TIMESTAMP_TZ: "timestamp with time zone",
            SodaDataTypeNames.BOOLEAN: "boolean",
        }
