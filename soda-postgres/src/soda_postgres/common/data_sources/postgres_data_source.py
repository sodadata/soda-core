from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import CAST, REGEX_LIKE
from soda_core.common.sql_dialect import DBDataType, SqlDialect, DataSourceDataTypes, SqlDataTypeMapping
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

    def get_sql_type_dict(self) -> dict[str, str]:
        base_dict: dict = super().get_sql_type_dict()
        base_dict[DBDataType.TEXT] = f"character varying({self.default_varchar_length()})"
        return base_dict

    def _build_cast_sql(self, cast: CAST) -> str:
        to_type_text: str = (
            self.get_data_type_type_str(cast.to_type) if isinstance(cast.to_type, DBDataType) else cast.to_type
        )
        return f"{self.build_expression_sql(cast.expression)}::{to_type_text}"

    POSTGRES_DATA_TYPES: DataSourceDataTypes = DataSourceDataTypes(
        supported_data_type_names=[
            # Character types
            "character varying", "varchar",
            "character", "char",
            "text",

            # Numeric types
            "smallint", "integer", "bigint",
            "decimal", "numeric",
            "real", "double precision",
            "smallserial", "serial", "bigserial",

            # Date/Time
            "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone",
            "date", "time", "time with time zone", "time without time zone",
            "interval",

            # Monetary
            "money",

            # Binary
            "bytea",

            # Boolean
            "boolean",

            # Enumerated types
            "enum",

            # Geometric types
            "point", "line", "lseg", "box", "path", "polygon", "circle",

            # Network address types
            "cidr", "inet", "macaddr", "macaddr8",

            # Bit string types
            "bit", "bit varying",

            # Text search types
            "tsvector", "tsquery",

            # UUID
            "uuid",

            # XML / JSON
            "xml", "json", "jsonb",

            # Arrays
            "array",

            # Composite types
            "composite",

            # Range types
            "int4range", "int8range", "numrange",
            "tsrange", "tstzrange", "daterange",

            # Object identifiers
            "oid", "regclass", "regtype", "regproc", "regprocedure", "regoper",
            "regoperator", "regconfig", "regdictionary", "pg_lsn"
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
        ]
    )

    def get_data_source_data_types(self) -> DataSourceDataTypes:
        return self.POSTGRES_DATA_TYPES
