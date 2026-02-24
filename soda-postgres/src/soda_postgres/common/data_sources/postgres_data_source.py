from logging import Logger
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import (
    DataSourceNamespace,
    SodaDataTypeName,
    SqlDataType,
)
from soda_core.common.sql_ast import (
    AND,
    CAST,
    COLUMN,
    CREATE_TABLE_COLUMN,
    EQ,
    FROM,
    GT,
    IN,
    JOIN,
    LEFT_INNER_JOIN,
    LITERAL,
    LOWER,
    ORDER_BY_ASC,
    RAW_SQL,
    REGEX_LIKE,
    SELECT,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSource as PostgresDataSourceModel,
)
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)
from soda_postgres.statements.postgres_metadata_tables_query import (
    PostgresMetadataTablesQuery,
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

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return PostgresMetadataTablesQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
        )


class PostgresSqlDataType(SqlDataType):
    def get_sql_data_type_str_with_parameters(self) -> str:
        if isinstance(self.datetime_precision, int) and self.name == PG_TIMESTAMP_WITH_TIME_ZONE:
            return f"timestamp({self.datetime_precision}) with time zone"
        elif isinstance(self.datetime_precision, int) and self.name == PG_TIMESTAMP_WITHOUT_TIME_ZONE:
            return f"timestamp({self.datetime_precision}) without time zone"
        return super().get_sql_data_type_str_with_parameters()


class PostgresSqlDialect(SqlDialect, sqlglot_dialect="postgres"):
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.DOUBLE, SodaDataTypeName.FLOAT),
    )

    def supports_materialized_views(self) -> bool:
        return True

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
        if create_table_column.type.name == PG_DOUBLE_PRECISION:
            if create_table_column.type.numeric_precision is not None:
                logger.warning(
                    f"Double precision column {create_table_column.name} has a numeric precision, but double precision does not support parameters! Ignoring in postgres."
                )
            if create_table_column.type.numeric_scale is not None:
                logger.warning(
                    f"Double precision column {create_table_column.name} has a numeric scale, but double precision does not support parameters! Ignoring in postgres."
                )
            return PG_DOUBLE_PRECISION
        return super()._build_create_table_column_type(create_table_column=create_table_column)

    @classmethod
    def is_same_soda_data_type_with_synonyms(cls, expected: SodaDataTypeName, actual: SodaDataTypeName) -> bool:
        # Postgres cursor can return bpchar, which is an unbounded synonym for char. We convert that to TEXT as that is the best fit. So we could expect CHAR, but actual is TEXT.
        if expected == SodaDataTypeName.CHAR and actual == SodaDataTypeName.TEXT:
            logger.debug(
                f"In is_same_soda_data_type_with_synonyms, expected {expected} and actual {actual} are treated as the same because of postgres cursor returning BPCHAR (best matching with TEXT)"
            )
            return True

        return super().is_same_soda_data_type_with_synonyms(expected, actual)

    ###
    # Tables and columns metadata queries
    ###
    def _pg_catalog(self) -> str:
        return "pg_catalog"

    def _pg_class(self) -> str:
        return "pg_class"

    def _pg_namespace(self) -> str:
        return "pg_namespace"

    def _current_database(self) -> str:
        return "current_database()"

    def relkind_table_type_sql_expression(self, table_alias: str = "c", column_alias: str = "table_type") -> str:
        return f"""CASE {table_alias}.relkind
            WHEN 'r' THEN
            CASE {table_alias}.relpersistence
                WHEN 't' THEN 'TEMPORARY TABLE'
                WHEN 'p' THEN 'BASE TABLE'
                WHEN 'u' THEN 'UNLOGGED TABLE'
                END
            WHEN 'v' THEN 'VIEW'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'i' THEN 'INDEX'
            WHEN 'S' THEN 'SEQUENCE'
            WHEN 't' THEN 'TOAST TABLE'
            WHEN 'f' THEN 'FOREIGN TABLE'
            WHEN 'p' THEN 'PARTITIONED TABLE'
            WHEN 'I' THEN 'PARTITIONED INDEX'
            END as {column_alias}"""

    def build_columns_metadata_query_str(self, table_namespace: DataSourceNamespace, table_name: str) -> str:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """

        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()

        ######
        current_database_expression = RAW_SQL(self._current_database())
        select: list = [
            SELECT(
                [
                    COLUMN("attname", table_alias="a", field_alias="column_name"),
                    # Normalize data type into information_schema.columns style. Consider doing this in python instead, but this is lightweight and simple enough.
                    RAW_SQL(
                        """CASE
                            -- arrays
                            WHEN t.typcategory = 'A' OR t.typelem <> 0 THEN 'ARRAY'

                            -- choose base type for domains, otherwise the type itself
                            ELSE CASE COALESCE(bt.typname, t.typname)
                            WHEN 'varchar'     THEN 'character varying'
                            WHEN 'bpchar'      THEN 'character'
                            WHEN 'bool'        THEN 'boolean'
                            WHEN 'int2'        THEN 'smallint'
                            WHEN 'int4'        THEN 'integer'
                            WHEN 'int8'        THEN 'bigint'
                            WHEN 'float4'      THEN 'real'
                            WHEN 'float8'      THEN 'double precision'
                            WHEN 'timestamptz' THEN 'timestamp with time zone'
                            WHEN 'timestamp'   THEN 'timestamp without time zone'
                            WHEN 'timetz'      THEN 'time with time zone'
                            WHEN 'time'        THEN 'time without time zone'
                            WHEN 'bit'         THEN 'bit'
                            WHEN 'varbit'      THEN 'bit varying'
                            ELSE COALESCE(bt.typname, t.typname)
                            END
                        END AS  \"data_type\"
                    """
                    ),
                    # Extract type parameters. No abstract level api for this, we have to replicate Postgres logic here.
                    # All a.atttypmod are offset by 4 in Postgres
                    #  varchar/char length (NULL otherwise)
                    RAW_SQL(
                        """CASE
                            WHEN t.typname IN ('varchar','bpchar') THEN
                                CASE
                                    WHEN a.atttypmod > 4 THEN a.atttypmod - 4
                                    ELSE NULL
                                END
                            ELSE NULL
                        END AS "character_maximum_length"
                    """
                    ),
                    # numeric precision (NULL otherwise)
                    RAW_SQL(
                        """CASE
                            WHEN t.typname = 'numeric' THEN
                                CASE
                                    WHEN a.atttypmod > 4 THEN ((a.atttypmod - 4) >> 16)
                                    ELSE NULL
                                END
                            ELSE NULL
                        END AS "numeric_precision"
                    """
                    ),
                    # numeric scale (NULL otherwise)
                    RAW_SQL(
                        """CASE
                            WHEN t.typname = 'numeric' THEN
                                CASE
                                    WHEN a.atttypmod > 4 THEN ((a.atttypmod - 4) & 65535)
                                    ELSE NULL
                                END
                            ELSE NULL
                        END AS "numeric_scale"
                    """
                    ),
                    # datetime precision (NULL otherwise)
                    RAW_SQL(
                        """CASE
                            WHEN t.typname IN ('time','timetz','timestamp','timestamptz') THEN
                                CASE
                                    WHEN a.atttypmod >= 0 THEN a.atttypmod
                                    ELSE NULL
                                END
                            ELSE NULL
                        END AS "datetime_precision"
                    """
                    ),
                    COLUMN(current_database_expression, field_alias="table_catalog"),
                    COLUMN("nspname", table_alias="n", field_alias="table_schema"),
                    COLUMN("relname", table_alias="c", field_alias="table_name"),
                    RAW_SQL(self.relkind_table_type_sql_expression()),
                ]
            ),
            FROM(
                self._pg_class(),
                table_prefix=[self._pg_catalog()],
                alias="c",
            ),
            JOIN(
                table_name=self._pg_namespace(),
                table_prefix=[self._pg_catalog()],
                alias="n",
                on_condition=EQ(
                    COLUMN("relnamespace", "c"),
                    COLUMN("oid", "n"),
                ),
            ),
            JOIN(
                table_name="pg_attribute",
                table_prefix=[self._pg_catalog()],
                alias="a",
                on_condition=EQ(
                    COLUMN("attrelid", "a"),
                    COLUMN("oid", "c"),
                ),
            ),
            JOIN(
                table_name="pg_type",
                table_prefix=[self._pg_catalog()],
                alias="t",
                on_condition=EQ(
                    COLUMN("atttypid", "a"),
                    COLUMN("oid", "t"),
                ),
            ),
            LEFT_INNER_JOIN(
                table_name="pg_type",
                table_prefix=[self._pg_catalog()],
                alias="bt",
                on_condition=EQ(
                    COLUMN("oid", "bt"),
                    RAW_SQL("NULLIF(t.typbasetype, 0)"),
                ),
            ),
            WHERE(
                AND(
                    [
                        # Only get object types that correspond to tables/views in information_schema.tables
                        IN(
                            COLUMN("relkind", "c"),
                            [LITERAL("r"), LITERAL("p"), LITERAL("v"), LITERAL("m"), LITERAL("f")],
                        ),
                        # Only get columns that are not dropped
                        GT(COLUMN("attnum", "a"), LITERAL(0)),
                        EQ(COLUMN("relname", "c"), LITERAL(self.metadata_casify(table_name))),
                    ]
                )
            ),
            ORDER_BY_ASC(COLUMN("attnum", "a")),
        ]

        if database_name:
            database_name_lower: str = database_name.lower()
            select.append(WHERE(EQ(LOWER(current_database_expression), LITERAL(database_name_lower))))

        if schema_name:
            select.append(WHERE(EQ(LOWER(COLUMN("nspname", "n")), LITERAL(schema_name.lower()))))

        return self.build_select_sql(select)
