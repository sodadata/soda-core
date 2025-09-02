from collections import namedtuple
from pathlib import Path

from duckdb import DuckDBPyConnection
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import DataSourceConnectionException
from soda_core.common.metadata_types import DataSourceNamespace, SodaDataTypeName
from soda_core.common.sql_ast import *
from soda_core.common.sql_dialect import SqlDialect
from soda_duckdb.common.data_sources.duckdb_data_source_connection import (
    DuckDBConnectionProperties,
)
from soda_duckdb.common.data_sources.duckdb_data_source_connection import (
    DuckDBDataSource as DuckDBDataSourceModel,
)
from soda_duckdb.common.data_sources.duckdb_data_source_connection import (
    DuckDBExistingConnectionProperties,
    DuckDBStandardConnectionProperties,
)

DuckDBColumn = namedtuple(
    "DuckDBColumn", ["name", "type_code", "display_size", "internal_size", "precision", "scale", "null_ok"]
)

_in_memory_connection = None


class DuckDBCursor:
    def __init__(self, connection):
        self._connection = connection

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self._connection, attr)

    def close(self):
        # because a duckdb cursor is actually the current connection,
        # we don't want to close it
        pass

    @property
    def description(self):
        """
        Makes the cursor description available as a list of DuckDBColumn namedtuples.
        This is to be compatible with the expected interface of a DBAPI cursor.
        """
        return [DuckDBColumn(*col) for col in self._connection.description]


class DuckDBDataSourceConnectionWrapper:
    def __init__(self, delegate):
        self._delegate = delegate

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self._delegate, attr)

    def cursor(self):
        return DuckDBCursor(self._delegate)


class DuckDBSqlDialect(SqlDialect):
    def get_database_prefix_index(self) -> int | None:
        return None

    def get_schema_prefix_index(self) -> int | None:
        return 0

    def information_schema_namespace_elements(self, prefixes: list[str]) -> list[str]:
        """
        The prefixes / namespace of the information schema for a given dataset prefix / namespace
        """
        return [self.schema_information_schema()]

    def supports_data_type_character_maximum_length(self):
        """
        From docs: "Variable-length character string. The maximum length n has no effect and is only provided for compatibility"
        """
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_MATCHES({expression}, '{matches.regex_pattern}')"

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        schema_name: str = prefixes[0]
        quoted_schema_name: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {quoted_schema_name}" + (";" if add_semicolon else "")

    def format_metadata_data_type(self, data_type: str) -> str:
        paranthesis_index = data_type.find("(")
        if paranthesis_index != -1:
            return data_type[:paranthesis_index]
        return data_type

    def get_sql_data_type_name_by_soda_data_type_names(self) -> dict:
        base = super().get_sql_data_type_name_by_soda_data_type_names()
        base[SodaDataTypeName.NUMERIC] = "decimal"
        return base

    def default_numeric_precision(self) -> Optional[int]:
        return 18

    def default_numeric_scale(self) -> Optional[int]:
        return 3

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

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        """
        Maps DBDataType names to data source type names.
        """
        return {
            SodaDataTypeName.CHAR: "varchar",  # DuckDB doesn't have fixed CHAR, maps to VARCHAR
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "varchar",  # TEXT is an alias for VARCHAR
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.DECIMAL: "decimal",  # DuckDB supports DECIMAL(p,s)
            SodaDataTypeName.NUMERIC: "decimal",  # NUMERIC is an alias for DECIMAL
            SodaDataTypeName.FLOAT: "real",  # single-precision float
            SodaDataTypeName.DOUBLE: "double",  # double-precision float
            SodaDataTypeName.TIMESTAMP: "timestamp",  # default = without timezone
            SodaDataTypeName.TIMESTAMP_TZ: "timestamptz",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            # Character / String types
            "char": SodaDataTypeName.CHAR,
            "varchar": SodaDataTypeName.VARCHAR,
            "string": SodaDataTypeName.VARCHAR,
            # TODO analyze if this should be supported
            # "text": SodaDataTypeName.TEXT,
            # Integer types
            "tinyint": SodaDataTypeName.SMALLINT,  # closest match (no explicit TINYINT in Soda)
            "smallint": SodaDataTypeName.SMALLINT,
            "int2": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "int": SodaDataTypeName.INTEGER,
            "int4": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "int8": SodaDataTypeName.BIGINT,
            # TODO Review data issues
            # "utinyint": SodaDataTypeName.INTEGER,    # Soda doesn’t have unsigned types
            # "usmallint": SodaDataTypeName.INTEGER,
            # "uinteger": SodaDataTypeName.INTEGER,
            # "uint": SodaDataTypeName.INTEGER,
            # "ubigint": SodaDataTypeName.BIGINT,
            #
            # "hugeint": SodaDataTypeName.BIGINT,      # Soda has no HUGEINT, map to BIGINT
            # Decimal / Numeric
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            # Floating point
            "real": SodaDataTypeName.FLOAT,
            "float4": SodaDataTypeName.FLOAT,
            "float": SodaDataTypeName.DOUBLE,  # "float" is alias for double in DuckDB
            "float8": SodaDataTypeName.DOUBLE,
            "double": SodaDataTypeName.DOUBLE,
            # Boolean
            "boolean": SodaDataTypeName.BOOLEAN,
            "bool": SodaDataTypeName.BOOLEAN,
            # Date & Time
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "datetime": SodaDataTypeName.TIMESTAMP,
            "timestamp without time zone": SodaDataTypeName.TIMESTAMP,
            "timestamptz": SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
        }

    def build_columns_metadata_query_str(self, table_namespace: DataSourceNamespace, table_name: str) -> str:
        return super().build_columns_metadata_query_str(table_namespace, table_name)


class DuckDBDataSourceConnection(DataSourceConnection):
    REGISTERED_FORMAT_MAP = {
        ".csv": "read_csv_auto",
        ".parquet": "read_parquet",
        ".json": "read_json_auto",
    }

    def _create_connection(
        self,
        config: DuckDBConnectionProperties,
    ):
        import duckdb

        try:
            if isinstance(config, DuckDBExistingConnectionProperties):
                return DuckDBDataSourceConnectionWrapper(config.duckdb_connection)
            elif isinstance(config, DuckDBStandardConnectionProperties):
                if (read_function := self.REGISTERED_FORMAT_MAP.get(self.extract_format(config))) is not None:
                    connection = DuckDBDataSourceConnectionWrapper(duckdb.connect(":default:"))
                    connection.sql(
                        f"CREATE TABLE {self.extract_dataset_name(config)} AS SELECT * FROM {read_function}('{config.database}')"
                    )

                    return connection
                else:
                    if config.database == ":memory:":
                        # Re-use existing in-memory connection if it exists
                        global _in_memory_connection
                        if _in_memory_connection is not None:
                            return DuckDBDataSourceConnectionWrapper(_in_memory_connection)
                        _in_memory_connection = duckdb.connect(
                            database=":memory:", read_only=config.read_only, config=config.configuration
                        )
                        return DuckDBDataSourceConnectionWrapper(_in_memory_connection)
                    return DuckDBDataSourceConnectionWrapper(
                        duckdb.connect(
                            database=config.database,
                            read_only=config.read_only,
                            config=config.configuration,
                        )
                    )

        except Exception as e:
            raise DataSourceConnectionException(e)

    def extract_format(self, config: DuckDBStandardConnectionProperties) -> str:
        return Path(config.database).suffix

    def extract_dataset_name(self, config: DuckDBStandardConnectionProperties) -> str:
        return Path(config.database).stem


class DuckDBDataSourceImpl(DataSourceImpl, model_class=DuckDBDataSourceModel):
    def _create_sql_dialect(self) -> SqlDialect:
        return DuckDBSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return DuckDBDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    @classmethod
    def from_existing_cursor(cls, cursor: DuckDBPyConnection, name: str) -> DataSourceImpl:
        ds_model = DuckDBDataSourceModel(
            name=name,
            connection_properties=DuckDBExistingConnectionProperties(
                duckdb_connection=cursor,
            ),
        )
        soda_connection = DuckDBDataSourceConnection(
            name=name,
            connection_properties={},
            connection=DuckDBDataSourceConnectionWrapper(cursor),
        )
        return cls(data_source_model=ds_model, connection=soda_connection)
