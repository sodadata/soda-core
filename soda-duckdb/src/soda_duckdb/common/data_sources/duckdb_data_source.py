from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_dialect import SqlDialect
from soda_duckdb.model.data_source.duckdb_data_source import (
    DuckDBDataSource as DuckDBDataSourceModel,
)

from soda_duckdb.model.data_source.duckdb_data_source import DuckDBDataSource
from soda_duckdb.model.data_source.duckdb_connection_properties import (
    DuckDBConnectionProperties,
    DuckDBStandardConnectionProperties,
    DuckDBExistingConnectionProperties,
)
from soda_core.model.data_source.data_source_connection_properties import DataSourceConnectionProperties
from pathlib import Path

from soda_core.common.exceptions import DataSourceConnectionException
from collections import namedtuple

from duckdb import DuckDBPyConnection

from soda_core.common.sql_ast import *


DuckDBColumn = namedtuple(
    "DuckDBColumn", ["name", "type_code", "display_size", "internal_size", "precision", "scale", "null_ok"]
)


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

    def supports_varchar_length(self):
        """
        From docs: "Variable-length character string. The maximum length n has no effect and is only provided for compatibility"
        """
        return False

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_MATCHES({expression}, '{matches.regex_pattern}')"


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
                        f"CREATE TABLE {self.extract_dataset_name(config)} AS SELECT * FROM {read_function}('{self.database}')"
                    )

                    return connection
                else:
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
        ds_model = DuckDBDataSource(
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
