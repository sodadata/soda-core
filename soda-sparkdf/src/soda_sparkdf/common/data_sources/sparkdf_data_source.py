from pyspark.sql import SparkSession
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl, MetadataTablesQuery
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import *
from soda_core.common.sql_dialect import SqlDialect
from soda_databricks.common.data_sources.databricks_data_source import (
    DatabricksSqlDialect,
)
from soda_databricks.common.statements.hive_metadata_tables_query import (
    HiveMetadataTablesQuery,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameConnectionProperties,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameDataSource as SparkDataFrameDataSourceModel,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameExistingSessionProperties,
    SparkDataFrameNewSessionProperties,
)
from tabulate import tabulate

_in_memory_connection = None


class SparkDataFrameCursor:
    def __init__(self, session: SparkSession):
        self._session = session

    # def __getattr__(self, attr):
    #     if attr in self.__dict__:
    #         return getattr(self, attr)
    #     return getattr(self._session, attr)

    def execute(self, sql: str):
        return self._session.sql(sql).collect()

    def close(self):
        # because a spark dataframe cursor is actually the current connection,
        # we don't want to close it
        pass

    @property
    def description(self):
        """
        Makes the cursor description available as a list of SparkDataFrameColumn namedtuples.
        This is to be compatible with the expected interface of a DBAPI cursor.
        """
        return [list(*col) for col in self._connection.description]  # TODO: implement this


class SparkDataFrameDataSourceConnectionWrapper:
    def __init__(self, session: SparkSession):
        self._session = session

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self._session, attr)

    def commit(self):
        pass  # Do nothing, Spark does not have a commit concept

    def cursor(self):
        return SparkDataFrameCursor(self._session)


class SparkDataFrameSqlDialect(DatabricksSqlDialect):
    SODA_DATA_TYPE_SYNONYMS = ()

    def get_database_prefix_index(self) -> int | None:
        return None

    def get_schema_prefix_index(self) -> int | None:
        return 0

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        schema_name: str = prefixes[0]
        quoted_schema_name: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {quoted_schema_name}" + (";" if add_semicolon else "")

    def post_schema_create_sql(self, prefixes: list[str]) -> Optional[list[str]]:
        pass  # Do nothing, Spark does not have a post-schema-create concept


class SparkDataFrameDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: dict, connection: Optional[object] = None):
        super().__init__(name, connection_properties, connection)

    def _create_connection(
        self,
        config: SparkDataFrameConnectionProperties,
    ):
        session = None
        if isinstance(config, SparkDataFrameExistingSessionProperties):
            session = config.spark_session
        elif isinstance(config, SparkDataFrameNewSessionProperties):
            session = (
                SparkSession.builder.master("local")
                .appName(self.name)
                .config("spark.sql.warehouse.dir", config.test_dir)
                .getOrCreate()
            )
        if session is None:
            raise ValueError("No session provided")
        self.session = session
        return SparkDataFrameDataSourceConnectionWrapper(session=session)

    def close_connection(self) -> None:
        "This is a no-op for SparkDataFrameDataSourceConnection, there is no connection to close."

    def execute_query(self, sql: str) -> QueryResult:
        logger.debug(
            f"SQL query fetchall in datasource {self.name} (first {self.MAX_CHARS_PER_SQL} chars): \n{self.truncate_sql(sql)}"
        )

        df = self.session.sql(sql)
        rows = df.collect()
        formatted_rows = self.format_rows(rows)
        truncated_rows = self.truncate_rows(formatted_rows)
        headers = [self._execute_query_get_result_row_column_name(c) for c in df.columns]
        table_text: str = tabulate(
            truncated_rows,
            headers=headers,
            tablefmt="github",
        )

        logger.debug(
            f"SQL query result (max {self.MAX_ROWS} rows, {self.MAX_CHARS_PER_STRING} chars per string):\n{table_text}"
        )
        return QueryResult(rows=formatted_rows, columns=df.columns)

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column  # No processing needed for SparkDataFrame


class SparkDataFrameDataSourceImpl(DataSourceImpl, model_class=SparkDataFrameDataSourceModel):
    def _create_sql_dialect(self) -> SqlDialect:
        return SparkDataFrameSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SparkDataFrameDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    @classmethod
    def from_existing_session(cls, session: SparkSession, name: str) -> DataSourceImpl:
        connection_properties = {"spark_session": session, "schema_": name}
        ds_model = SparkDataFrameDataSourceModel(
            name=name,
            connection_properties=connection_properties,
        )
        soda_connection = SparkDataFrameDataSourceConnection(
            name=name,
            connection_properties=connection_properties,
            connection=SparkDataFrameDataSourceConnectionWrapper(session),
        )
        return cls(data_source_model=ds_model, connection=soda_connection)

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return HiveMetadataTablesQuery(sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection)
