from datetime import datetime, timezone
from typing import Optional

from freezegun import freeze_time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl, MetadataTablesQuery
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import ColumnMetadata, SodaDataTypeName
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

_in_memory_connection = None


class SparkDataFrameCursor:
    # Copy from v3 of the cursor implementation
    def __init__(self, spark_session: SparkSession, test_dir: Optional[str] = None):
        self.spark_session = spark_session
        self.df: DataFrame | None = None
        self.description: tuple[tuple] | None = None
        self.rowcount: int = -1
        self.cursor_index: int = -1

    def execute(self, sql: str):
        self.df = self.spark_session.sql(sqlQuery=sql)
        self.description = self.convert_spark_df_schema_to_dbapi_description(self.df)
        self.cursor_index = 0

    def fetchall(self) -> tuple[tuple]:
        rows = []
        with freeze_time(
            datetime.now(timezone.utc)
        ):  # We need to freeze the time to UTC at the time of collecting to avoid issues with timestamps
            # Spark stores the timestamps in UTC (verified by reading the parquet file that spark creates), but when querying it converts it to the (python) session-local timezone.
            # By using freeze_time, we can ensure that the timestamps are collected in UTC, regardless of the session-local timezone. This plays nice with the freshness checks.
            spark_rows: list[Row] = self.df.collect()
        # Alternative approach: convert to PyArrow. This will set the timestamps correctly, but introduces memory and time overhead (for the conversion).
        # This also requires more changes regarding the cursor implementation: spark_rows: list[Row] = self.df.toArrow().to_pylist()
        self.rowcount = len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchmany(self, size: int) -> tuple[tuple]:
        rows = []
        self.rowcount = self.df.count()
        with freeze_time(
            datetime.now(timezone.utc)
        ):  # We need to freeze the time to UTC at the time of collecting to avoid issues with timestamps. See the comment in fetchall() for more details.
            spark_rows: list[Row] = self.df.offset(self.cursor_index).limit(size).collect()
        self.cursor_index += len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchone(self) -> tuple:
        with freeze_time(
            datetime.now(timezone.utc)
        ):  # We need to freeze the time to UTC at the time of collecting to avoid issues with timestamps. See the comment in fetchall() for more details.
            spark_rows: list[Row] = self.df.collect()
        self.rowcount = len(spark_rows)
        spark_row = spark_rows[0]
        row = self.convert_spark_row_to_dbapi_row(spark_row)
        return tuple(row)

    @staticmethod
    def convert_spark_row_to_dbapi_row(spark_row):
        return [spark_row[field] for field in spark_row.__fields__]

    def close(self):
        pass  # No-op

    @staticmethod
    def convert_spark_df_schema_to_dbapi_description(df) -> tuple[tuple]:
        return tuple((field.name, type(field.dataType).__name__) for field in df.schema.fields)


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
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.TIMESTAMP_TZ, SodaDataTypeName.TIMESTAMP),
    )

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

    def build_column_metadatas_from_query_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        # Filter out dataset description rows (first such line starts with #, ignore the rest) or empty
        filtered_rows = []
        for row in query_result.rows:
            if row[0].startswith("#"):  # ignore all description rows
                break
            if not row[0] and not row[1]:  # empty row
                continue

            filtered_rows.append(row)

        return super().build_column_metadatas_from_query_result(
            QueryResult(rows=filtered_rows, columns=query_result.columns)
        )

    def literal_datetime_with_tz(self, datetime: datetime):
        # Always convert the timestamp to utc when we insert. Spark is not aware of the timezones, so we need to do this conversion so it's ready to be extracted as UTC.
        return f"to_utc_timestamp('{datetime.isoformat()}', 'UTC')"

    def literal_datetime(self, datetime: datetime):
        # Always convert the timestamp to utc when we insert. Spark is not aware of the timezones, so we need to do this conversion so it's ready to be extracted as UTC.
        return f"to_utc_timestamp('{datetime.isoformat()}', 'UTC')"


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
            session.sql("SET spark.sql.session.timeZone = +00:00;")
            session.sql("SET TIME ZONE 'UTC';")
        if session is None:
            raise ValueError("No session provided")
        self.session = session
        return SparkDataFrameDataSourceConnectionWrapper(session=session)

    def close_connection(self) -> None:
        "This is a no-op for SparkDataFrameDataSourceConnection, there is no connection to close."

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]  # The first element of the tuple is the column name


class SparkDataFrameDataSourceImpl(DataSourceImpl, model_class=SparkDataFrameDataSourceModel):
    def _create_sql_dialect(self) -> SqlDialect:
        return SparkDataFrameSqlDialect(data_source_impl=self)

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SparkDataFrameDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return HiveMetadataTablesQuery(sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection)

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

    def build_columns_metadata_query_str(self, dataset_prefixes: list[str], dataset_name: str) -> str:
        if len(dataset_prefixes) == 0:
            return f"DESCRIBE {dataset_name}"
        elif len(dataset_prefixes) == 1:
            schema_name: str = dataset_prefixes[0]
            return f"DESCRIBE {schema_name}.{dataset_name}"
        elif len(dataset_prefixes) == 2:
            database_name: str = dataset_prefixes[0]
            schema_name: str = dataset_prefixes[1]
            return f"DESCRIBE {database_name}.{schema_name}.{dataset_name}"
        else:
            raise ValueError(f"Invalid number of dataset prefixes: {len(dataset_prefixes)}")

    def test_schema_exists(self, prefixes: list[str]) -> bool:
        result = self.connection.session.sql(f"SHOW SCHEMAS LIKE '{prefixes[0]}'").collect()
        for row in result:
            if row[0] and row[0].lower() == prefixes[0].lower():
                return True
        return False


# Alias to make the import and usage cleaner
SparkDataFrameDataSource = SparkDataFrameDataSourceImpl
