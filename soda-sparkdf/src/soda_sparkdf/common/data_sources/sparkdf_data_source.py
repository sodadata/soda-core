from datetime import datetime, timezone, tzinfo
from typing import Any, Optional

from freezegun import freeze_time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    parse_session_timezone,
)
from soda_core.common.data_source_impl import DataSourceImpl, MetadataTablesQuery
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import ColumnMetadata, SodaDataTypeName
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import FullyQualifiedTableName
from soda_core.common.statements.table_types import FullyQualifiedViewName, TableType
from soda_databricks.common.data_sources.databricks_data_source import (
    DatabricksSqlDialect,
)
from soda_databricks.common.statements.hive_metadata_tables_query import (
    HiveMetadataTablesQuery,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameActiveSessionProperties,
    SparkDataFrameConnectionProperties,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameDataSource as SparkDataFrameDataSourceModel,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameExistingSessionProperties,
    SparkDataFrameNewSessionProperties,
    SparkDataFrameRemoteSessionProperties,
)

_in_memory_connection = None


class SparkDataFrameCursor:
    CACHE_ROW_COUNT = 100

    def __init__(self, spark_session: SparkSession, test_dir: Optional[str] = None):
        self.spark_session = spark_session
        self.df: DataFrame | None = None
        self.description: tuple[tuple] | None = None
        self.cursor_index: int = -1
        self._cache_index = -1
        self._cached_rows: list[Row] | None = None

    def execute(self, sql: str):
        self.df = self.spark_session.sql(sqlQuery=sql)
        self.description = self.convert_spark_df_schema_to_dbapi_description(self.df)
        self.cursor_index = 0
        self._cache_index = -1
        self._cached_rows = None

    @property
    def rowcount(self) -> int:
        if self.df is None:
            return -1
        return self.df.count()

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
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchmany(self, size: int) -> tuple[tuple]:
        rows = []
        with freeze_time(
            datetime.now(timezone.utc)
        ):  # We need to freeze the time to UTC at the time of collecting to avoid issues with timestamps. See the comment in fetchall() for more details.
            spark_rows: list[Row] = self.df.offset(self.cursor_index).limit(size).collect()
        self.cursor_index += len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchone(self) -> tuple | None:
        # Fetches have overhead, so we load and cache small pages here as a compromise
        if self._cached_rows is None or self.cursor_index >= self._cache_index + self.CACHE_ROW_COUNT:
            with freeze_time(
                datetime.now(timezone.utc)
            ):  # We need to freeze the time to UTC at the time of collecting to avoid issues with timestamps. See the comment in fetchall() for more details.
                self._cached_rows = self.df.offset(self.cursor_index).limit(self.CACHE_ROW_COUNT).collect()
                self._cache_index = self.cursor_index
        access_index = self.cursor_index - self._cache_index
        if not self._cached_rows or access_index >= len(self._cached_rows):
            return None
        spark_row = self._cached_rows[access_index]
        self.cursor_index += 1
        row = self.convert_spark_row_to_dbapi_row(spark_row)
        return tuple(row)

    @staticmethod
    def convert_spark_row_to_dbapi_row(spark_row):
        return [spark_row[field] for field in spark_row.__fields__]

    def close(self):
        pass  # No-op

    @staticmethod
    def convert_spark_df_schema_to_dbapi_description(df) -> tuple[tuple]:
        # simpleString() yields Spark SQL type names like "int", "decimal(10,2)", "string" —
        # parseable by the DWH extension and aligned with the dialect's supported-type list.
        return tuple((field.name, field.dataType.simpleString()) for field in df.schema.fields)


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


class SparkDataFrameSqlDialect(DatabricksSqlDialect, sqlglot_dialect="spark"):
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.TIMESTAMP_TZ, SodaDataTypeName.TIMESTAMP),
    )
    # Class-level default: legacy local-Spark rejects ``DROP TABLE ... CASCADE``
    # with a ParseException. In catalog mode (Databricks) CASCADE is supported, so
    # __init__ promotes this to True via an instance attribute that shadows the class.
    SUPPORTS_DROP_TABLE_CASCADE: bool = False

    def __init__(self, use_catalog: bool = False):
        super().__init__()
        # In catalog mode, prefixes are [catalog, schema] (Unity-Catalog style 3-level
        # namespace). In legacy mode, prefixes are [schema] only — local Spark has no
        # catalog concept beyond ``spark_catalog``.
        self.use_catalog = use_catalog
        # Instance override — see class-level comment above.
        self.SUPPORTS_DROP_TABLE_CASCADE = use_catalog

    def get_database_prefix_index(self) -> int | None:
        return 0 if self.use_catalog else None

    def get_schema_prefix_index(self) -> int | None:
        return 1 if self.use_catalog else 0

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        if self.use_catalog:
            catalog_name: str = prefixes[0]
            schema_name: str = prefixes[1]
            quoted = f"{self.quote_default(catalog_name)}.{self.quote_default(schema_name)}"
        else:
            quoted = self.quote_default(prefixes[0])
        return f"CREATE SCHEMA IF NOT EXISTS {quoted}" + (";" if add_semicolon else "")

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
        # When the caller supplies a pre-built ``connection``, the base ``open_connection`` skips
        # ``_create_connection`` (which is where ``self.session`` would normally be assigned).
        # Pull the session out of ``connection_properties`` up-front so ``self.session`` is
        # always available — DWH calls like ``test_schema_exists`` rely on it.
        self.session: Optional[SparkSession] = None
        if isinstance(connection_properties, dict):
            existing_session = connection_properties.get("spark_session")
            if existing_session is not None:
                self.session = existing_session
        super().__init__(name, connection_properties, connection)

    def _create_connection(
        self,
        config: SparkDataFrameConnectionProperties,
    ):
        session = None
        if isinstance(config, SparkDataFrameExistingSessionProperties):
            session = config.spark_session
        elif isinstance(config, SparkDataFrameActiveSessionProperties):
            # Pick up the thread-local active SparkSession (the Databricks notebook's
            # ``spark``, or whatever the caller last did ``.getOrCreate()`` on). No URI,
            # no credentials, no global state we own — just pyspark's existing notion
            # of which session is active in this thread.
            session = SparkSession.getActiveSession()
            if session is None:
                raise ValueError(
                    "SparkDataFrame is configured with use_active_session=True but no active "
                    "SparkSession was found. Build a session (e.g. SparkSession.builder.…"
                    "getOrCreate()) before opening this connection, or switch to the "
                    "existing_session / remote / new_session connection mode."
                )
        elif isinstance(config, SparkDataFrameRemoteSessionProperties):
            # Spark Connect URI. ``token`` becomes a gRPC bearer header (handled by
            # pyspark.sql.connect.ChannelBuilder); ``x-databricks-cluster-id`` is forwarded
            # as gRPC metadata, which is how Databricks routes the session to a cluster.
            # ``getOrCreate`` caches per URI in-process, so multiple data sources pointing
            # at the same workspace+cluster end up sharing one underlying session.
            # NB: ``config.token`` is a SecretStr — unwrap only when assembling the URI
            # (which is kept as a local variable, never logged).
            uri = (
                f"sc://{config.host}:443/"
                f";use_ssl=true"
                f";token={config.token.get_secret_value()}"
                f";x-databricks-cluster-id={config.cluster_id}"
            )
            session = SparkSession.builder.remote(uri).getOrCreate()
            session.sql("SET TIME ZONE 'UTC'")
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

    def _fetch_session_timezone(self) -> tzinfo:
        # New sessions created by this adapter explicitly ``SET TIME ZONE 'UTC';``,
        # but ``SparkDataFrameExistingSessionProperties`` lets a caller wrap an existing
        # SparkSession that may have any configured zone. Read the live setting so the
        # value mappers see the same zone the Spark engine will use to interpret
        # naive returns. ``parse_session_timezone`` accepts Spark's reported value
        # ('UTC', '+00:00', 'America/Los_Angeles', etc.) and the connection-level
        # wrapper falls back to UTC if the call raises.
        tz_value = self.session.conf.get("spark.sql.session.timeZone")
        return parse_session_timezone(tz_value)

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]  # The first element of the tuple is the column name

    def _cursor_execute_update_and_commit(self, cursor: Any, sql: str) -> int:
        cursor.execute(sql)
        # Skip cursor.rowcount for Spark — it triggers df.count() which runs a full Spark job.
        self.commit()
        return 0


class SparkDataFrameMetadataTablesQuery(HiveMetadataTablesQuery):
    """SparkDF variant of HiveMetadataTablesQuery.

    Handles two SparkDF-specific quirks:
      1. SHOW TABLES / SHOW VIEWS throws AnalysisException when the schema doesn't exist
         (the Databricks SQL connector returns empty). Wrap each call in try/except so DWH
         introspection of the not-yet-created diagnostics schema doesn't crash.
      2. SHOW TABLES in Spark returns both tables and views; collect view names first and
         subtract them so TableType.TABLE results are disjoint from TableType.VIEW.

    Also emits ``database_name=None`` since SparkDF has no catalog concept (the dialect
    uses ``database_prefix_index=None``).
    """

    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
        types_to_return: Optional[list[TableType]] = None,
    ) -> list[FullyQualifiedTableName]:
        if types_to_return is None:
            types_to_return = [TableType.TABLE]
        results: list[FullyQualifiedTableName] = []

        view_names: set[str] = set()
        if TableType.TABLE in types_to_return:
            try:
                view_sql = self.build_sql_statement(
                    database_name=database_name, schema_name=schema_name, object_type_to_fetch=TableType.VIEW
                )
                view_result: QueryResult = self.data_source_connection.execute_query(view_sql)
                view_names = {row[1] for row in view_result.rows}
            except Exception:
                pass  # Schema may not exist yet

        if TableType.TABLE in types_to_return:
            try:
                sql = self.build_sql_statement(
                    database_name=database_name, schema_name=schema_name, object_type_to_fetch=TableType.TABLE
                )
                query_result: QueryResult = self.data_source_connection.execute_query(sql)
                filtered_rows = [row for row in query_result.rows if row[1] not in view_names]
                filtered_result = QueryResult(rows=filtered_rows, columns=query_result.columns)
                results.extend(
                    self.get_results(
                        filtered_result,
                        object_type_to_fetch=TableType.TABLE,
                        include_table_name_like_filters=include_table_name_like_filters,
                        exclude_table_name_like_filters=exclude_table_name_like_filters,
                    )
                )
            except Exception:
                pass

        if TableType.VIEW in types_to_return:
            try:
                sql = self.build_sql_statement(
                    database_name=database_name, schema_name=schema_name, object_type_to_fetch=TableType.VIEW
                )
                query_result = self.data_source_connection.execute_query(sql)
                results.extend(
                    self.get_results(
                        query_result,
                        object_type_to_fetch=TableType.VIEW,
                        include_table_name_like_filters=include_table_name_like_filters,
                        exclude_table_name_like_filters=exclude_table_name_like_filters,
                    )
                )
            except Exception:
                pass

        return results

    def get_results(
        self,
        query_result: QueryResult,
        object_type_to_fetch: TableType,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list[FullyQualifiedTableName]:
        if object_type_to_fetch == TableType.TABLE:
            names_for_filtering = [table_name for _, table_name, _ in query_result.rows]
        elif object_type_to_fetch == TableType.VIEW:
            names_for_filtering = [view_name for _, view_name, *_ in query_result.rows]
        else:
            raise ValueError(f"Invalid object type to fetch: {object_type_to_fetch}")
        filtered_names = self._filter_include_exclude(
            names_for_filtering, include_table_name_like_filters, exclude_table_name_like_filters
        )

        if object_type_to_fetch == TableType.TABLE:
            return [
                FullyQualifiedTableName(database_name=None, schema_name=schema_name, table_name=table_name)
                for schema_name, table_name, _is_temporary in query_result.rows
                if table_name in filtered_names
            ]
        elif object_type_to_fetch == TableType.VIEW:
            return [
                FullyQualifiedViewName(database_name=None, schema_name=schema_name, view_name=view_name)
                for schema_name, view_name, *_ in query_result.rows
                if view_name in filtered_names
            ]
        else:
            raise ValueError(f"Invalid object type to fetch: {object_type_to_fetch}")


class SparkDataFrameDataSourceImpl(DataSourceImpl, model_class=SparkDataFrameDataSourceModel):
    def _create_sql_dialect(self) -> SqlDialect:
        return SparkDataFrameSqlDialect(use_catalog=self._read_use_catalog_flag())

    def _read_use_catalog_flag(self) -> bool:
        # Pydantic model when fully parsed, dict during the from_existing_session bootstrap.
        props = self.data_source_model.connection_properties
        if isinstance(props, dict):
            return bool(props.get("use_catalog", False))
        return bool(getattr(props, "use_catalog", False))

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SparkDataFrameDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return SparkDataFrameMetadataTablesQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
        )

    @classmethod
    def from_existing_session(cls, session: SparkSession, name: str, use_catalog: bool = False) -> DataSourceImpl:
        # Locally-owned dict so we never mutate caller-shared state via the
        # ``connection_properties`` plumbing.
        connection_properties = {
            "spark_session": session,
            "schema_": name,
            "use_catalog": use_catalog,
        }
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

    @property
    def bulk_columns_metadata_available(self) -> bool:
        return False

    def test_schema_exists(self, prefixes: list[str]) -> bool:
        # Catalog-mode prefixes are [catalog, schema]; we list schemas in the catalog and do
        # exact-match in Python rather than ``LIKE '<schema>'`` so a schema named
        # ``soda_diagnostics`` doesn't false-positive against ``sodaXdiagnostics`` via the
        # ``_`` wildcard. Quote the catalog so names with hyphens (legal in UC) parse cleanly.
        if self.sql_dialect.get_database_prefix_index() is not None and len(prefixes) >= 2:
            catalog_name = prefixes[0]
            schema_name = prefixes[1]
            quoted_catalog = self.sql_dialect.quote_default(catalog_name)
            try:
                result = self.connection.session.sql(f"SHOW SCHEMAS IN {quoted_catalog}").collect()
            except Exception:
                # Catalog doesn't exist (or we can't see into it) — the subsequent
                # ``CREATE SCHEMA IF NOT EXISTS <cat>.<schema>`` will surface the real error.
                return False
            for row in result:
                if row[0] and row[0].lower() == schema_name.lower():
                    return True
            return False
        result = self.connection.session.sql("SHOW SCHEMAS").collect()
        for row in result:
            if row[0] and row[0].lower() == prefixes[0].lower():
                return True
        return False


# Alias to make the import and usage cleaner
SparkDataFrameDataSource = SparkDataFrameDataSourceImpl
