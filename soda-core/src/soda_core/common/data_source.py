from __future__ import annotations

from abc import ABC, abstractmethod

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.yaml import YamlSource, YamlFileContent


class DataSource(ABC):

    @classmethod
    def create(
            cls,
            data_source_yaml_source: YamlSource,
            name: str,
            type_name: str,
            connection_properties: dict,
            variables: dict[str, str] | None
    ) -> DataSource:
        data_source_yaml_file_content: YamlFileContent = data_source_yaml_source.parse_yaml_file_content(
            file_type="data source", variables=variables
        )

        from soda_core.common.data_sources.postgres_data_source import PostgresDataSource
        return PostgresDataSource(
                data_source_yaml_file_content=data_source_yaml_file_content,
                name=name,
                type_name=type_name,
                connection_properties=connection_properties
            )

    def __init__(
            self,
            data_source_yaml_file_content: YamlFileContent,
            name: str,
            type_name: str,
            connection_properties: dict,
    ):
        self.data_source_yaml_file_content: YamlFileContent = data_source_yaml_file_content
        self.logs: Logs = data_source_yaml_file_content.logs
        self.name: str = name
        self.type_name: str = type_name
        self.sql_dialect: SqlDialect = self._create_sql_dialect()
        self.connection_properties: dict | None = connection_properties
        self.data_source_connection: DataSourceConnection | None = None

    @abstractmethod
    def _get_data_source_type_name(self) -> str:
        pass

    @abstractmethod
    def _create_data_source_connection(
            self,
            name: str,
            connection_properties: dict,
            logs: Logs
    ) -> DataSourceConnection:
        pass

    @abstractmethod
    def _create_sql_dialect(self) -> SqlDialect:
        pass

    def __enter__(self) -> None:
        self.open_connection()

    def open_connection(self) -> None:
        self.data_source_connection = self._create_data_source_connection(
            name=self.name,
            connection_properties=self.connection_properties,
            logs=self.data_source_yaml_file_content.logs
        )

    def has_open_connection(self) -> bool:
        return isinstance(self.data_source_connection, DataSourceConnection)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def close_connection(self) -> None:
        if self.data_source_connection:
            self.data_source_connection.close_connection()

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return MetadataTablesQuery(sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection)

    def create_metadata_columns_query(self) -> MetadataColumnsQuery:
        return MetadataColumnsQuery(sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection)

    def execute_query(self, sql: str) -> QueryResult:
        return self.data_source_connection.execute_query(sql=sql)

    def execute_update(self, sql: str) -> UpdateResult:
        return self.data_source_connection.execute_update(sql=sql)

    def get_max_aggregation_query_length(self) -> int:
        # What is the maximum query length of common analytical databases?
        # ChatGPT said:
        # Here are the maximum query lengths for some common analytical databases:
        # PostgreSQL: 1 GB
        # MySQL: 1 MB (configurable via max_allowed_packet)
        # SQL Server: 65,536 bytes (approximately 65 KB)
        # Oracle: 64 KB (depends on SQL string encoding)
        # Snowflake: 1 MB
        # BigQuery: No documented limit on query size, but practical limits on complexity and performance.
        return 63 * 1024 * 1024
