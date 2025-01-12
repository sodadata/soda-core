from __future__ import annotations

import re
from abc import ABC, abstractmethod

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery, ColumnMetadata
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
            variables: dict[str, str] | None,
            format_regexes: dict[str, str]
    ) -> DataSource:
        data_source_yaml_file_content: YamlFileContent = data_source_yaml_source.parse_yaml_file_content(
            file_type="data source", variables=variables
        )

        from soda_core.common.data_sources.postgres_data_source import PostgresDataSource
        return PostgresDataSource(
                data_source_yaml_file_content=data_source_yaml_file_content,
                name=name,
                type_name=type_name,
                connection_properties=connection_properties,
                format_regexes=format_regexes
            )

    def __init__(
            self,
            data_source_yaml_file_content: YamlFileContent,
            name: str,
            type_name: str,
            connection_properties: dict,
            format_regexes: dict[str, str],
    ):
        self.data_source_yaml_file_content: YamlFileContent = data_source_yaml_file_content
        self.logs: Logs = data_source_yaml_file_content.logs
        self.name: str = name
        self.type_name: str = type_name
        self.sql_dialect: SqlDialect = self._create_sql_dialect()
        self.connection_properties: dict | None = connection_properties
        self.data_source_connection: DataSourceConnection | None = None
        self.format_regexes: dict[str, str] = format_regexes

    @abstractmethod
    def get_data_source_type_name(self) -> str:
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

    def build_dataset_prefix(self, data_source_location: dict[str, str] | None) -> list[str] | None:
        if isinstance(data_source_location, dict):
            if "database" not in data_source_location:
                self.logs.error(f"For {self.get_data_source_type_name()}, 'database' is required in 'data_source_location'")
            if "schema" not in data_source_location:
                self.logs.error(f"For {self.get_data_source_type_name()}, 'schema' is required in 'data_source_location'")
            return [data_source_location.get("database"), data_source_location.get("schema")]

    def is_data_type_equal(self, expected_data_type: str, actual_column_metadata: ColumnMetadata) -> bool:
        expected_data_type_lower: str = expected_data_type.lower()
        expected_data_type_lower = expected_data_type_lower.replace("character varying", "varchar")
        expected_data_type_lower = expected_data_type_lower.replace("integer", "int")
        has_length: bool = bool(re.match(r"^[a-zA-Z0-9 ]+\(\d+\)$", expected_data_type_lower))
        actual_data_type = self.get_data_type_text(column_metadata=actual_column_metadata, include_length=has_length)
        return expected_data_type_lower == actual_data_type

    def get_data_type_text(self, column_metadata: ColumnMetadata, include_length: bool = True) -> str:
        data_type: str = column_metadata.data_type
        data_type = data_type.replace("character varying", "varchar")
        data_type = data_type.replace("integer", "int")
        if include_length and isinstance(column_metadata.max_length, int):
            data_type = f"{data_type}({column_metadata.max_length})"
        return data_type

    def get_format_regex(self, format: str) -> str | None:
        if format is None:
            return None
        if self.format_regexes is None:
            self.logs.error("'format_regexes' not configured in data source")
        format_regex: str | None = self.format_regexes.get(format)
        if format_regex is None:
            self.logs.error(f"Validity format regex '{format}' not configured in data source 'format_regexes'")
        return format_regex
