from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_columns_query import (
    ColumnMetadata,
    MetadataColumnsQuery,
)
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.yaml import YamlObject, YamlSource
from soda_core.contracts.contract_verification import DataSource

logger: logging.Logger = soda_logger


class DataSourceImpl(ABC):
    @classmethod
    def from_yaml_source(
        cls, data_source_yaml_source: YamlSource, variables: Optional[dict] = None
    ) -> Optional[DataSourceImpl]:
        assert isinstance(data_source_yaml_source, YamlSource)

        data_source_yaml_source.set_file_type("Data source")
        data_source_yaml_source.resolve(variables=variables)
        data_source_yaml: YamlObject = data_source_yaml_source.parse()
        if not data_source_yaml:
            return None

        data_source_type_name: str = data_source_yaml.read_string("type")
        data_source_name: Optional[str] = data_source_yaml.read_string("name")

        connection_yaml: YamlObject = data_source_yaml.read_object_opt("connection")
        connection_properties: Optional[dict] = None
        if connection_yaml:
            connection_properties = connection_yaml.to_dict()

        return DataSourceImpl.create(
            data_source_yaml_source=data_source_yaml_source,
            name=data_source_name,
            type_name=data_source_type_name,
            connection_properties=connection_properties,
        )

    @classmethod
    def create(
        cls,
        data_source_yaml_source: YamlSource,
        name: str,
        type_name: str,
        connection_properties: dict,
    ) -> DataSourceImpl:
        from soda_core.common.data_sources.postgres_data_source import (
            PostgresDataSourceImpl,
        )

        return PostgresDataSourceImpl(
            data_source_yaml_source=data_source_yaml_source,
            name=name,
            type_name=type_name,
            connection_properties=connection_properties,
        )

    def __init__(
        self,
        data_source_yaml_source: YamlSource,
        name: str,
        type_name: str,
        connection_properties: dict,
    ):
        self.data_source_yaml_source: YamlSource = data_source_yaml_source
        self.name: str = name
        self.type_name: str = type_name
        self.sql_dialect: SqlDialect = self._create_sql_dialect()
        self.connection_properties: Optional[dict] = connection_properties
        self.data_source_connection: Optional[DataSourceConnection] = None

    def __str__(self) -> str:
        return self.name

    @abstractmethod
    def get_data_source_type_name(self) -> str:
        pass

    @abstractmethod
    def _create_data_source_connection(self, name: str, connection_properties: dict) -> DataSourceConnection:
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
        )

    def has_open_connection(self) -> bool:
        return (
            isinstance(self.data_source_connection, DataSourceConnection)
            and self.data_source_connection.connection is not None
        )

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

    def is_different_data_type(self, expected_column: ColumnMetadata, actual_column_metadata: ColumnMetadata) -> bool:
        canonical_expected_data_type: str = self.get_canonical_data_type(expected_column.data_type)
        canonical_actual_data_type: str = self.get_canonical_data_type(actual_column_metadata.data_type)

        if canonical_expected_data_type != canonical_actual_data_type:
            return True

        if (
            isinstance(expected_column.character_maximum_length, int)
            and expected_column.character_maximum_length != actual_column_metadata.character_maximum_length
        ):
            return True

        return False

    def get_canonical_data_type(self, data_type: str) -> str:
        canonical_data_type: str = data_type.lower()
        canonical_data_type_mappings: dict = self.get_canonical_data_type_mappings()
        if canonical_data_type in canonical_data_type_mappings:
            canonical_data_type = canonical_data_type_mappings.get(canonical_data_type)
        return canonical_data_type

    def get_canonical_data_type_mappings(self) -> dict:
        return {"character varying": "varchar", "integer": "int"}

        # has_length: bool = bool(re.match(r"^[a-zA-Z0-9 ]+\(\d+\)$", expected_data_type_lower))
        # actual_data_type = self.get_data_type_text(column_metadata=actual_column_metadata, include_length=has_length)
        # return expected_data_type_lower == actual_data_type

    def get_data_type_text(self, column_metadata: ColumnMetadata, include_length: bool = True) -> str:
        data_type: str = column_metadata.data_type
        data_type = data_type.replace("character varying", "varchar")
        data_type = data_type.replace("integer", "int")
        if include_length and isinstance(column_metadata.character_maximum_length, int):
            data_type = f"{data_type}({column_metadata.character_maximum_length})"
        return data_type

    def get_format_regex(self, format: str) -> Optional[str]:
        if format is None:
            return None
        if self.format_regexes is None:
            logger.error(f"'format_regexes' not configured in data source")
        format_regex: Optional[str] = self.format_regexes.get(format)
        if format_regex is None:
            logger.error(f"Validity format regex '{format}' not configured " f"in data source 'format_regexes'")
        return format_regex

    def test_connection_error_message(self) -> Optional[str]:
        try:
            with self:
                self.data_source_connection.execute_query(f"SELECT 1")
                return None
        except Exception as e:
            return str(e)

    def build_data_source(self) -> DataSource:
        return DataSource(name=self.name, type=self.type_name)

    # Deprecated
    def validate_dataset_qualified_name(self, dsn_parts: list[str]):
        if not isinstance(dsn_parts, list) or len(dsn_parts) != 4:
            logger.error(f"Invalid dataset qualified name: {dsn_parts}")
