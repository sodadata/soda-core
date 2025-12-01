from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Dict, Optional, Type

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.exceptions import DataSourceConnectionException
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import (
    ColumnMetadata,
    DataSourceNamespace,
    DbSchemaDataSourceNamespace,
    SchemaDataSourceNamespace,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import (
    FullyQualifiedTableName,
    MetadataTablesQuery,
)
from soda_core.common.yaml import DataSourceYamlSource, YamlObject
from soda_core.contracts.contract_verification import DataSource
from soda_core.model.data_source.data_source import DataSourceBase

logger: logging.Logger = soda_logger

if TYPE_CHECKING:
    from soda_core.contracts.impl.contract_verification_impl import ContractImpl


class DataSourceImpl(ABC):
    __implementation_classes: Dict[str, Callable[[], Type["DataSourceImpl"]]] = {}
    __model_classes: Dict[str, Type[DataSourceBase]] = {}

    @classmethod
    def from_yaml_source(
        cls, data_source_yaml_source: DataSourceYamlSource, provided_variable_values: Optional[dict] = None
    ) -> Optional[DataSourceImpl]:
        data_source_yaml_source.resolve(variables=provided_variable_values)
        data_source_yaml: YamlObject = data_source_yaml_source.parse()
        if not data_source_yaml:
            return None

        type_name = data_source_yaml.yaml_dict.get("type")
        if not type_name:
            raise ValueError("Missing required 'type' in data source YAML")

        impl_class = cls.__implementation_classes.get(type_name)
        if not impl_class:
            raise ImportError(
                f"Data source type '{type_name}' not available. "
                f"Make sure to install the required plugin, e.g. `pip install soda-{type_name}`"
            )

        model_class = cls.__model_classes.get(type_name)
        if not model_class:
            raise ImportError(
                f"Model class for data source type '{type_name}' not found. "
                f"This is likely a bug in the plugin implementation."
            )

        validated_model = model_class.model_validate(data_source_yaml.yaml_dict)
        return impl_class(data_source_model=validated_model)

    def __init__(
        self,
        data_source_model: DataSourceBase,
        connection: Optional[DataSourceConnection] = None,
    ):
        self.data_source_model: DataSourceBase = data_source_model
        self.name: str = data_source_model.name
        self.type_name: str = data_source_model.get_class_type()
        self.sql_dialect: SqlDialect = self._create_sql_dialect()
        self.data_source_connection: Optional[DataSourceConnection] = connection

    def __init_subclass__(cls, model_class: Type[DataSourceBase], **kwargs):
        super().__init_subclass__(**kwargs)
        type_name = model_class.get_class_type()
        cls.__model_classes[type_name] = model_class
        cls.__implementation_classes[type_name] = cls

    def create_copy_with_different_connection(self, connection: DataSourceConnection) -> DataSourceImpl:
        "Use this method when you want to use the DataSourceImpl while having multiple connections open. Can be used for bulk inserts for example."
        return self.__class__(data_source_model=self.data_source_model, connection=connection)

    def __str__(self) -> str:
        return self.name

    @abstractmethod
    def _create_data_source_connection(self) -> DataSourceConnection:
        pass

    @abstractmethod
    def _create_sql_dialect(self) -> SqlDialect:
        pass

    def __enter__(self) -> None:
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    @property
    def connection(self) -> DataSourceConnection:
        if not self.has_open_connection():
            self.open_connection()

        return self.data_source_connection

    def open_connection(self) -> None:
        try:
            self.data_source_connection = self._create_data_source_connection()
        except Exception as e:
            raise DataSourceConnectionException(e) from e

    def has_open_connection(self) -> bool:
        return (
            isinstance(self.data_source_connection, DataSourceConnection)
            and self.data_source_connection.connection is not None
        )

    def close_connection(self) -> None:
        if self.has_open_connection():
            self.data_source_connection.close_connection()

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        return self.connection.execute_query(sql=sql, log_query=log_query)

    def execute_query_one_by_one(
        self, sql: str, row_callback: Callable[[tuple, tuple[tuple]], None], log_query: bool = True
    ) -> tuple[tuple]:
        # Returns the description of the query.
        return self.data_source_connection.execute_query_one_by_one(
            sql=sql, row_callback=row_callback, log_query=log_query
        )

    def execute_update(self, sql: str, log_query: bool = True) -> UpdateResult:
        return self.connection.execute_update(sql=sql, log_query=log_query)

    def test_connection_error_message(self) -> Optional[str]:
        try:
            with self:
                self.data_source_connection.execute_query(f"SELECT 1")
                return None
        except Exception as e:
            return str(e)

    def build_data_source(self) -> DataSource:
        return DataSource(name=self.name, type=self.type_name)

    # TODO refactor to method here and delegate query building and result extraction to SqlDialect similar to get_columns_metadata
    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return MetadataTablesQuery(sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection)

    def parse_column_names_from_query_result(self, query_result: QueryResult) -> list[str]:
        return [self.connection._execute_query_get_result_row_column_name(column) for column in query_result.columns]

    def get_columns_metadata(self, dataset_prefixes: list[str], dataset_name: str) -> list[ColumnMetadata]:
        sql: str = self.build_columns_metadata_query_str(dataset_prefixes=dataset_prefixes, dataset_name=dataset_name)
        query_result: QueryResult = self.execute_query(sql)
        return self.sql_dialect.build_column_metadatas_from_query_result(query_result)

    def build_columns_metadata_query_str(self, dataset_prefixes: list[str], dataset_name: str) -> str:
        schema_name: Optional[str] = self.extract_schema_from_prefix(dataset_prefixes)
        database_name: Optional[str] = self.extract_database_from_prefix(dataset_prefixes)

        table_namespace: DataSourceNamespace = (
            SchemaDataSourceNamespace(schema=schema_name)
            if database_name is None
            else DbSchemaDataSourceNamespace(database=database_name, schema=schema_name)
        )

        # BigQuery must be able to override to get the location
        return self.sql_dialect.build_columns_metadata_query_str(
            table_namespace=table_namespace, table_name=dataset_name
        )

    def extract_schema_from_prefix(self, prefixes: list[str]) -> Optional[str]:
        schema_index: int | None = self.sql_dialect.get_schema_prefix_index()
        if schema_index is None:
            return None
        schema_name: str = prefixes[schema_index] if schema_index < len(prefixes) else None
        return schema_name

    def extract_database_from_prefix(self, prefixes: list[str]) -> Optional[str]:
        database_index: int | None = self.sql_dialect.get_database_prefix_index()
        if database_index is None:
            return None
        database_name: str = prefixes[database_index] if database_index < len(prefixes) else None
        return database_name

    def _build_table_namespace_for_schema_query(self, prefixes: list[str]) -> tuple[DataSourceNamespace, str]:
        """
        Builds the table namespace for the schema query.
        Returns the table namespace and the schema name.
        """
        schema_name: Optional[str] = self.extract_schema_from_prefix(prefixes)
        database_name: str | None = self.extract_database_from_prefix(prefixes)
        if schema_name is None:
            raise ValueError(f"Cannot determine schema name from prefixes: {prefixes}")

        table_namespace: DataSourceNamespace = (
            SchemaDataSourceNamespace(schema=schema_name)
            if database_name is None
            else DbSchemaDataSourceNamespace(
                database=database_name,
                schema=schema_name,
            )
        )
        return table_namespace, schema_name

    def test_schema_exists(self, prefixes: list[str]) -> bool:
        table_namespace, schema_name = self._build_table_namespace_for_schema_query(prefixes=prefixes)

        # Query all schemas to check if the target schema exists
        schemas_query_sql: str = self.sql_dialect.build_schemas_metadata_query_str(
            table_namespace=table_namespace, filter_on_schema_name=schema_name
        )
        query_result: QueryResult = self.execute_query(schemas_query_sql)

        # Check if schema exists in the results
        schema_exists: bool = False
        schema_name_lower: str = schema_name.lower()
        for row in query_result.rows:
            if row[0] and row[0].lower() == schema_name_lower:
                schema_exists = True
                break
        return schema_exists

    def verify_if_table_exists(self, prefixes: list[str], table_name: str) -> bool:
        fully_qualified_table_names: list[FullyQualifiedTableName] = self._get_fully_qualified_table_names(
            prefixes=prefixes, table_name=table_name
        )
        return any(
            fully_qualified_table_name.table_name == table_name
            for fully_qualified_table_name in fully_qualified_table_names
        )

    def _get_fully_qualified_table_names(self, prefixes: list[str], table_name: str) -> list[FullyQualifiedTableName]:
        metadata_tables_query: MetadataTablesQuery = self.create_metadata_tables_query()
        database_name = self.extract_database_from_prefix(prefixes)
        schema_name = self.extract_schema_from_prefix(prefixes)

        fully_qualified_table_names: list[FullyQualifiedTableName] = metadata_tables_query.execute(
            database_name=database_name,
            schema_name=schema_name,
            include_table_name_like_filters=[table_name],
        )
        return fully_qualified_table_names

    def switch_warehouse(self, warehouse: str, contract_impl: ContractImpl) -> None:
        # Noop by default, only some data sources need to implement this
        pass

    def get_current_warehouse(self) -> Optional[str]:
        # Noop by default, only some data sources need to implement this
        return None
