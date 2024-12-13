from __future__ import annotations

from abc import ABC, abstractmethod

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.create_schema import CreateSchema
from soda_core.common.statements.create_table import CreateTable
from soda_core.common.statements.drop_schema import DropSchema
from soda_core.common.statements.drop_table import DropTable
from soda_core.common.statements.insert_into import InsertInto
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.yaml import YamlFile


class DataSource(ABC):

    @classmethod
    def create(
            cls,
            data_source_yaml_file: YamlFile,
            name: str,
            type_name: str,
            connection_properties: dict,
            spark_session: object | None
    ) -> DataSource:
        from soda_postgres.common.data_sources.postgres_data_source_connection import PostgresDataSource
        return PostgresDataSource(
                data_source_yaml_file=data_source_yaml_file,
                name=name,
                type_name=type_name,
                connection_properties=connection_properties
            )

    def __init__(
            self,
            data_source_yaml_file: YamlFile,
            name: str,
            type_name: str,
            connection_properties: dict,
    ):
        self.data_source_yaml_file: YamlFile = data_source_yaml_file
        self.name: str = name
        self.type_name: str = type_name
        self.sql_dialect = self._create_sql_dialect()
        self.logs: Logs = self.data_source_yaml_file.logs
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
            logs=self.data_source_yaml_file.logs
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def close_connection(self) -> None:
        if self.data_source_connection:
            self.data_source_connection.close_connection()

    def create_create_schema(self) -> CreateSchema:
        return self.sql_dialect.create_create_schema(self)

    def create_drop_schema(self) -> DropSchema:
        return self.sql_dialect.create_drop_schema(self)

    def create_create_table(self) -> CreateTable:
        return self.sql_dialect.create_create_table(self)

    def create_drop_table(self) -> DropTable:
        return self.sql_dialect.create_drop_table(self)

    def create_insert_into(self) -> InsertInto:
        return self.sql_dialect.create_insert_into(self)

    def create_table_names_query(self) -> MetadataTablesQuery:
        return self.sql_dialect.create_metadata_tables_query(self)
