from __future__ import annotations

from abc import ABC, abstractmethod

from soda.common.statements.create_schema import CreateSchema
from soda.common.statements.create_table import CreateTable
from soda.common.statements.drop_schema import DropSchema
from soda.common.statements.drop_table import DropTable
from soda.common.statements.insert_into import InsertInto
from soda.common.statements.table_name_query import TableNamesQuery
from soda.common.yaml import YamlFile


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
        from soda.contracts.data_sources.postgres_data_source import PostgresDataSource
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
        self.data_source_connection: DataSourceConnection = self._create_data_source_connection(
            name=self.name,
            connection_properties=self.connection_properties,
            logs=self.data_source_yaml_file.logs
        )

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
        self.data_source_connection.open_connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def close_connection(self) -> None:
        self.data_source_connection.close_connection()

    def create_create_schema(self) -> CreateSchema:
        return CreateSchema(self.data_source_connection, self.sql_dialect)

    def create_drop_schema(self) -> DropSchema:
        return DropSchema(self.data_source_connection, self.sql_dialect)

    def create_table_names_query(self) -> TableNamesQuery:
        return TableNamesQuery(self.data_source_connection, self.sql_dialect)

    def create_create_table(self) -> CreateTable:
        return CreateTable(self.data_source_connection, self.sql_dialect)

    def create_drop_table(self) -> DropTable:
        return DropTable(self.data_source_connection, self.sql_dialect)

    def create_insert_into(self) -> InsertInto:
        return InsertInto(self.data_source_connection, self.sql_dialect)
