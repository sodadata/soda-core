from __future__ import annotations

from dataclasses import dataclass

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


@dataclass
class CreateTableColumn:
    column_name: str
    data_type: str


class CreateTable:

    def __init__(self, data_source: DataSource):
        self.data_source: DataSource = data_source
        self.database_name: str | None = None
        self.schema_name: str | None = None
        self.dataset_name: str | None = None
        self.columns: list[CreateTableColumn] | None = None

    def with_database_name(self, database_name: str | None) -> CreateTable:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> CreateTable:
        self.schema_name = schema_name
        return self

    def with_dataset_name(self, dataset_name: str | None) -> CreateTable:
        self.dataset_name = dataset_name
        return self

    def with_columns(self, columns: list[CreateTableColumn]) -> CreateTable:
        self.columns = columns
        return self

    def execute(self) -> None:
        sql: str = self._build_sql()
        self.data_source.data_source_connection.execute_update(sql)

    def _build_sql(self) -> str:
        table_name_qualified_quoted: str = self.data_source.sql_dialect.qualify_table(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=self.dataset_name
        )

        columns_sql: str = ",\n".join(
            [
                f"  {column.column_name} {column.data_type}"
                for column in self.columns
            ]
        )
        return self._compose_create_table_statement(table_name_qualified_quoted, columns_sql)

    def _compose_create_table_statement(self, table_name_qualified_quoted: str, columns_sql: str) -> str:
        return f"CREATE TABLE {table_name_qualified_quoted} ( \n{columns_sql} \n);"
