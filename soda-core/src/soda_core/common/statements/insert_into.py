from __future__ import annotations

from dataclasses import dataclass

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


@dataclass
class InsertIntoRow:
    # The values of a column in the data source specific literal syntax
    values: list[str]


class InsertInto:

    def __init__(self, sql_dialect: SqlDialect, data_source_connection: DataSourceConnection):
        self.sql_dialect = sql_dialect
        self.data_source_connection = data_source_connection
        self.database_name: str | None = None
        self.schema_name: str | None = None
        self.dataset_name: str | None = None
        self.literal_row_values: list[list[str]] | None = None

    def with_database_name(self, database_name: str | None) -> InsertInto:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> InsertInto:
        self.schema_name = schema_name
        return self

    def with_dataset_name(self, dataset_name: str | None) -> InsertInto:
        self.dataset_name = dataset_name
        return self

    def with_literal_row_values(self, literal_row_values: list[list[str]]) -> InsertInto:
        self.literal_row_values = literal_row_values
        return self

    def execute(self) -> None:
        sql: str = self._build_sql()
        self.data_source.data_source_connection.execute_update(sql)

    def _build_sql(self) -> str:
        table_name_qualified_quoted = self.data_source.sql_dialect.qualify_table(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=self.dataset_name
        )

        def format_literal_row_values(row: list[str]) -> str:
            return ",".join(row)

        rows_sql = ",\n".join(
            [
                f"  ({format_literal_row_values(row)})" for row in self.literal_row_values
            ]
        )

        return self._compose_insert_test_data_stmt(table_name_qualified_quoted, rows_sql)

    def _compose_insert_test_data_stmt(self, table_name_qualified_quoted, rows_sql):
        return f"INSERT INTO {table_name_qualified_quoted} VALUES \n" f"{rows_sql};"