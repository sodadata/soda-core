from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


class DropTable:

    def __init__(self, data_source: DataSource):
        self.data_source: DataSource = data_source
        self.database_name: str | None = None
        self.schema_name: str | None = None
        self.dataset_name: str | None = None

    def with_database_name(self, database_name: str | None) -> DropTable:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> DropTable:
        self.schema_name = schema_name
        return self

    def with_dataset_name(self, dataset_name: str | None) -> DropTable:
        self.dataset_name = dataset_name
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

        return self._compose_drop_table_statement(table_name_qualified_quoted)

    def _compose_drop_table_statement(self, table_name_qualified_quoted: str) -> str:
        return f"DROP TABLE {table_name_qualified_quoted};"
