from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


class CreateSchema:

    def __init__(self, data_source: DataSource):
        self.data_source: DataSource = data_source
        self.database_name: str | None = None
        self.schema_name: str | None = None

    def with_database_name(self, database_name: str | None) -> CreateSchema:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> CreateSchema:
        self.schema_name = schema_name
        return self

    def _build_sql(self) -> str:
        schema_name_quoted: str = self.sql_dialect.quote_default(self.schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted} AUTHORIZATION CURRENT_USER;"

    def execute(self, data_source: DataSource) -> None:
        sql: str = self._build_sql()
        data_source.data_source_connection.execute_update(sql)
