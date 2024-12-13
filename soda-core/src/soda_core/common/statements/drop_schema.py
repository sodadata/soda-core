from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


class DropSchema:

    def __init__(self, data_source: DataSource):
        self.data_source: DataSource = data_source
        self.database_name: str | None = None
        self.schema_name: str | None = None

    def with_database_name(self, database_name: str | None) -> DropSchema:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> DropSchema:
        self.schema_name = schema_name
        return self

    def execute(self) -> None:
        sql: str = self._build_sql()
        self.data_source.data_source_connection.execute_update(sql)

    def _build_sql(self) -> str:
        schema_name_quoted: str = self.data_source.sql_dialect.quote_default(self.schema_name)
        return f"DROP SCHEMA IF EXISTS {schema_name_quoted} CASCADE;"
