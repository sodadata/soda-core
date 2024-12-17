from __future__ import annotations

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


class CreateSchema:

    def __init__(self, sql_dialect: SqlDialect, data_source_connection: DataSourceConnection):
        self.sql_dialect = sql_dialect
        self.data_source_connection = data_source_connection
        self.database_name: str | None = None
        self.schema_name: str | None = None

    def with_database_name(self, database_name: str | None) -> CreateSchema:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> CreateSchema:
        self.schema_name = schema_name
        return self

    def build(self) -> str:
        schema_name_quoted: str = self.sql_dialect.quote_default(self.schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted} AUTHORIZATION CURRENT_USER;"

    def build(self) -> str:
        schema_name_quoted: str = self.sql_dialect.quote_default(self.schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted} {columnpart}AUTHORIZATION CURRENT_USER;"
