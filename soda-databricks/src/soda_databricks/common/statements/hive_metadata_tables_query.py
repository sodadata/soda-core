from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import (
    FullyQualifiedTableName,
    MetadataTablesQuery,
)


class HiveMetadataTablesQuery(MetadataTablesQuery):
    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection,
        prefixes: Optional[list[str]] = None,
    ):
        self.sql_dialect = sql_dialect
        self.data_source_connection: DataSourceConnection = data_source_connection
        self.prefixes = prefixes

    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list[FullyQualifiedTableName]:
        sql: str = self.build_sql_statement(database_name=database_name, schema_name=schema_name)
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return self.get_results(query_result)

    def build_sql_statement(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list:
        schema_str = ""
        if schema_name:
            schema_str = f" FROM {self.sql_dialect.quote_default(schema_name)}"
        return f"SHOW TABLES{schema_str}"

    def get_results(self, query_result: QueryResult) -> list[FullyQualifiedTableName]:
        # TODO: apply filters

        return [
            FullyQualifiedTableName(database_name="hive_metastore", schema_name=schema_name, table_name=table_name)
            for schema_name, table_name, _is_temporary in query_result.rows
        ]
