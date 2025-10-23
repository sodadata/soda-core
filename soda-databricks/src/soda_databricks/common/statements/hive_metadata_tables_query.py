from __future__ import annotations

import re
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
        return self.get_results(query_result, include_table_name_like_filters, exclude_table_name_like_filters)

    def build_sql_statement(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> str:  # Return type for this function is a string, not a list (for the super method it is a list!)
        schema_str = ""
        if schema_name:
            schema_str = f" FROM {self.sql_dialect.quote_default(schema_name)}"
        return f"SHOW TABLES{schema_str}"

    def get_results(
        self,
        query_result: QueryResult,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list[FullyQualifiedTableName]:
        names_for_filtering = [table_name for _, table_name, _ in query_result.rows]
        filtered_names = self._filter_include_exclude(
            names_for_filtering, include_table_name_like_filters, exclude_table_name_like_filters
        )

        return [
            FullyQualifiedTableName(database_name="hive_metastore", schema_name=schema_name, table_name=table_name)
            for schema_name, table_name, _is_temporary in query_result.rows
            if table_name in filtered_names
        ]

    # Copy from soda-library (v3)
    @staticmethod
    def _filter_include_exclude(
        item_names: list[str], included_items: Optional[list[str]] = None, excluded_items: Optional[list[str]] = None
    ) -> list[str]:
        filtered_names = item_names
        if included_items or excluded_items:

            def matches(name, pattern: str) -> bool:
                pattern_regex = pattern.replace("%", ".*").lower()
                is_match = re.fullmatch(pattern_regex, name.lower())
                return bool(is_match)

            if included_items:
                filtered_names = [
                    filtered_name
                    for filtered_name in filtered_names
                    if any(matches(filtered_name, included_item) for included_item in included_items)
                ]
            if excluded_items:
                filtered_names = [
                    filtered_name
                    for filtered_name in filtered_names
                    if all(not matches(filtered_name, excluded_item) for excluded_item in excluded_items)
                ]
        return filtered_names
