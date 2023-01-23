from __future__ import annotations

from typing import TYPE_CHECKING

from soda.profiling.profile_columns_result import (
    ProfileColumnsResultColumn,
    ProfileColumnsResultTable,
)

if TYPE_CHECKING:
    from soda.execution.data_source import DataSource


class ProfilerTableMetadata:
    def __init__(self, data_source: DataSource, include_columns: list[str], exclude_columns: list[str]) -> None:
        self.data_source = data_source
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns

    def get_profile_result_tables(self) -> list[ProfileColumnsResultTable]:
        include_patterns = self.parse_profiling_expressions(self.include_columns)
        exclude_patterns = self.parse_profiling_expressions(self.exclude_columns)

        tables_columns_metadata = self.data_source.get_tables_columns_metadata(
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            query_name="profile-columns-get-table-and-column-metadata",
        )
        if not tables_columns_metadata:
            return []

        result_tables = []
        for table_name, columns_metadata in tables_columns_metadata.items():
            row_count = self.data_source.get_table_row_count(table_name)
            result_table = ProfileColumnsResultTable(
                table_name=table_name, data_source=self.data_source.data_source_name, row_count=row_count
            )
            columns = []
            for column_name, data_type in columns_metadata.items():
                result_column = ProfileColumnsResultColumn(column_name=column_name, column_type=data_type)
                columns.append(result_column)
            result_table.result_columns = columns
            result_tables.append(result_table)
        return result_tables

    @staticmethod
    def parse_profiling_expressions(profiling_expressions: list[str]) -> list[dict[str, str]]:
        parsed_profiling_expressions = []
        for profiling_expression in profiling_expressions:
            table_name_pattern, column_name_pattern = profiling_expression.split(".")
            parsed_profiling_expressions.append(
                {
                    "table_name_pattern": table_name_pattern,
                    "column_name_pattern": column_name_pattern,
                }
            )

        return parsed_profiling_expressions
