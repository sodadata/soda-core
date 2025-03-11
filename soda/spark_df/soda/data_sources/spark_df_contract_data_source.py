from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from soda.data_sources.spark_df_connection import SparkDfConnection
from soda.execution.data_type import DataType

from soda.contracts.impl.contract_data_source import FileClContractDataSource
from soda.contracts.impl.sql_dialect import SqlDialect
from soda.contracts.impl.yaml_helper import YamlFile

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SparkDfSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def _get_default_quote_char(self) -> str:
        return "`"

    def stmt_create_schema_if_not_exists(self, database_name: str, schema_name: str) -> str:
        schema_name_quoted: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted}"

    def _get_create_table_sql_type_dict(self) -> dict[str, str]:
        return {
            DataType.TEXT: "string",
            DataType.INTEGER: "integer",
            DataType.DECIMAL: "double",
            DataType.DATE: "date",
            DataType.TIME: "timestamp",
            DataType.TIMESTAMP: "timestamp",
            DataType.TIMESTAMP_TZ: "timestamp",  # No timezone support in Spark
            DataType.BOOLEAN: "boolean",
        }

    def _get_schema_check_sql_type_dict(self) -> dict[str, str]:
        return {
            DataType.TEXT: "string",
            DataType.INTEGER: "int",
            DataType.DECIMAL: "double",
            DataType.DATE: "date",
            DataType.TIME: "timestamp",
            DataType.TIMESTAMP: "timestamp",
            DataType.TIMESTAMP_TZ: "timestamp",  # No timezone support in Spark
            DataType.BOOLEAN: "boolean",
        }

    def stmt_select_table_names(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        table_name_like_filter: str | None = None,
        include_table_name_like_filters: list[str] = None,
        exclude_table_name_like_filters: list[str] = None,
    ) -> str:
        from_schema: str = f" FROM {self.quote_default(schema_name)}" if schema_name else ""
        like_filter: str = f" LIKE '{table_name_like_filter}'" if table_name_like_filter else ""
        return f"SHOW TABLES{from_schema}{like_filter}"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"date'{date_string}'"

    def literal_datetime(self, datetime: datetime):
        formatted = datetime.strftime("%Y-%m-%d %H:%M:%S")
        return f"timestamp'{formatted}'"

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"{expr} rlike('{regex_pattern}')"

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def quote_table(self, table_name) -> str:
        return f"`{table_name}`"

    def quote_column(self, column_name: str) -> str:
        return f"`{column_name}`"

    def regex_replace_flags(self) -> str:
        return ""


class SparkDfContractDataSource(FileClContractDataSource):
    def __init__(self, data_source_yaml_file: YamlFile, spark_session: SparkSession):
        data_source_yaml_dict: dict = data_source_yaml_file.get_dict()
        data_source_yaml_dict[self._KEY_TYPE] = "spark_df"
        data_source_yaml_dict.setdefault(self._KEY_NAME, "spark_ds")
        # Next line avoids parsing error in the constructor of FileClContractDataSource when it is absent
        data_source_yaml_dict[self._KEY_CONNECTION] = {}
        super().__init__(data_source_yaml_file)
        self.spark_session: SparkSession = spark_session

    def _create_sql_dialect(self) -> SqlDialect:
        return SparkDfSqlDialect()

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        return SparkDfConnection(self.spark_session)

    def sql_find_table_names(
        self,
        filter: str | None = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
        table_column_name: str = "table_name",
        schema_column_name: str = "table_schema",
    ) -> str:
        from_clause = f" FROM {self.schema}" if self.schema else ""
        return f"SHOW TABLES{from_clause}"

    @staticmethod
    def _filter_include_exclude(
        item_names: list[str], included_items: list[str], excluded_items: list[str]
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

    # def get_table_columns(
    #     self,
    #     table_name: str,
    #     query_name: str,
    #     included_columns: list[str] | None = None,
    #     excluded_columns: list[str] | None = None,
    # ) -> dict[str, str] | None:
    #     """
    #     :return: A dict mapping column names to data source data types.  Like eg
    #     {"id": "varchar", "cst_size": "int8", ...}
    #     """
    #     columns = {}
    #     query = Query(
    #         data_source_scan=self.data_source_scan,
    #         unqualified_query_name=query_name,
    #         sql=self.sql_get_table_columns(
    #             table_name, included_columns=included_columns, excluded_columns=excluded_columns
    #         ),
    #     )
    #     query.execute()
    #     if len(query.rows) > 0:
    #         rows = query.rows
    #         # Remove the partitioning information (see https://spark.apache.org/docs/latest/sql-ref-syntax-aux-describe-table.html)
    #         partition_indices = [i for i in range(len(rows)) if rows[i][0].startswith("# Partition")]
    #         if partition_indices:
    #             rows = rows[: partition_indices[0]]
    #         columns = {row[0]: row[1] for row in rows}
    #
    #         if included_columns or excluded_columns:
    #             column_names = list(columns.keys())
    #             filtered_column_names = self._filter_include_exclude(column_names, included_columns, excluded_columns)
    #             columns = {col_name: dtype for col_name, dtype in columns.items() if col_name in filtered_column_names}
    #     return columns
    #
    # def sql_get_table_columns(
    #     self,
    #     table_name: str,
    #     included_columns: list[str] | None = None,
    #     excluded_columns: list[str] | None = None,
    # ):
    #     return f"DESCRIBE {table_name}"
    #
    # def sql_get_column(self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None) -> str:
    #     table_filter_expression = self.sql_table_include_exclude_filter(
    #         "table_name", "table_schema", include_tables, exclude_tables
    #     )
    #     where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
    #     return (
    #         f"SELECT table_name, column_name, data_type, is_nullable \n"
    #         f"FROM {self.schema}.INFORMATION_SCHEMA.COLUMNS"
    #         f"{where_clause}"
    #     )
    #
    # def sql_get_table_names_with_count(
    #     self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    # ) -> str:
    #     return ""
    #
    # @staticmethod
    # def pattern_matches(spark_object_name: str, spark_object_name_pattern: str | None) -> bool:
    #     if spark_object_name_pattern is None:
    #         return True
    #     else:
    #         pattern_regex = spark_object_name_pattern.replace("%", ".*").lower()
    #         is_match = re.fullmatch(pattern_regex, spark_object_name.lower())
    #         return bool(is_match)
    #
    # def get_included_table_names(
    #     self,
    #     query_name: str,
    #     include_patterns: list[dict[str, str]],
    #     exclude_patterns: list[dict[str, str]],
    #     table_names_only: bool = False,
    # ) -> list[str]:
    #     query = Query(
    #         data_source_scan=self.data_source_scan,
    #         unqualified_query_name=query_name,
    #         sql=self.sql_find_table_names(),
    #     )
    #     query.execute()
    #     table_names = [row[1] for row in query.rows]
    #
    #     included_table_names = [
    #         table_name
    #         for table_name in table_names
    #         if any(
    #             self.pattern_matches(table_name, include_pattern["table_name_pattern"])
    #             for include_pattern in include_patterns
    #         )
    #         and not any(
    #             self.pattern_matches(table_name, exclude_pattern["table_name_pattern"])
    #             and (exclude_pattern.get("column_name_pattern") == "%" or table_names_only)
    #             for exclude_pattern in exclude_patterns
    #         )
    #     ]
    #
    #     return included_table_names
    #
    # def column_table_pattern_match(
    #     self, table_name: str, column_name: str, profiling_patterns: list[dict[str, str]]
    # ) -> bool:
    #     column_table_name_pattern_match = any(
    #         (
    #             self.pattern_matches(table_name, pattern["table_name_pattern"])
    #             and self.pattern_matches(column_name, pattern.get("column_name_pattern"))
    #         )
    #         for pattern in profiling_patterns
    #     )
    #     return column_table_name_pattern_match
    #
    # def get_tables_columns_metadata(
    #     self,
    #     query_name: str,
    #     include_patterns: list[dict[str, str]] | None = None,
    #     exclude_patterns: list[dict[str, str]] | None = None,
    #     table_names_only: bool = False,
    # ) -> dict[str, str] | None:
    #     if (not include_patterns) and (not exclude_patterns):
    #         return []
    #     included_table_names: list[str] = self.get_included_table_names(
    #         query_name, include_patterns, exclude_patterns, table_names_only=table_names_only
    #     )
    #     if table_names_only:
    #         return included_table_names
    #     tables_and_columns_metadata = defaultdict(dict)
    #     for table_name in included_table_names:
    #         query = Query(
    #             data_source_scan=self.data_source_scan,
    #             unqualified_query_name=f"get-tables-columns-metadata-describe-table-{table_name}-spark",
    #             sql=f"DESCRIBE {table_name}",
    #         )
    #         query.execute()
    #         columns_metadata = query.rows
    #
    #         partition_indices = [
    #             i for i in range(len(columns_metadata)) if columns_metadata[i][0].startswith("# Partition")
    #         ]
    #         if partition_indices:
    #             columns_metadata = columns_metadata[: partition_indices[0]]
    #
    #         if columns_metadata and len(columns_metadata) > 0:
    #             for column_name, column_datatype, _ in columns_metadata:
    #                 column_name_included = self.column_table_pattern_match(table_name, column_name, include_patterns)
    #                 column_name_excluded = self.column_table_pattern_match(table_name, column_name, exclude_patterns)
    #                 if column_name_included and not column_name_excluded:
    #                     tables_and_columns_metadata[table_name][column_name] = column_datatype
    #
    #     if tables_and_columns_metadata:
    #         return tables_and_columns_metadata
    #     else:
    #         return None
    #
    # def default_casify_table_name(self, identifier: str) -> str:
    #     return identifier.lower()
    #
    # def rollback(self):
    #     # Spark does not have transactions so do nothing here.
    #     pass
    #
