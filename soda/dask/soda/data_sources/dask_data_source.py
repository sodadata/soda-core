from __future__ import annotations

import logging
from textwrap import dedent

import dask.dataframe as dd
from dask.dataframe.core import Series
from soda.common.logs import Logs
from soda.data_sources.dask_connection import DaskConnection
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType


class DaskDataSource(DataSource):
    TYPE = "dask"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "string": ["character varying", "varchar", "text"],
        "int": ["integer", "int"],
        "double": ["decimal"],
        "timestamp": ["timestamptz"],
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        DataType.TEXT: "string",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "double",
        DataType.DATE: "date",
        DataType.TIME: "timestamp",
        DataType.TIMESTAMP: "timestamp",
        DataType.TIMESTAMP_TZ: "timestamp",  # No timezone support in Spark
        DataType.BOOLEAN: "boolean",
    }

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.context = data_source_properties.get("context")
        self.context.register_function(self.regexp_like, "regexp_like", [("regex_pattern", str)], str, row_udf=False)
        self.context.register_aggregation(self.distinct_count(), "distinct_count", [("x", float)], float)
        self.context.register_aggregation(self.distinct_count(), "distinct_count_str", [("x", str)], float)

    def connect(self) -> None:
        self.connection = DaskConnection(self.context)

    def quote_table(self, table_name: str) -> str:
        return f"{table_name}"

    def sql_get_table_names_with_count(
        self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    ) -> str | None:
        return None

    def cast_to_text(self, expr: str) -> str:
        return f"CAST({expr} AS string)"

    def sql_get_table_columns(
        self, table_name: str, included_columns: list[str] | None = None, excluded_columns: list[str] | None = None
    ) -> str:
        return f"SHOW COLUMNS FROM {table_name}"

    def sql_find_table_names(
        self,
        filter: str | None = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
        table_column_name: str = "Table",
        schema_column_name: str = "table_schema",
    ) -> str:
        sql = "SHOW TABLES"
        return sql

    def sql_get_column(self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None) -> str:
        table_filter_expression = self.sql_table_include_exclude_filter(
            "table_name", "table_schema", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return (
            f"SELECT table_name, column_name, data_type, is_nullable \n"
            f"FROM {self.schema}.INFORMATION_SCHEMA.COLUMNS"
            f"{where_clause}"
        )

    def profiling_sql_aggregates_numeric(self, table_name: str, column_name: str) -> str:
        column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)

        return dedent(
            f"""
            SELECT
                avg({column_name}) as average
                , sum({column_name}) as sum
                , var_pop({column_name}) as variance
                , stddev({column_name}) as standard_deviation
                , distinct_count(cast({column_name} as double)) as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
            FROM {qualified_table_name}
            """
        )

    def profiling_sql_aggregates_text(self, table_name: str, column_name: str) -> str:
        column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)
        return dedent(
            f"""
            SELECT
                distinct_count_str({column_name}) as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
                , avg(length({column_name})) as avg_length
                , min(length({column_name})) as min_length
                , max(length({column_name})) as max_length
            FROM {qualified_table_name}
            """
        )

    def test(self, sql: str) -> None:
        logging.debug(f"Running SQL query:\n{sql}")
        df = self.connection.context.sql(sql)
        df.compute()

    @staticmethod
    def regexp_like(selected_column: Series, regex_pattern: str) -> Series:
        selected_column = selected_column.str.contains(regex_pattern, regex=True, na=False)
        return selected_column

    @staticmethod
    def distinct_count() -> dd.Aggregation:
        def chunk(s: Series) -> Series:
            """
            The function applied to the
            individual partition (map)
            """
            return s.apply(lambda x: list(set(x)))

        def agg(s: Series) -> Series:
            """
            The function whic will aggrgate
            the result from all the partitions(reduce)
            """
            s = s._selected_obj
            return s.groupby(level=list(range(s.index.nlevels))).sum()

        def finalize(s: Series) -> Series:
            """
            The optional functional that will be
            applied to the result of the agg_tu functions
            """
            return s.apply(lambda x: len(set(x)))

        return dd.Aggregation("distinct_count", chunk, agg, finalize)
