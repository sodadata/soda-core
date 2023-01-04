from __future__ import annotations

import logging
from textwrap import dedent

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.dataframe.core import Series
from dask_sql import Context
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
        DataType.TEXT: "varchar",
        DataType.INTEGER: "bigint",
        DataType.DECIMAL: "float",
        DataType.DATE: "timestamp",
        DataType.TIME: "timestamp",
        DataType.TIMESTAMP: "timestamp",
        DataType.TIMESTAMP_TZ: "timestamp_with_local_time_zone",
        DataType.BOOLEAN: "boolean",
    }

    PANDAS_TYPE_FOR_CREATE_TABLE_MAP = {
        DataType.TEXT: "O",
        DataType.INTEGER: pd.Int64Dtype(),
        DataType.DECIMAL: "f",
        DataType.DATE: "M",
        DataType.TIME: "datetime64[ns]",
        DataType.TIMESTAMP: "datetime64[ns]",
        DataType.TIMESTAMP_TZ: pd.DatetimeTZDtype(tz="UTC"),
        DataType.BOOLEAN: "boolean",
    }
    # Supported data types used in create statements. These are used in tests for creating test data and do not affect the actual library functionality.
    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "str",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "timestamp",
        DataType.TIMESTAMP_TZ: "timestamptz",
        DataType.BOOLEAN: "bool",
    }

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.context: Context = data_source_properties.get("context")
        self.context.register_function(
            self.nullif_custom, "nullif_custom", [("regex_pattern", str)], str, row_udf=False
        )
        self.context.register_function(
            self.regexp_like,
            "regexp_like",
            [("x", np.dtype("object")), ("regex_pattern", np.dtype("object"))],
            np.dtype("object"),
            row_udf=False,
            replace=True,
        )
        self.context.register_function(self.length, "length", [("x", np.dtype("object"))], np.int32)
        self.context.register_function(
            self.regexp_replace_custom,
            "regexp_replace_custom",
            [("regex_pattern", str), ("replacement_pattern", str), ("flags", str)],
            str,
            row_udf=False,
        )

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
        table_column_name: str = "table",
        schema_column_name: str = "table_schema",
    ) -> str:
        # First register `show tables` in a dask table and then apply query on that table
        # to find the intended tables
        show_tables_temp_query = "show tables"
        dd_show_tables = self.context.sql(show_tables_temp_query).compute()

        # Due to a bug in dask-sql we cannot use uppercases in column names
        dd_show_tables.columns = ["table"]

        self.context.create_table("showtables", dd_show_tables)

        sql = f"select {table_column_name} \n" f"from showtables"
        where_clauses = []

        if filter:
            where_clauses.append(f"lower({self.default_casify_column_name(table_column_name)}) like '{filter.lower()}'")

        includes_excludes_filter = self.sql_table_include_exclude_filter(
            table_column_name, schema_column_name, include_tables, exclude_tables
        )
        if includes_excludes_filter:
            where_clauses.append(includes_excludes_filter)

        if where_clauses:
            where_clauses_sql = "\n  and ".join(where_clauses)
            sql += f"\nwhere {where_clauses_sql}"
        return sql

    def default_casify_table_name(self, identifier: str) -> str:
        """Formats table identifier to e.g. a default case for a given data source."""
        return identifier.lower()

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

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: list[object] | None, expr: str):
        if metric_name in ["stddev", "stddev_pop", "stddev_samp", "var_pop"]:
            return f"{metric_name}({expr})"
        # if metric_name in ["percentile", "percentile_disc"]:
        #     # TODO ensure proper error if the metric_args[0] is not a valid number
        #     percentile_fraction = metric_args[1] if metric_args else None
        #     return f"PERCENTILE_DISC({percentile_fraction}) WITHIN GROUP (ORDER BY {expr})"
        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

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

    def cast_text_to_number(self, column_name, validity_format: str):
        """Cast string to number

        - first regex replace removes extra chars, keeps: "digits + - . ,"
        - second regex changes "," to "."
        - Nullif makes sure that if regexes return empty string then Null is returned instead
        """
        regex = self.escape_regex(r"'[^-0-9\.\,]'")
        return dedent(
            f"""
            CAST(
                NULLIF_CUSTOM(
                    REGEXP_REPLACE_CUSTOM(
                        REGEXP_REPLACE_CUSTOM(
                            {column_name},
                            {regex},
                            ''{self.regex_replace_flags()}
                        ),
                        ',',
                        '.'{self.regex_replace_flags()}
                    ),
                    ''
                ) AS {self.SQL_TYPE_FOR_CREATE_TABLE_MAP[DataType.DECIMAL]}
            )"""
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
    def nullif_custom(selected_column: Series, null_replacement: str) -> Series:
        selected_column = selected_column.replace(null_replacement, None)
        return selected_column

    @staticmethod
    def regexp_replace_custom(
        selected_column: Series, regex_pattern: str, replacement_pattern: str, flags: str
    ) -> Series:
        selected_column = selected_column.str.replace(regex_pattern, replacement_pattern, regex=True, flags=0)
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

    @staticmethod
    def length(x: str) -> int:
        return len(x)
