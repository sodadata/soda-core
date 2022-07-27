from __future__ import annotations

import logging
import struct
from datetime import datetime, timedelta, timezone
from textwrap import dedent

import pyodbc
from soda.common.exceptions import DataSourceConnectionError
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class SQLServerDataSource(DataSource):

    TYPE = "sqlserver"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {"TEXT": ["text", "varchar", "char"]}

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "varchar(255)",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "datetime",
        DataType.TIMESTAMP_TZ: "datetimeoffset",
        DataType.BOOLEAN: "boolean",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP: dict = {
        DataType.TEXT: "varchar",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "datetime",
        DataType.TIMESTAMP_TZ: "datetimeoffset",
        DataType.BOOLEAN: "boolean",
    }
    NUMERIC_TYPES_FOR_PROFILING = [
        "bigint",
        "numeric",
        "bit",
        "smallint",
        "decimal",
        "smallmoney",
        "int",
        "tinyint",
        "money",
        "float",
        "real",
    ]

    TEXT_TYPES_FOR_PROFILING = ["char", "varchar", "text"]

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)

        self.host = data_source_properties.get("host", "localhost")
        self.port = data_source_properties.get("port", "1433")
        self.driver = data_source_properties.get("driver", "ODBC Driver 18 for SQL Server")
        self.username = data_source_properties.get("username")
        self.password = data_source_properties.get("password")
        self.database = data_source_properties.get("database", "master")
        self.schema = data_source_properties.get("schema", "dbo")
        self.trusted_connection = data_source_properties.get("trusted_connection", False)
        self.encrypt = data_source_properties.get("encrypt", False)
        self.trust_server_certificate = data_source_properties.get("trust_server_certificate", False)

    def connect(self):
        def handle_datetime(dto_value):
            tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
            return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000)

        def handle_datetimeoffset(dto_value):
            tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
            return datetime(
                tup[0],
                tup[1],
                tup[2],
                tup[3],
                tup[4],
                tup[5],
                tup[6] // 1000,
                timezone(timedelta(hours=tup[7], minutes=tup[8])),
            )

        try:
            self.connection = pyodbc.connect(
                ("Trusted_Connection=YES;" if self.trusted_connection else "")
                + ("TrustServerCertificate=YES;" if self.trust_server_certificate else "")
                + ("Encrypt=YES;" if self.encrypt else "")
                + "DRIVER={"
                + self.driver
                + "};SERVER="
                # + "SERVER="
                + self.host
                + ";PORT="
                + self.port
                + ";DATABASE="
                + self.database
                + ";UID="
                + self.username
                + ";PWD="
                + self.password
            )

            self.connection.add_output_converter(-155, handle_datetimeoffset)
            self.connection.add_output_converter(-150, handle_datetime)
            return self.connection
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def validate_configuration(self, logs: Logs) -> None:
        pass

    def safe_connection_data(self):
        return [self.type, self.host, self.port, self.schema, self.database]

    def literal_date(self, dt: datetime):
        return f"'{dt.strftime('%Y-%m-%d')}'"

    def expr_length(self, expr):
        return f"LEN({expr})"

    def expr_avg(self, expr):
        return f"AVG( CAST({expr} as FLOAT))"

    def profiling_sql_aggregates_numeric(self, table_name: str, column_name: str) -> str:
        column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)
        return dedent(
            f"""
            SELECT
                avg({column_name}) as average
                , sum({column_name}) as sum
                , var({column_name}) as variance
                , stdev({column_name}) as standard_deviation
                , count(distinct({column_name})) as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
            FROM {qualified_table_name}
            """
        )

    def profiling_sql_values_frequencies_query(
        self,
        data_type_category: str,
        table_name: str,
        column_name: str,
        limit_mins_maxs: int,
        limit_frequent_values: int,
    ) -> str:
        cast_to_text = self.cast_to_text

        value_frequencies_cte = self.profiling_sql_value_frequencies_cte(table_name, column_name)

        union = self.sql_union()

        frequent_values_cte = f"""frequent_values AS (
                            SELECT TOP {limit_frequent_values} {cast_to_text("'frequent_values'")} AS metric_, ROW_NUMBER() OVER(ORDER BY frequency_ DESC) AS index_, value_, frequency_
                            FROM value_frequencies
                            ORDER BY frequency_ desc
                        )"""

        if data_type_category == "text":
            return dedent(
                f"""
                    WITH
                        {value_frequencies_cte},
                        {frequent_values_cte}
                    SELECT *
                    FROM frequent_values
                    ORDER BY metric_ ASC, index_ ASC
                """
            )

        elif data_type_category == "numeric":

            mins_cte = f"""mins AS (
                            SELECT TOP {limit_mins_maxs} {cast_to_text("'mins'")} AS metric_, ROW_NUMBER() OVER(ORDER BY value_ ASC) AS index_, value_, frequency_
                            FROM value_frequencies
                            WHERE value_ IS NOT NULL
                            ORDER BY value_ ASC

                        )"""

            maxs_cte = f"""maxs AS (
                            SELECT TOP {limit_mins_maxs} {cast_to_text("'maxs'")} AS metric_, ROW_NUMBER() OVER(ORDER BY value_ DESC) AS index_, value_, frequency_
                            FROM value_frequencies
                            WHERE value_ IS NOT NULL
                            ORDER BY value_ DESC

                        )"""

            return dedent(
                f"""
                    WITH
                        {value_frequencies_cte},
                        {mins_cte},
                        {maxs_cte},
                        {frequent_values_cte},
                        result AS (
                            SELECT * FROM mins
                            {union}
                            SELECT * FROM maxs
                            {union}
                            SELECT * FROM frequent_values
                        )
                    SELECT *
                    FROM result
                    ORDER BY metric_ ASC, index_ ASC
                """
            )

        raise AssertionError("data_type_category must be either 'numeric' or 'text'")

    def profiling_sql_aggregates_text(self, table_name: str, column_name: str) -> str:
        column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)
        return dedent(
            f"""
            SELECT
                count(distinct({column_name})) as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
                , avg(len({column_name})) as avg_length
                , min(len({column_name})) as min_length
                , max(len({column_name})) as max_length
            FROM {qualified_table_name}
            """
        )

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"PATINDEX ('%{regex_pattern}%' ,{expr} ) = 0 "

    def sql_select_all(self, table_name: str, limit: int | None = None) -> str:
        qualified_table_name = self.qualified_table_name(table_name)
        limit_sql = ""
        if limit is not None:
            limit_sql = f" \n TOP {limit} \n"
        sql = f"SELECT {limit_sql} * FROM {qualified_table_name}"
        return sql

    def sql_select_column_with_filter_and_limit(
        self, column_name: str, table_name: str, filter_clause: str, limit: int | None = None
    ) -> str:

        sql = f"SELECT TOP {limit} \n" f" {column_name} \n" f"FROM {table_name}{filter_clause}"
        return sql

    def expr_false_condition(self):
        return "1 = 0"
