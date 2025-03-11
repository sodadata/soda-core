from __future__ import annotations

import logging
import re
import struct
import time
from datetime import datetime, timedelta, timezone
from itertools import chain, repeat
from textwrap import dedent
from typing import Callable, Mapping

import pyodbc
from azure.core.credentials import AccessToken
from azure.identity import (
    AzureCliCredential,
    DefaultAzureCredential,
    EnvironmentCredential,
)
from soda.__version__ import SODA_CORE_VERSION
from soda.common.exceptions import DataSourceConnectionError
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


_AZURE_AUTH_FUNCTION_TYPE = Callable[[str], AccessToken]
_SQL_COPT_SS_ACCESS_TOKEN = 1256
_MAX_REMAINING_AZURE_ACCESS_TOKEN_LIFETIME = 300
_AZURE_CREDENTIAL_SCOPE = "https://database.windows.net//.default"
_SYNAPSE_CREDENTIAL_SCOPE = "DW"
_FABRIC_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api"


def _get_auto_access_token(scope: str) -> AccessToken:
    return DefaultAzureCredential().get_token(scope)


def _get_environment_access_token(scope: str) -> AccessToken:
    return EnvironmentCredential().get_token(scope)


def _get_azure_cli_access_token(scope: str) -> AccessToken:
    return AzureCliCredential().get_token(scope)


def _get_mssparkutils_access_token(scope: str) -> AccessToken:
    from notebookutils import mssparkutils

    aad_token = mssparkutils.credentials.getToken(scope)
    expires_on = int(time.time() + 4500.0)
    token = AccessToken(
        token=aad_token,
        expires_on=expires_on,
    )
    return token


def _get_synapse_spark_access_token(scope: str) -> AccessToken:
    return _get_mssparkutils_access_token(scope)


def _get_fabric_spark_access_token(scope: str) -> AccessToken:
    return _get_mssparkutils_access_token(scope)

def _derive_scope(authentication_method: str, hostname: str) -> str:
    if "azuresynapse.net" in hostname:
        return _SYNAPSE_CREDENTIAL_SCOPE
    if "fabric.microsoft.com" in hostname:
        return _FABRIC_CREDENTIAL_SCOPE
    if "database.windows.net" in hostname:
        return _AZURE_CREDENTIAL_SCOPE
    if "synapse" in authentication_method:
        return _SYNAPSE_CREDENTIAL_SCOPE
    if "fabric" in authentication_method:
        return _FABRIC_CREDENTIAL_SCOPE
    return _AZURE_CREDENTIAL_SCOPE


_AZURE_AUTH_FUNCTIONS: Mapping[str, _AZURE_AUTH_FUNCTION_TYPE] = {
    "auto": _get_auto_access_token,
    "cli": _get_azure_cli_access_token,
    "environment": _get_environment_access_token,
    "synapsespark": _get_synapse_spark_access_token,
    "fabricspark": _get_fabric_spark_access_token,
}


def convert_bytes_to_mswindows_byte_string(value):
    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes


def convert_access_token_to_mswindows_byte_string(token):
    value = bytes(token.token, "UTF-8")
    return convert_bytes_to_mswindows_byte_string(value)


def get_pyodbc_attrs(authentication_method: str, scope: str|None, host: str):
    if not authentication_method.lower() in _AZURE_AUTH_FUNCTIONS:
        return None

    global _azure_access_token
    _azure_access_token = None

    if _azure_access_token:
        time_remaining = _azure_access_token.expires_on - time.time()
        if time_remaining < _MAX_REMAINING_AZURE_ACCESS_TOKEN_LIFETIME:
            _azure_access_token = None

    if not _azure_access_token:
        scope = scope or _derive_scope(authentication_method.lower(), host.lower())
        _azure_access_token = _AZURE_AUTH_FUNCTIONS[authentication_method.lower()](scope)

    token_bytes = convert_access_token_to_mswindows_byte_string(_azure_access_token)
    return {_SQL_COPT_SS_ACCESS_TOKEN: token_bytes}


class SQLServerDataSource(DataSource):
    TYPE = "sqlserver"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {"TEXT": ["text", "varchar", "char", "nvarchar", "nchar"]}

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "varchar(255)",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "datetime",
        DataType.TIMESTAMP_TZ: "datetimeoffset",
        DataType.BOOLEAN: "bit",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP: dict = {
        DataType.TEXT: "varchar",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "datetime",
        DataType.TIMESTAMP_TZ: "datetimeoffset",
        DataType.BOOLEAN: "bit",
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

    TEXT_TYPES_FOR_PROFILING = ["char", "varchar", "text", "nchar", "nvarchar"]
    LIMIT_KEYWORD = "TOP"

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)

        self.host = data_source_properties.get("host", "localhost")
        self.port = data_source_properties.get("port", 1433)
        self.driver = data_source_properties.get("driver", "ODBC Driver 18 for SQL Server")
        self.authentication = data_source_properties.get("authentication", "SQL")
        self.scope = data_source_properties.get("scope", None)
        self.username = data_source_properties.get("username", None)
        self.password = data_source_properties.get("password", None)
        self.client_id = data_source_properties.get("client_id", None)
        self.client_secret = data_source_properties.get("client_secret", None)
        self.tenant_id = data_source_properties.get("tenant_id", None)
        self.database = data_source_properties.get("database", "master")
        self.schema = data_source_properties.get("schema", "dbo")
        self.trusted_connection = data_source_properties.get("trusted_connection", False)
        self.encrypt = data_source_properties.get("encrypt", False)
        self.trust_server_certificate = data_source_properties.get("trust_server_certificate", False)
        self.connection_max_retries = data_source_properties.get("connection_max_retries", 0)
        self.enable_tracing = data_source_properties.get("enable_tracing", False)
        self.login_timeout = data_source_properties.get("login_timeout", 0)

        # sqlserver reuses only a handful of default formats.
        reuse_formats = ["percentage"]

        self.DEFAULT_FORMATS = {k: v for k, v in self.DEFAULT_FORMATS.items() if k in reuse_formats}
        self.DEFAULT_FORMAT_EXPRESSIONS = self.build_default_format_expressions()

    def build_default_format_expressions(self) -> dict[str, str]:
        def construct_like(
            pattern_like: str, pattern_not_like: str | None = None, extra_conditions: str | None = None
        ) -> str:
            if pattern_not_like and extra_conditions:
                return (
                    f"(({{expr}} like '{pattern_like}' and {{expr}} not like '{pattern_not_like}') {extra_conditions})"
                )
            elif pattern_not_like:
                return f"({{expr}} like '{pattern_like}' and {{expr}} not like '{pattern_not_like}')"
            elif extra_conditions:
                return f"(({{expr}} like '{pattern_like}') {extra_conditions})"

            return f"({{expr}} like '{pattern_like}')"

        return {
            "integer": construct_like(r"[-+0-9]%", r"[-+0-9][.,]%"),
            "positive integer": construct_like(r"[+0-9]%", r"[+0-9][.,]%"),
            "negative integer": construct_like(r"-%[0-9]%", r"-%[-0-9][.,]%", r"or {expr} like '0'"),
            "decimal": construct_like(r"[-0-9][.0-9]%[0-9]%"),
            "date eu": f"({{expr}} LIKE '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]' OR {{expr}} LIKE '[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]' OR {{expr}} LIKE '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]')",
            "date us": f"({{expr}} LIKE '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]' OR {{expr}} LIKE '[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]' OR {{expr}} LIKE '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]')",
            "date inverse": f"({{expr}} LIKE '[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]' OR {{expr}} LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' OR {{expr}} LIKE '[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9]')",
            "ip address": f"({{expr}} LIKE '[0-9]%.%' and {{expr}} like '[0-9].[0-9].[0-9].[0-9]'  or {{expr}} like '[0-9][0-9].%' or {{expr}} like '[0-9][0-9][0-9].%' or {{expr}} like '[0-9][0-9][0-9].%')",
            "uuid": f"({{expr}} LIKE '[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]')",
            "phone number": f"({{expr}} LIKE '[+!0-9]%' and {{expr}} not like '%[a-z!A-z]')",
            "email": f"({{expr}} LIKE '%_@_%.__%')",
        }

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

        def build_connection_string():
            connection_parameters_string = self.get_connection_parameters_string()
            conn_params = []

            conn_params.append(f"DRIVER={{{self.driver}}}")
            conn_params.append(f"DATABASE={self.database}")

            if "\\" in self.host:
                # If there is a backslash in the host name, the host is a
                # SQL Server named instance. In this case then port number has to be omitted.
                conn_params.append(f"SERVER={self.host}")
            else:
                conn_params.append(f"SERVER={self.host},{int(self.port)}")

            if connection_parameters_string and connection_parameters_string != "":
                conn_params.append(connection_parameters_string)

            if self.trusted_connection:
                conn_params.append("Trusted_Connection=YES")

            if self.trust_server_certificate:
                conn_params.append("TrustServerCertificate=YES")

            if self.encrypt:
                conn_params.append("Encrypt=YES")

            if int(self.connection_max_retries) > 0:
                conn_params.append(f"ConnectRetryCount={int(self.connection_max_retries)}")

            if self.enable_tracing:
                conn_params.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")

            if self.authentication.lower() == "sql":
                conn_params.append(f"UID={{{self.username}}}")
                conn_params.append(f"PWD={{{self.password}}}")
            elif self.authentication.lower() == "activedirectoryinteractive":
                conn_params.append("Authentication=ActiveDirectoryInteractive")
                conn_params.append(f"UID={{{self.username}}}")
            elif self.authentication.lower() == "activedirectorypassword":
                conn_params.append("Authentication=ActiveDirectoryPassword")
                conn_params.append(f"UID={{{self.username}}}")
                conn_params.append(f"PWD={{{self.password}}}")
            elif self.authentication.lower() == "activedirectoryserviceprincipal":
                conn_params.append("Authentication=ActiveDirectoryServicePrincipal")
                conn_params.append(f"UID={{{self.client_id}}}")
                conn_params.append(f"PWD={{{self.client_secret}}}")
            elif "activedirectory" in self.authentication.lower():
                conn_params.append(f"Authentication={self.authentication}")

            conn_params.append(f"APP=soda-core-fabric/{SODA_CORE_VERSION}")

            conn_str = ";".join(conn_params)

            return conn_str

        try:
            self.connection = pyodbc.connect(
                build_connection_string(),
                attrs_before=get_pyodbc_attrs(self.authentication, self.scope, self.host),
                timeout=int(self.login_timeout),
            )

            self.connection.add_output_converter(-155, handle_datetimeoffset)
            self.connection.add_output_converter(-150, handle_datetime)
            return self.connection
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def get_connection_parameter_value(self, value):
        if isinstance(value, bool):
            return "YES" if value else "NO"

        return value

    def validate_configuration(self, logs: Logs) -> None:
        pass

    def safe_connection_data(self):
        return [self.type, self.host, self.port, self.schema, self.database]

    def literal_date(self, dt: datetime):
        return f"'{dt.strftime('%Y-%m-%d')}'"

    def expr_count(self, expr):
        return f"COUNT_BIG({expr})"

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
                , {self.expr_count(f'distinct({column_name})')} as distinct_values
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
                {self.expr_count(f'distinct({column_name})')} as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
                , avg(len({column_name})) as avg_length
                , min(len({column_name})) as min_length
                , max(len({column_name})) as max_length
            FROM {qualified_table_name}
            """
        )

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"PATINDEX ('{regex_pattern}', {expr}) > 0"

    def sql_select_all(self, table_name: str, limit: int | None = None, filter: str | None = None) -> str:
        qualified_table_name = self.qualified_table_name(table_name)

        filter_sql = ""
        if filter:
            filter_sql = f" \n WHERE {filter}"

        limit_sql = ""
        if limit is not None:
            limit_sql = f" \n TOP {limit} \n"

        columns_names = ", ".join(self.sql_select_all_column_names(table_name))

        sql = f"SELECT {limit_sql} {columns_names} FROM {qualified_table_name}{filter_sql}"
        return sql

    def sql_select_column_with_filter_and_limit(
        self,
        column_name: str,
        table_name: str,
        filter_clause: str | None = None,
        sample_clause: str | None = None,
        limit: int | None = None,
    ) -> str:
        """
        Returns a SQL query that selects a column from a table with optional filter, sample query
        and limit.
        """
        filter_clauses_str = f"\n WHERE {filter_clause}" if filter_clause else ""
        sample_clauses_str = f"\n {sample_clause}" if sample_clause else ""
        limit_clauses_str = f"TOP {limit}" if limit else ""

        sql = (
            f"SELECT {limit_clauses_str} \n"
            f"  {column_name} \n"
            f"FROM {table_name}{sample_clauses_str}{filter_clauses_str}"
        )
        return sql

    def sql_groupby_count_categorical_column(
        self,
        select_query: str,
        column_name: str,
        limit: int | None = None,
    ) -> str:
        cte = select_query.replace("\n", " ")
        # delete multiple spaces
        cte = re.sub(" +", " ", cte)
        top_limit = f"TOP {limit}" if limit else ""
        sql = dedent(
            f"""
                WITH processed_table AS (
                    {cte}
                )
                SELECT {top_limit}
                    {column_name}
                    , {self.expr_count_all()} AS frequency
                FROM processed_table
                GROUP BY {column_name}
            """
        )
        return dedent(sql)

    def expr_false_condition(self):
        return "1 = 0"

    def sql_get_duplicates_aggregated(
        self,
        column_names: str,
        table_name: str,
        filter: str,
        limit: str | None = None,
        invert_condition: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> str | None:
        qualified_table_name = self.qualified_table_name(table_name)
        limit_sql = ""
        main_query_columns = f"{column_names}, frequency" if exclude_patterns else "*"

        if limit:
            limit_sql = f"TOP {limit}"

        sql = dedent(
            f"""
            WITH frequencies AS (
                SELECT {column_names}, {self.expr_count_all()} AS frequency
                FROM {qualified_table_name}
                WHERE {filter}
                GROUP BY {column_names})
            SELECT {limit_sql} {main_query_columns}
            FROM frequencies
            WHERE frequency {'<=' if invert_condition else '>'} 1
            ORDER BY frequency DESC"""
        )

        return sql

    def sql_get_duplicates(
        self,
        column_names: str,
        table_name: str,
        filter: str,
        limit: str | None = None,
        invert_condition: bool = False,
    ) -> str | None:
        qualified_table_name = self.qualified_table_name(table_name)
        columns = column_names.split(", ")

        main_query_columns = self.sql_select_all_column_names(table_name)
        qualified_main_query_columns = ", ".join([f"main.{c}" for c in main_query_columns])
        join = " AND ".join([f"main.{c} = frequencies.{c}" for c in columns])

        limit_sql = ""
        if limit:
            limit_sql = f"TOP {limit}"

        sql = dedent(
            f"""
            WITH frequencies AS (
                SELECT {column_names}
                FROM {qualified_table_name}
                WHERE {filter}
                GROUP BY {column_names}
                HAVING {self.expr_count_all()} {'<=' if invert_condition else '>'} 1)
            SELECT {limit_sql} {qualified_main_query_columns}
            FROM {qualified_table_name} main
            JOIN frequencies ON {join}
            """
        )

        return sql

    def sql_reference_query(
        self,
        columns: str,
        table_name: str,
        target_table_name: str,
        join_condition: str,
        where_condition: str,
        limit: int | None = None,
    ) -> str:
        limit_sql = ""
        if limit:
            limit_sql = f"TOP {limit}"

        sql = dedent(
            f"""
            SELECT {limit_sql} {columns}
                FROM {table_name}  SOURCE
                LEFT JOIN {target_table_name} TARGET on {join_condition}
            WHERE {where_condition}"""
        )

        return sql

    def quote_table(self, table_name: str) -> str:
        return f"[{table_name}]"

    def quote_column(self, column_name: str) -> str:
        return f"[{column_name}]"

    def is_quoted(self, table_name: str) -> bool:
        return (
            (table_name.startswith('"') and table_name.endswith('"'))
            or (table_name.startswith("'") and table_name.endswith("'"))
            or (table_name.startswith("[") and table_name.endswith("]"))
        )

    def sql_information_schema_tables(self) -> str:
        return "INFORMATION_SCHEMA.TABLES"

    def sql_information_schema_columns(self) -> str:
        return "INFORMATION_SCHEMA.COLUMNS"

    def default_casify_sql_function(self) -> str:
        """Returns the sql function to use for default casify."""
        return ""

    def default_casify_system_name(self, identifier: str) -> str:
        return identifier

    def qualified_table_name(self, table_name: str) -> str:
        """
        table_name can be quoted or unquoted
        """
        if self.quote_tables and not self.is_quoted(table_name):
            table_name = self.quote_table(table_name)

        if self.table_prefix:
            return f"{self.table_prefix}.{table_name}"
        return table_name
