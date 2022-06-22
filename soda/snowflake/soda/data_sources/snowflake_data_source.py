from __future__ import annotations

import logging
import re

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake import connector
from snowflake.connector.network import DEFAULT_SOCKET_CONNECT_TIMEOUT
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class SnowflakeDataSource(DataSource):
    TYPE = "snowflake"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "TEXT": ["character varying", "varchar", "string"],
        "NUMBER": ["integer", "int"],
        "FLOAT": ["decimal"],
        "TIMESTAMP_NTZ": ["timestamp"],
        "TIMESTAMP_TZ": ["timestamptz"],
    }
    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "TEXT",
        DataType.INTEGER: "INT",
        DataType.DECIMAL: "FLOAT",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP_NTZ",
        DataType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
        DataType.BOOLEAN: "BOOLEAN",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        DataType.TEXT: "TEXT",
        DataType.INTEGER: "NUMBER",
        DataType.DECIMAL: "FLOAT",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP_NTZ",
        DataType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
        DataType.BOOLEAN: "BOOLEAN",
    }

    NUMERIC_TYPES_FOR_PROFILING = ["FLOAT", "NUMBER", "INT"]
    TEXT_TYPES_FOR_PROFILING = ["TEXT"]

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.user = data_source_properties.get("username")
        self.password = data_source_properties.get("password")
        self.account = data_source_properties.get("account")
        self.data_source = data_source_properties.get("data_source")
        self.warehouse = data_source_properties.get("warehouse")
        self.login_timeout = data_source_properties.get("connection_timeout", DEFAULT_SOCKET_CONNECT_TIMEOUT)
        self.role = data_source_properties.get("role")
        self.client_session_keep_alive = data_source_properties.get("client_session_keep_alive")
        self.session_parameters = data_source_properties.get("session_params")

    def connect(self):
        self.connection = connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            data_source=self.data_source,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            login_timeout=self.login_timeout,
            role=self.role,
            client_session_keep_alive=self.client_session_keep_alive,
            session_parameters=self.session_parameters,
        )

    def __get_private_key(self):
        if not (self.data_source_properties.get("private_key_path") or self.data_source_properties.get("private_key")):
            return None

        if self.data_source_properties.get("private_key_passphrase"):
            encoded_passphrase = self.data_source_properties.get("private_key_passphrase").encode()
        else:
            encoded_passphrase = None

        pk_bytes = None
        if self.data_source_properties.get("private_key"):
            pk_bytes = self.data_source_properties.get("private_key").encode()
        elif self.data_source_properties.get("private_key_path"):
            with open(self.data_source_properties.get("private_key_path"), "rb") as pk:
                pk_bytes = pk.read()

        p_key = serialization.load_pem_private_key(pk_bytes, password=encoded_passphrase, backend=default_backend())

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def regex_replace_flags(self) -> str:
        return ""

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: list[object] | None, expr: str):
        # TODO add all of these snowflake specific statistical aggregate functions: https://docs.snowflake.com/en/sql-reference/functions-aggregation.html
        if metric_name in [
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "variance",
            "var_pop",
            "var_samp",
        ]:
            return f"{metric_name.upper()}({expr})"
        if metric_name in ["percentile", "percentile_disc"]:
            # TODO ensure proper error if the metric_args[0] is not a valid number
            percentile_fraction = metric_args[1] if metric_args else None
            return f"PERCENTILE_DISC({percentile_fraction}) WITHIN GROUP (ORDER BY {expr})"
        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    def sql_get_table_names_with_count(
        self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    ) -> str:
        table_filter_expression = self.sql_table_include_exclude_filter(
            "table_name", "table_schema", include_tables, exclude_tables
        )
        where_clause = f"AND {table_filter_expression}" if table_filter_expression else ""
        sql = f"""
            SELECT table_name, row_count
            FROM information_schema.tables
            WHERE table_schema != 'INFORMATION_SCHEMA'
            {where_clause}
            """
        return sql

    def default_casify_table_name(self, identifier: str) -> str:
        return identifier.upper()

    def default_casify_column_name(self, identifier: str) -> str:
        return identifier.upper()

    def default_casify_type_name(self, identifier: str) -> str:
        return identifier.upper()

    def safe_connection_data(self):
        return [
            self.type,
            self.account,
        ]

    def create_test_table_manager(self):
        from tests.snowflake_data_source_fixture import SnowflakeDataSourceFixture

        return SnowflakeDataSourceFixture(self)
