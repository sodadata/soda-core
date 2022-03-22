import logging
import re
from typing import Dict, List, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake import connector
from snowflake.connector.network import DEFAULT_SOCKET_CONNECT_TIMEOUT
from soda.common.exceptions import DataSourceConnectionError
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class DataSourceImpl(DataSource):
    TYPE = "snowflake"

    SCHEMA_CHECK_TYPES_MAPPING: Dict = {
        "TEXT": ["character varying", "varchar", "string"],
        "NUMBER": ["integer", "int"],
    }
    SQL_TYPE_FOR_CREATE_TABLE_MAP: Dict = {
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

    def connect(self, connection_properties):
        self.connection_properties = connection_properties
        try:
            self.connection = connector.connect(
                user=connection_properties.get("username"),
                password=connection_properties.get("password"),
                account=connection_properties.get("account"),
                data_source=connection_properties.get("data_source"),
                database=connection_properties.get("database"),
                schema=connection_properties.get("schema"),
                warehouse=connection_properties.get("warehouse"),
                login_timeout=connection_properties.get("connection_timeout", DEFAULT_SOCKET_CONNECT_TIMEOUT),
                role=connection_properties.get("role"),
                client_session_keep_alive=connection_properties.get("client_session_keep_alive"),
                session_parameters=connection_properties.get("session_params"),
            )
            return self.connection

        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def __get_private_key(self):
        if not (self.connection_properties.get("private_key_path") or self.connection_properties.get("private_key")):
            return None

        if self.connection_properties.get("private_key_passphrase"):
            encoded_passphrase = self.connection_properties.get("private_key_passphrase").encode()
        else:
            encoded_passphrase = None

        pk_bytes = None
        if self.connection_properties.get("private_key"):
            pk_bytes = self.connection_properties.get("private_key").encode()
        elif self.connection_properties.get("private_key_path"):
            with open(self.connection_properties.get("private_key_path"), "rb") as pk:
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

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):
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
        self, include_tables: Optional[List[str]] = None, exclude_tables: Optional[List[str]] = None
    ) -> str:
        table_filter_expression = self.sql_table_filter_based_on_includes_excludes(
            "table_name", "row_count", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return f"SELECT table_name, row_count \n" f"FROM information_schema.tables" f"{where_clause}"

    @staticmethod
    def format_column_default(identifier: str) -> str:
        return identifier.upper()

    @staticmethod
    def format_type_default(identifier: str) -> str:
        return identifier.upper()

    def safe_connection_data(self):
        return [
            self.type,
            self.connection_properties.get("account"),
        ]
