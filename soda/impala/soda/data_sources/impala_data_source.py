import logging
import re
from typing import Union

from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class ImpalaDataSource(DataSource):
    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "string": ["varchar", "char"],
        "int": ["integer", "int"],
    }
    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "string",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "decimal",
        DataType.DATE: "timestamp",  # No date support in Impala
        DataType.TIME: "timestamp",
        DataType.TIMESTAMP: "timestamp",
        DataType.TIMESTAMP_TZ: "timestamp",  # No timezone support in Impala
        DataType.BOOLEAN: "boolean",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        DataType.TEXT: "string",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "decimal",
        DataType.DATE: "timestamp",  # No date support in Impala
        DataType.TIME: "timestamp",
        DataType.TIMESTAMP: "timestamp",
        DataType.TIMESTAMP_TZ: "timestamp",  # No timezone support in Impala
        DataType.BOOLEAN: "boolean",
    }

    NUMERIC_TYPES_FOR_PROFILING = [
        "int",
        "bigint",
        "smallint",
        "tinyint",
        "double",
        "float",
        "decimal",
    ]
    TEXT_TYPES_FOR_PROFILING = ["string", "varchar", "char"]

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.host = data_source_properties.get("host")
        self.port = data_source_properties.get("port")
        self.username = data_source_properties.get("username")
        self.password = data_source_properties.get("password")
        self.http_path = data_source_properties.get("http_path", None)
        self.use_http_transport = data_source_properties.get("use_http_transport", False)
        self.use_ssl = data_source_properties.get("use_ssl", False)
        self.auth_mechanism = data_source_properties.get("auth_mechanism", None)

    def connect(self):
        import impala.dbapi

        if isinstance(self.host, str) and len(self.host) > 0:
            self.connection = impala.dbapi.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                http_path=self.http_path,
                use_http_transport=self.use_http_transport,
                use_ssl=self.use_ssl,
                auth_mechanism=self.auth_mechanism,
                port=self.port,
                database=self.database,
            )
        else:
            raise ConnectionError(f"Invalid impala connection properties: invalid host: {self.host}")
        return self.connection

    def safe_connection_data(self):
        return [
            self.type,
            self.host,
            self.port,
            self.database,
        ]

    def sql_get_table_columns(
        self,
        table_name: str,
        included_columns: Union[list[str], None] = None,
        excluded_columns: Union[list[str], None] = None,
    ):
        return f"DESCRIBE {table_name}"

    def sql_find_table_names(
        self,
        filter: Union[str, None] = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
        table_column_name: str = "table_name",
        schema_column_name: str = "table_schema",
    ) -> str:
        from_clause = f" FROM {self.database}" if self.database else ""
        return f"SHOW TABLES{from_clause}"

    def sql_get_table_names_with_count(
        self, include_tables: Union[list[str], None] = None, exclude_tables: Union[list[str], None] = None
    ) -> str:
        return ""

    def regex_replace_flags(self) -> str:
        return ""

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def default_casify_table_name(self, identifier: str) -> str:
        return identifier.lower()

    def quote_table(self, table_name) -> str:
        return f"`{table_name}`"

    def quote_column(self, column_name: str) -> str:
        return f"`{column_name}`"
