#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

from soda.common.exceptions import DataSourceConnectionError
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class DuckDBDataSource(DataSource):
    TYPE = "duckdb"

    # Maps synonym types for the convenience of use in checks.
    # Keys represent the data_source type, values are lists of "aliases" that can be used in SodaCL as synonyms.
    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "character varying": ["VARCHAR"],
        "double precision": ["DOUBLE"],
        "timestamp without time zone": ["TIMESTAMP"],
        "timestamp with time zone": ["TIMESTAMP WITH TIME ZONE"],
    }

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "VARCHAR",
        DataType.INTEGER: "INTEGER",
        DataType.DECIMAL: "DECIMAL",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
        DataType.BOOLEAN: "BOOLEAN",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        DataType.TEXT: "VARCHAR",
        DataType.INTEGER: "INTEGER",
        DataType.DECIMAL: "DECIMAL(18,3)",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
        DataType.BOOLEAN: "BOOLEAN",
    }

    NUMERIC_TYPES_FOR_PROFILING = [
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "DECIMAL",
        "DECIMAL(18,3)",
        "DOUBLE",
        "REAL",
    ]
    TEXT_TYPES_FOR_PROFILING = ["CHAR", "VARCHAR"]

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.path = data_source_properties.get("path")
        self.read_only = data_source_properties.get("read_only", False)
        self.duckdb_connection = data_source_properties.get("duckdb_connection")

    def connect(self):
        import duckdb

        try:
            if self.duckdb_connection:
                self.connection = self.duckdb_connection
            else:
                self.connection = duckdb.connect(
                    database=self.path if self.path else ":memory:", read_only=self.read_only
                )
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

        return self.connection

    def safe_connection_data(self):
        return [self.path, self.read_only]

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"REGEXP_MATCHES({expr}, '{regex_pattern}')"

    def default_casify_sql_function(self) -> str:
        """Returns the sql function to use for default casify."""
        return ""
