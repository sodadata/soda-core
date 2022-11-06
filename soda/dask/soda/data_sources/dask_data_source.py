from __future__ import annotations

import logging

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

    def test(self, sql: str) -> None:
        logging.debug(f"Running SQL query:\n{sql}")
        df = self.connection.context.sql(sql)
        df.compute()
