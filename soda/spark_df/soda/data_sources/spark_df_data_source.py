from __future__ import annotations

from typing import Dict, List, Tuple

from pyspark.sql import SparkSession
from soda.common.logs import Logs
from soda.data_sources.spark_data_source import SparkSQLBase
from soda.data_sources.spark_df_connection import SparkDfConnection
from soda.data_sources.spark_df_schema_query import SparkDfTableColumnsQuery
from soda.execution.data_type import DataType
from soda.execution.schema_query import TableColumnsQuery


class DataSourceImpl(SparkSQLBase):
    TYPE = "spark-df"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "string": ["character varying", "varchar"],
        "int": ["integer", "int"],
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

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict, connection_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties, connection_properties)

    def connect(self, connection_properties: dict):
        spark_session = connection_properties.get("spark_session")
        self.connection = SparkDfConnection(spark_session)

    def quote_table(self, table_name) -> str:
        return f"{table_name}"

    def sql_get_table_columns(self, table_name: str):
        # This should not be called. Scan implementation should call other methods in this class
        raise NotImplementedError("Bug. Please report error code 88737")

    def get_table_columns(self, table_name: str, query_name: str) -> dict[str, str]:
        rows = self.get_table_column_rows(table_name)
        table_columns = {row[0]: row[1] for row in rows}
        return table_columns

    def get_table_column_rows(self, table_name: str) -> list[tuple]:
        """
        :return: List[Tuple] with each tuple (column_name: str, data_type: str) or None if the table does not exist
        """
        rows = None
        spark_session: SparkSession = self.connection.spark_session
        spark_table = spark_session.table(table_name)
        if spark_table:
            rows = []
            spark_table_schema = spark_table.schema
            for field in spark_table_schema.fields:
                column_row = [field.name, field.dataType.simpleString()]
                rows.append(column_row)
        return rows

    def create_table_columns_query(self, partition: Partition, schema_metric: SchemaMetric) -> TableColumnsQuery:
        return SparkDfTableColumnsQuery(partition, schema_metric)

    def sql_get_table_names_with_count(
        self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    ) -> str:
        return None
