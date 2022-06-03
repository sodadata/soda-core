from __future__ import annotations

from soda.common.logs import Logs
from soda.data_sources.spark_data_source import SparkSQLBase
from soda.data_sources.spark_df_connection import SparkDfConnection
from soda.data_sources.spark_df_schema_query import SparkDfSchemaQuery
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType
from soda.execution.schema_query import SchemaQuery


class DataSourceImpl(SparkSQLBase, DataSource):
    TYPE = "spark-df"

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

    def sql_to_get_column_metadata_for_table(self, table_name: str):
        schema = self.connection.spark_session.table(table_name).schema
        raise NotImplementedError("TODO")

    def create_schema_query(self, partition: Partition, schema_metric: SchemaMetric) -> SchemaQuery:
        return SparkDfSchemaQuery(partition, schema_metric)

    def sql_get_table_names_with_count(
        self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    ) -> str:
        return None
