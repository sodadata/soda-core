from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_table import (
    TestColumn,
    TestDataType,
    TestTable,
    TestTableSpecification,
)


class DatabricksDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self):
        super().__init__()

    def _create_database_name(self) -> str | None:
        return os.getenv("DATABRICKS_DATABASE", "soda_test")

    def _create_data_source_yaml_dict(self) -> dict:
        return {
            "type": "databricks",
            "name": "DATABRICKS_TEST_DS",
            "connection": {
                "host": os.getenv("DATABRICKS_HOST"),
                "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
                "access_token": os.getenv("DATABRICKS_TOKEN"),
                "catalog": os.getenv("DATABRICKS_CATALOG", "unity_catalog"),
            },
        }

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: databricks
            name: DATABRICKS_TEST_DS
            connection:
                host: {os.getenv("DATABRICKS_HOST")}
                http_path: {os.getenv("DATABRICKS_HTTP_PATH")}
                access_token: {os.getenv("DATABRICKS_TOKEN")}
                catalog: {os.getenv("DATABRICKS_CATALOG", "unity_catalog")}
        """

    def create_test_schema_if_not_exists_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.dataset_prefix[1]};"

    def _get_contract_data_type_dict(self) -> dict[str, str]:
        return {
            TestDataType.TEXT: "varchar",
            TestDataType.INTEGER: "integer",
            TestDataType.DECIMAL: "double precision",
            TestDataType.DATE: "date",
            TestDataType.TIME: "time",
            TestDataType.TIMESTAMP: "timestamp without time zone",
            TestDataType.TIMESTAMP_TZ: "timestamp with time zone",
            TestDataType.BOOLEAN: "boolean",
        }
