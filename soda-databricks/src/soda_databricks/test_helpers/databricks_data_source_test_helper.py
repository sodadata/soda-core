from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper


class DatabricksDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("DATABRICKS_CATALOG", "unity_catalog")

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
