from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestDataType


class BigQueryDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("BIGQUERY_DATABASE", "soda-testing-su5upe")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: bigquery
            name: BIGQUERY_TEST_DS
            connection:
                account_info_json: '{os.getenv("BIGQUERY_ACCOUNT_INFO_JSON", "")}'
                location: '{os.getenv("BIGQUERY_LOCATION", "US")}'

        """

    def _get_create_table_sql_type_dict(self) -> dict[str, str]:
        return {
            TestDataType.TEXT: "STRING",
            TestDataType.INTEGER: "INT64",
            TestDataType.DECIMAL: "FLOAT64",
            TestDataType.DATE: "DATE",
            TestDataType.TIME: "TIME",
            TestDataType.TIMESTAMP: "TIMESTAMP",
            TestDataType.TIMESTAMP_TZ: "TIMESTAMP",  # BigQuery does not have a separate TZ type; it's always in UTC
            TestDataType.BOOLEAN: "BOOL",
        }
