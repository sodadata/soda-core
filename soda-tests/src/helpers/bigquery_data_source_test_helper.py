from __future__ import annotations

import json
import os

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestDataType


class BigQueryDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        # Parse the dataset name from the account info json
        account_info_json = json.loads(os.getenv("BIGQUERY_ACCOUNT_INFO_JSON", "{}"))
        database_name = account_info_json.get("project_id", "soda-testing-dataset")
        return database_name

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

        """
        # location: '{os.getenv("BIGQUERY_LOCATION", "US")}'

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

    def _get_contract_data_type_dict(self) -> dict[str, str]:
        """
        DataSourceTestHelpers can override this method as an easy way
        to customize the get_schema_check_sql_type behavior
        """
        return self._get_create_table_sql_type_dict()
