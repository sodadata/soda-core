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

    def _get_contract_data_type_dict(self) -> dict[str, str]:
        """
        DataSourceTestHelpers can override this method as an easy way
        to customize the get_schema_check_sql_type behavior
        """
        return self._get_create_table_sql_type_dict()

    def quote_column(self, column_name: str) -> str:
        return self.data_source_impl.sql_dialect.quote_default(column_name)

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp('{datetime_in_iso8601}')"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc(timestamp({timestamp_literal}), day)"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval 1 day"
