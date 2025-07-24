from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper


class SnowflakeDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("SNOWFLAKE_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: snowflake
            name: SNOWFLAKE_TEST_DS
            connection:
                account: {os.getenv("SNOWFLAKE_ACCOUNT")}
                user: {os.getenv("SNOWFLAKE_USER")}
                password: {os.getenv("SNOWFLAKE_PASSWORD")}
                database: {self.dataset_prefix[0]}
        """

    def _adjust_schema_name(self, schema_name: str) -> str:
        return schema_name.upper()
