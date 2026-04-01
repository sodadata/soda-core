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
            name: {self.name}
            connection:
                account: {os.getenv("SNOWFLAKE_ACCOUNT")}
                user: {os.getenv("SNOWFLAKE_USER")}
                password: {os.getenv("SNOWFLAKE_PASSWORD")}
                database: {self.dataset_prefix[0]}
        """

    def _adjust_schema_name(self, schema_name: str) -> str:
        return schema_name.upper()

    def _snapshot_passthrough_queries(self) -> dict:
        from soda_core.common.data_source_results import QueryResult

        # Snowflake lazily runs SELECT CURRENT_WAREHOUSE() to detect the active warehouse.
        # This is session-level metadata that shouldn't be part of per-test snapshots.
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "SODA_TESTING_WH")
        return {
            "SELECT CURRENT_WAREHOUSE()": QueryResult(rows=[(warehouse,)], columns=None),
        }
