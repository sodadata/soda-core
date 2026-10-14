from __future__ import annotations

import base64
import json
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
                {self._auth_yaml_str()}
                database: {self.dataset_prefix[0]}
        """

    @staticmethod
    def _auth_yaml_str() -> str:
        # Prefer key-pair auth (CI); fall back to password for local setups.
        # Values are JSON-encoded so the multi-line PEM stays a valid single-line YAML scalar.
        private_key_b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_ENCRYPTED")
        if private_key_b64:
            private_key = base64.b64decode(private_key_b64).decode()
            passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            auth = f"private_key: {json.dumps(private_key)}"
            if passphrase:
                auth += f"\n                private_key_passphrase: {json.dumps(passphrase)}"
            return auth
        return f"password: {os.getenv('SNOWFLAKE_PASSWORD')}"

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
