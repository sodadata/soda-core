from __future__ import annotations

import base64
import os
from io import StringIO

from helpers.data_source_test_helper import DataSourceTestHelper
from ruamel.yaml import YAML


class SnowflakeDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("SNOWFLAKE_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        # Built as a dict and dumped so the multi-line PEM and passphrase are quoted/escaped by the
        # YAML emitter instead of relying on hand-written indentation in a template string.
        data_source: dict = {
            "type": "snowflake",
            "name": self.name,
            "connection": {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                **self._auth_config(),
                "database": self.dataset_prefix[0],
            },
        }
        yaml = YAML()
        yaml.default_flow_style = False
        buf = StringIO()
        yaml.dump(data_source, buf)
        return buf.getvalue()

    @staticmethod
    def _auth_config() -> dict:
        # Prefer key-pair auth (CI); fall back to password for local setups.
        private_key_b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_ENCRYPTED")
        if not private_key_b64:
            return {"password": os.getenv("SNOWFLAKE_PASSWORD")}
        auth = {"private_key": base64.b64decode(private_key_b64).decode()}
        if passphrase := os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            auth["private_key_passphrase"] = passphrase
        return auth

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
