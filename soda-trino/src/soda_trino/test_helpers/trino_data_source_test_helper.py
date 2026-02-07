from __future__ import annotations

import os

TRINO_HOST = os.getenv("TRINO_HOST", "")
TRINO_USERNAME = os.getenv("TRINO_USERNAME", "")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "")

from helpers.data_source_test_helper import DataSourceTestHelper


class TrinoDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("REDSHIFT_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: trino
            name: {self.name}
            connection:
                host: '{TRINO_HOST}'
                username: {TRINO_USERNAME}
                password: '{TRINO_PASSWORD}'
                catalog: '{TRINO_CATALOG}'
        """
