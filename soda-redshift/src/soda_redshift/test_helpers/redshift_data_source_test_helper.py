from __future__ import annotations

import os

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", None)
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "soda_sql")
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME", "")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")

from helpers.data_source_test_helper import DataSourceTestHelper


class RedshiftDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("REDSHIFT_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: redshift
            name: REDSHIFT_TEST_DS
            connection:
                host: '{REDSHIFT_HOST}'
                port: '{REDSHIFT_PORT}'
                database: '{REDSHIFT_DATABASE}'
                user: '{REDSHIFT_USERNAME}'
                password: '{REDSHIFT_PASSWORD}'
        """
