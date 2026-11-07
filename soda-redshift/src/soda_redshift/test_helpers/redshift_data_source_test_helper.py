from __future__ import annotations

import os

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", None)
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "soda_test")
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME", "")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")

from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.sql_dialect import SqlDialect
from soda_redshift.common.data_sources.redshift_data_source import RedshiftSqlDialect


class RedshiftDataSourceTestHelper(DataSourceTestHelper):
    @classmethod
    def create_sql_dialects(cls) -> list[SqlDialect]:
        return [RedshiftSqlDialect()]

    def _create_database_name(self) -> str | None:
        return os.getenv("REDSHIFT_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: redshift
            name: {self.name}
            connection:
                host: '{REDSHIFT_HOST}'
                port: {REDSHIFT_PORT}
                database: '{REDSHIFT_DATABASE}'
                user: '{REDSHIFT_USERNAME}'
                password: '{REDSHIFT_PASSWORD}'
                keepalives_idle: 60
                keepalives_interval: 30
                keepalives_count: 3
        """
