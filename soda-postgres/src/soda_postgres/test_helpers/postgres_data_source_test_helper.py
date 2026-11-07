from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.sql_dialect import SqlDialect
from soda_postgres.common.data_sources.postgres_data_source import PostgresSqlDialect


class PostgresDataSourceTestHelper(DataSourceTestHelper):
    @classmethod
    def create_sql_dialects(cls) -> list[SqlDialect]:
        return [PostgresSqlDialect()]

    def _create_database_name(self) -> str | None:
        return os.getenv("POSTGRES_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: postgres
            name: {self.name}
            connection:
                host: {os.getenv("POSTGRES_HOST", "localhost")}
                user: {os.getenv("POSTGRES_USERNAME", "soda_test")}
                password: {os.getenv("POSTGRES_PASSWORD")}
                port: {int(os.getenv("POSTGRES_PORT", "5432"))}
                database: {self.dataset_prefix[0]}
        """
