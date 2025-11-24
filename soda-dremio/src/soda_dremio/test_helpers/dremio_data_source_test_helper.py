from __future__ import annotations

import os
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper


class DremioDataSourceTestHelper(DataSourceTestHelper):
    def _create_schema_name(self) -> Optional[str]:
        # Dremio can only reliably write to the $scratch space
        # Use a multi-level schema name to expose any related bugs 
        schema: str = super()._create_schema_name()
        return f"$scratch.foo.bar.{schema}"

    def _create_dataset_prefix(self) -> list[str]:
        schema_name: str = self._create_schema_name()
        return [schema_name]

    def _create_database_name(self) -> str | None:
        return None

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: dremio
            name: {self.name}
            connection:
                host: {os.getenv("DREMIO_HOST", "localhost")}
                username: {os.getenv("DREMIO_USERNAME", "admin")}
                password: {os.getenv("DREMIO_PASSWORD", "admin1234")}
                port: {int(os.getenv("DREMIO_PORT", "32010"))}
        """

    def drop_schema_if_exists_sql(self, schema: str) -> str:
        return f"DROP FOLDER IF EXISTS {schema} CASCADE;"
