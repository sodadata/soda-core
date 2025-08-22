from __future__ import annotations

from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper


class DuckdbDataSourceTestHelper(DataSourceTestHelper):
    def _create_dataset_prefix(self) -> list[str]:
        schema_name: str = self._create_schema_name()
        return [schema_name]

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: duckdb
            name: {self.name}
            connection:
                database: ":memory:"
                schema: main
        """

    def _create_schema_name(self) -> Optional[str]:
        return "main"

    def drop_test_schema_if_exists(self) -> None:
        """
        In-memory DuckDB does not support schemas, so this method is a no-op.
        """
