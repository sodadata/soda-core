from __future__ import annotations

import os
import tempfile
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper


class DuckdbDataSourceTestHelper(DataSourceTestHelper):
    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        # The DB file must live somewhere writable both on the host and
        # inside the memory_container (the repo bind mount is read-only
        # there). A bare relative path used to land in the pytest rootdir —
        # i.e. inside the repo.
        database_dir = os.getenv("DUCKDB_TEST_DATABASE_DIR", tempfile.gettempdir())
        database_path = os.path.join(database_dir, f"{self.name}.db")
        return f"""
            type: duckdb
            name: {self.name}
            connection:
                database: "{database_path}"
                schema: main
        """

    def _create_database_name(self) -> Optional[str]:
        return None

    def _create_schema_name(self) -> Optional[str]:
        return "main"

    def _create_dataset_prefix(self) -> list[str]:
        schema_name: str = self._create_schema_name()
        return [schema_name]

    def drop_test_schema_if_exists(self) -> None:
        """
        In-memory DuckDB does not support schemas, so this method is a no-op.
        """
