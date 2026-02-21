from __future__ import annotations

import os
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper

TRINO_HOST = os.getenv("TRINO_HOST", "")
TRINO_USERNAME = os.getenv("TRINO_USERNAME", "")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "")
TRINO_PORT = int(os.getenv("TRINO_PORT", 443))
TRINO_VERIFY = os.getenv("TRINO_VERIFY", "true")


class TrinoDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> Optional[str]:
        if TRINO_CATALOG is None or TRINO_CATALOG == "":
            raise ValueError("TRINO_CATALOG is not set, check .env file")
        return TRINO_CATALOG

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
                user: {TRINO_USERNAME}
                password: '{TRINO_PASSWORD}'
                catalog: '{TRINO_CATALOG}'
                port: {TRINO_PORT}
                verify: {TRINO_VERIFY}
        """
