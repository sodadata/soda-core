from __future__ import annotations

import os

from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper


class PostgresDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self):
        super().__init__()

    def _create_database_name(self) -> str | None:
        return os.getenv("POSTGRES_DATABASE", "soda_test")

    def _create_data_source_yaml_dict(self) -> dict:
        return {
            "type": "postgres",
            "name": "postgres_test_ds",
            "connection": {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "user": os.getenv("POSTGRES_USERNAME", "soda_test"),
                "password": os.getenv("POSTGRES_PASSWORD"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "database": self.database_name,
            },
            "format_regexes": {"single_digit_test_format": "^[0-9]$"},
        }
