from __future__ import annotations

import logging
import os

from helpers.data_source_fixture import DataSourceFixture, is_cicd

logger = logging.getLogger(__name__)


class ImpalaDataSourceFixture(DataSourceFixture):
    def __init__(self, test_data_source: str):
        super().__init__(test_data_source=test_data_source)

    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source impala": {
                "type": "impala",
                "host": os.getenv("IMPALA_HOST", "localhost"),
                #"username": os.getenv("IMPALA_USERNAME", "impala"),
                #"password": os.getenv("IMPALA_PASSWORD"),
                #"database": os.getenv("IMPALA_DATABASE", "default"),
                "port": int(os.getenv("IMPALA_PORT", "21050")),
                "auth_mechanism": "NOSASL",
            }
        }

    def _create_schema_if_not_exists_sql(self):
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name}"
