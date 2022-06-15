from __future__ import annotations

import logging
import os

from soda.data_sources.spark_df_data_source import DataSourceImpl
from tests.helpers.test_table_manager import TestTableManager

logger = logging.getLogger(__name__)


class PostgresTestTableManager(TestTableManager):
    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)
        is_local_dev = os.getenv("GITHUB_ACTIONS") is None
        is_schema_reuse_disabled = os.getenv("POSTGRES_REUSE_SCHEMA", "").lower() == "disabled"
        self.local_dev_schema_reused = is_local_dev and not is_schema_reuse_disabled

    def initialize_schema(self):
        if not self.local_dev_schema_reused:
            self.drop_schema_if_exists()
        self._create_schema_if_not_exists()

    def _use_schema_sql(self) -> str | None:
        return f"SET search_path TO {self.schema_name}"

    def drop_schema_if_exists(self):
        if not self.local_dev_schema_reused:
            drop_schema_if_exists_sql = self._drop_schema_if_exists_sql()
            self.update(drop_schema_if_exists_sql)

    def _create_schema_if_not_exists_sql(self):
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name} AUTHORIZATION CURRENT_USER"

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE"
