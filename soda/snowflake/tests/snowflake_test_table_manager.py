from __future__ import annotations

import logging

from soda.data_sources.spark_df_data_source import DataSourceImpl
from tests.helpers.test_table_manager import TestTableManager

logger = logging.getLogger(__name__)


class SnowflakeTestTableManager(TestTableManager):
    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)

    def _create_schema_if_not_exists_sql(self) -> str:
        return f"CREATE DATABASE IF NOT EXISTS {self.schema_name}"

    def _use_schema_sql(self) -> str | None:
        return f"USE DATABASE {self.schema_name}"

    def _drop_schema_if_exists_sql(self):
        return f"DROP DATABASE IF EXISTS {self.schema_name} CASCADE"
