from __future__ import annotations

import logging

from soda.data_sources.spark_df_data_source import DataSourceImpl
from tests.helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class SnowflakeDataSourceFixture(DataSourceFixture):
    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)

    def _create_schema_if_not_exists_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"

    def _use_schema_sql(self) -> str | None:
        return None

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE"
