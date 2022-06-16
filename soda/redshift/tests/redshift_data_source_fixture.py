import logging

from tests.helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class RedshiftDataSourceFixture(DataSourceFixture):

    def __init__(self, test_data_source: str):
        super().__init__(test_data_source)

    def _drop_schema_if_exists_sql(self) -> str:
        return f"DROP DATABASE {self.schema_name}"

    def _create_schema_if_not_exists_sql(self) -> str:
        return f"CREATE DATABASE {self.schema_name}"
