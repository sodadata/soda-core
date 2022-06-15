import logging

from google.cloud import bigquery
from soda.data_sources.spark_df_data_source import DataSourceImpl
from tests.helpers.test_table_manager import TestTableManager

logger = logging.getLogger(__name__)


class BigQueryTestTableManager(TestTableManager):
    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)

    def _get_dataset_id(self):
        return f"{self.data_source.project_id}.{self.schema_name}"

    def _create_schema_if_not_exists(self):
        dataset_id = self._get_dataset_id()
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        self.data_source.client.create_dataset(dataset, timeout=30)

    def drop_schema_if_exists(self):
        dataset_id = self._get_dataset_id()
        self.data_source.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def _drop_test_table_sql(self, table_name):
        qualified_table_name = self.data_source.qualified_table_name(table_name)
        return f"DROP TABLE {qualified_table_name};"
