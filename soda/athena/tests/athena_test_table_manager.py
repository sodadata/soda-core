import decimal
import logging
from datetime import datetime

from pyathena import OperationalError
from pyspark.sql import types
from soda.data_sources.spark_df_data_source import DataSourceImpl
from soda.execution.data_type import DataType
from tests.helpers.test_table import TestTable
from tests.helpers.test_table_manager import TestTableManager

logger = logging.getLogger(__name__)


class AthenaTestTableManager(TestTableManager):

    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)

    def drop_schema_if_exists(self):
        try:
            super().drop_schema_if_exists()
        except Exception as e:
            # drop database if exists throws an exception that can be ignored
            if isinstance(e, OperationalError) and "Database does not exist" in str(e):
                logger.debug("Ignoring drop database exception")
            else:
                raise e

    def _create_test_table_sql(self, test_table: TestTable) -> str:
        sql = super()._create_test_table_sql(test_table)

        sql += f"LOCATION '{self.data_source.athena_staging_dir}data/{test_table.unique_table_name}/' "

        return sql
