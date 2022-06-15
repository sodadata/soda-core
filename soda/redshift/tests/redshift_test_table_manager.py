import decimal
import logging
from datetime import datetime

from pyspark.sql import types
from soda.data_sources.spark_df_data_source import DataSourceImpl
from soda.execution.data_type import DataType
from tests.helpers.test_table import TestTable
from tests.helpers.test_table_manager import TestTableManager

logger = logging.getLogger(__name__)


class RedshiftTestTableManager(TestTableManager):

    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)

    def _drop_schema_if_exists_sql(self) -> str:
        return f"DROP DATABASE {self.schema_name}"

    def _create_schema_if_not_exists_sql(self) -> str:
        return f"CREATE DATABASE {self.schema_name}"
