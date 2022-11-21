from __future__ import annotations

import logging

import dask.dataframe as dd
import pandas as pd
from dask_sql import Context
from helpers.data_source_fixture import DataSourceFixture
from helpers.test_table import TestTable
from soda.scan import Scan

logger = logging.getLogger(__name__)


class DaskDataSourceFixture(DataSourceFixture):
    def __init__(self, test_data_source: str):
        super().__init__(test_data_source)
        self.context = Context()

    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {"data_source dask": {"type": "dask"}}

    def _test_session_starts(self) -> None:
        scan = Scan()
        scan.add_dask_context(dask_context=self.context, data_source_name=self.data_source_name)
        self.data_source = scan._data_source_manager.get_data_source(self.data_source_name)
        scan._get_or_create_data_source_scan(self.data_source_name)

    def _create_and_insert_test_table(self, test_table: TestTable):
        a = dd.from_pandas(
            pd.DataFrame(data=test_table.values, columns=[test_column.name for test_column in test_table.test_columns])
        )
        return None

    def _create_schema_if_not_exists_sql(self) -> str:
        ...

    def _use_schema_sql(self) -> str | None:
        ...

    def _drop_schema_if_exists_sql(self):
        ...
