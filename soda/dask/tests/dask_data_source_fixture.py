from __future__ import annotations

import logging

from helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class DaskDataSourceFixture(DataSourceFixture):
    def __init__(self, test_data_source: str):
        super().__init__(test_data_source)

    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {"data_source dask": {"type": "dask"}}

    def _create_schema_if_not_exists_sql(self) -> str:
        ...

    def _use_schema_sql(self) -> str | None:
        ...

    def _drop_schema_if_exists_sql(self):
        ...
