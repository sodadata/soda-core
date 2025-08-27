from __future__ import annotations

import logging
import os

from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


ATHENA_ACCESS_KEY_ID = os.getenv("ATHENA_ACCESS_KEY_ID", None)
ATHENA_SECRET_ACCESS_KEY = os.getenv("ATHENA_SECRET_ACCESS_KEY", None)
ATHENA_S3_TEST_DIR = os.getenv("ATHENA_S3_TEST_DIR")
# Drop the extra / at the end of the S3 test dir. This gives conflicts with the location to clean up later.
ATHENA_S3_TEST_DIR = ATHENA_S3_TEST_DIR[:-1] if ATHENA_S3_TEST_DIR.endswith("/") else ATHENA_S3_TEST_DIR
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "eu-west-1")
ATHENA_CATALOG = os.getenv("ATHENA_CATALOG", "awsdatacatalog")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP")


class AthenaDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self, name: str):
        super().__init__(name)
        self.s3_test_dir = ATHENA_S3_TEST_DIR
        if self.s3_test_dir.endswith("/"):
            self.s3_test_dir = self.s3_test_dir[:-1]

    def _create_database_name(self) -> str | None:
        return ATHENA_CATALOG

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
                type: athena
                name: {self.name}
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: {ATHENA_SECRET_ACCESS_KEY}
                    staging_dir: {ATHENA_S3_TEST_DIR}/staging-dir
                    region_name: {ATHENA_REGION_NAME}
                    catalog: {ATHENA_CATALOG}
                    {f"work_group: {ATHENA_WORKGROUP}" if ATHENA_WORKGROUP else ""}
            """

    def _get_3_schema_dir(self):
        schema_name = self.dataset_prefix[self.data_source_impl.sql_dialect.get_schema_prefix_index()]
        return f"{self.s3_test_dir}/staging-dir/{ATHENA_CATALOG}/{schema_name}"

    def drop_test_schema_if_exists(self) -> str:
        super().drop_test_schema_if_exists()
        self.data_source_impl._delete_s3_files(self._get_3_schema_dir())
