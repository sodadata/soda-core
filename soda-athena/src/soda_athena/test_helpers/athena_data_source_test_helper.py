from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper

ATHENA_ACCESS_KEY_ID = os.getenv("ATHENA_ACCESS_KEY_ID", None)
ATHENA_SECRET_ACCESS_KEY = os.getenv("ATHENA_SECRET_ACCESS_KEY", None)
ATHENA_S3_TEST_DIR = os.getenv("ATHENA_S3_TEST_DIR", None)
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "eu-west-1")


class AthenaDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return super()._create_database_name()

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
                type: athena
                name: ATHENA_TEST_DS
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: {ATHENA_SECRET_ACCESS_KEY}
                    staging_dir: {ATHENA_S3_TEST_DIR}
                    region_name: {ATHENA_REGION_NAME}
            """
