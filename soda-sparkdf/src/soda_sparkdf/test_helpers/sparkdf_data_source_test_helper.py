from __future__ import annotations

import os
import shutil
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper


class SparkDataFrameDataSourceTestHelper(DataSourceTestHelper):
    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        self.test_dir = f"/tmp/soda_test_sparkdf_{self.name}"
        # Create the directory if it doesn't exist,
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)
        else:  # Remove it if it does.
            shutil.rmtree(self.test_dir)
            os.makedirs(self.test_dir)
        return f"""
            type: sparkdf
            name: {self.name}
            connection:
                new_session: true
                test_dir: {self.test_dir}
        """

    # We need these methods to comply with the rest of the test helper infrastructure
    def _create_database_name(self) -> Optional[str]:
        return None

    def _create_schema_name(self) -> Optional[str]:
        return "main"

    def _create_dataset_prefix(self) -> list[str]:
        schema_name: str = self._create_schema_name()
        return [schema_name]

    def drop_test_schema_if_exists(self) -> None:
        """
        In-memory SparkDF does not support schemas, so this is a no-op.
        """
