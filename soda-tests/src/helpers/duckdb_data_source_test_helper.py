from __future__ import annotations

from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper

from helpers.test_table import TestDataType


class DuckdbDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self):
        super().__init__()

    def _create_dataset_prefix(self) -> list[str]:
        schema_name: str = self._create_schema_name()
        return [schema_name]

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: duckdb
            name: duckdb-test-ds
            connection:
                database: ":memory:"
                schema: main
        """

    def _create_schema_name(self) -> Optional[str]:
        return "main"

    def _get_contract_data_type_dict(self) -> dict[str, str]:
        return {
            TestDataType.TEXT: "VARCHAR",
            TestDataType.INTEGER: "INTEGER",
            TestDataType.DECIMAL: "DOUBLE",
            TestDataType.DATE: "DATE",
            TestDataType.TIME: "TIME",
            TestDataType.TIMESTAMP: "TIMESTAMP",
            TestDataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
            TestDataType.BOOLEAN: "BOOLEAN",
        }
