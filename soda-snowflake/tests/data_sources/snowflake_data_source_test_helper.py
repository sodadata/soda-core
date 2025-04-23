from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_table import (
    TestColumn,
    TestDataType,
    TestTable,
    TestTableSpecification,
)


class SnowflakeDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self):
        super().__init__()

    def _create_database_name(self) -> str | None:
        return os.getenv("SNOWFLAKE_DATABASE", "soda_test")

    def _create_data_source_yaml_dict(self) -> dict:
        return {
            "type": "snowflake",
            "name": "SNOWFLAKE_TEST_DS",
            "connection": {
                "host": os.getenv("SNOWFLAKE_HOST"),
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "database": self.dataset_prefix[0],
            },
        }

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: snowflake
            name: SNOWFLAKE_TEST_DS
            connection:
                host: {os.getenv("SNOWFLAKE_HOST")}
                account: {os.getenv("SNOWFLAKE_ACCOUNT")}
                user: {os.getenv("SNOWFLAKE_USER")}
                password: {os.getenv("SNOWFLAKE_PASSWORD")}
                database: {self.dataset_prefix[0]}
        """

    def create_test_schema_if_not_exists_sql(self) -> str:
        sql_dialect: "SqlDialect" = self.data_source_impl.sql_dialect
        schema_name: str = self.dataset_prefix[1]
        return f"CREATE SCHEMA IF NOT EXISTS {sql_dialect.quote_default(schema_name)};"

    def _adjust_schema_name(self, schema_name: str) -> str:
        return schema_name.upper()

    def _get_contract_data_type_dict(self) -> dict[str, str]:
        """
        DataSourceTestHelpers can override this method as an easy way
        to customize the get_schema_check_sql_type behavior
        """
        return {
            TestDataType.TEXT: "TEXT",
            TestDataType.INTEGER: "NUMBER",
            TestDataType.DECIMAL: "FLOAT",
            TestDataType.DATE: "DATE",
            TestDataType.TIME: "TIME",
            TestDataType.TIMESTAMP: "TIMESTAMP_NTZ",
            TestDataType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
            TestDataType.BOOLEAN: "BOOLEAN",
        }
