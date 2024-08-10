from __future__ import annotations

import os

from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper


class SnowflakeContractDataSourceTestHelper(ContractDataSourceTestHelper):

    def __init__(self):
        super().__init__()

    def _create_database_name(self) -> str | None:
        return os.getenv("SNOWFLAKE_DATABASE", "sodasql")

    def _create_contract_data_source_yaml_dict(
        self,
        database_name: str | None,
        schema_name: str | None
    ) -> dict:
        return {
            "type": "snowflake",
            "name": "snowflake_test_ds",
            "connection": {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USERNAME"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "schema": schema_name if schema_name else os.getenv("SNOWFLAKE_SCHEMA", "public"),
                "database": database_name
            }
        }

    def _create_schema_if_not_exists_sql(self):
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name} AUTHORIZATION CURRENT_USER"

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE"
