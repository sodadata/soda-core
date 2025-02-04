from __future__ import annotations

import os

from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)


class PostgresContractDataSourceTestHelper(ContractDataSourceTestHelper):

    def __init__(self):
        super().__init__()

    def _create_database_name(self) -> str | None:
        return os.getenv("POSTGRES_DATABASE", "sodasql")

    def _create_contract_data_source_yaml_dict(self, database_name: str | None, schema_name: str | None) -> dict:
        return {
            "type": "postgres",
            "name": "postgres_test_ds",
            "connection": {
                "host": "localhost",
                "user": os.getenv("POSTGRES_USERNAME", "sodasql"),
                "password": os.getenv("POSTGRES_PASSWORD"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "database": database_name,
            },
        }
