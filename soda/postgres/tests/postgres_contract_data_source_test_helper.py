import os

from contracts.helpers.test_data_source import ContractDataSourceTestHelper


class PostgresContractDataSourceTestHelper(ContractDataSourceTestHelper):

    def __init__(self):
        super().__init__()

    def _create_contract_data_source_yaml_dict(self) -> dict:
        return {
            "type": "postgres",
            "name": "postgres_test_ds",
            "connection": {
                "host": "localhost",
                "user": os.getenv("POSTGRES_USERNAME", "sodasql"),
                "password": os.getenv("POSTGRES_PASSWORD"),
                "database": os.getenv("POSTGRES_DATABASE", "sodasql"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
            }
        }
