import os

from helpers.data_source_fixture import DataSourceFixture


class VerticaDataSourceFixture(DataSourceFixture):
    def __init__(self, test_data_source: str):
        super().__init__(test_data_source=test_data_source)

    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source vertica": {
                "type": "vertica",
                "host": "localhost",
                "username": os.getenv("VERTICA_USERNAME", "dbadmin"),
                "password": os.getenv("VERTICA_PASSWORD", "password"),
                "database": os.getenv("VERTICA_DATABASE", "vmart"),
                "schema": schema_name if schema_name else os.getenv("VERTICA_SCHEMA", "public"),
            }
        }

    def _create_schema_if_not_exists_sql(self):
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE"
