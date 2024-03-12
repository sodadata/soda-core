from __future__ import annotations

import logging
import os

from helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class FabricDataSourceFixture(DataSourceFixture):
    def __init__(self, test_data_source: str):
        super().__init__(test_data_source)

    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source fabric": {
                "type": "fabric",
                "driver": os.getenv("FABRIC_DRIVER"),
                "host": os.getenv("FABRIC_HOST"),
                "port": os.getenv("FABRIC_PORT", "1433"),
                "database": os.getenv("FABRIC_DATABASE"),
                "schema": schema_name if schema_name else os.getenv("FABRIC_SCHEMA", "dbo"),
                "tenant_id": os.getenv("FABRIC_TENANT_ID"),
                "tenant_client_id": os.getenv("FABRIC_CLIENT_ID"),
                "tenant_client_secret": os.getenv("FABRIC_CLIENT_SECRET"),
            }
        }

    def _create_schema_if_not_exists_sql(self) -> str:
        return (
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema_name}') "
            f"BEGIN EXEC('CREATE SCHEMA {self.schema_name};') "
            f"END"
        )

    def _drop_schema_if_exists_sql(self):
        return (
            f"IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema_name}') "
            f"BEGIN EXEC('DROP SCHEMA {self.schema_name};') "
            f"END"
        )
