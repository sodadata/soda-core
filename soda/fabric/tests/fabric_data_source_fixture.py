import os

from sqlserver_data_source_fixture import SQLServerDataSourceFixture


class FabricDataSourceFixture(SQLServerDataSourceFixture):
    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source fabric": {
                "type": "fabric",
                "host": os.getenv("FABRIC_ENDPOINT"),
                "database": os.getenv("FABRIC_DWH"),
                "schema": schema_name or os.getenv("FABRIC_SCHEMA", "dbo"),
                "driver": os.getenv("FABRIC_DRIVER", "ODBC Driver 18 for SQL Server"),
                "client_id": os.getenv("FABRIC_CLIENT_ID"),
                "client_secret": os.getenv("FABRIC_CLIENT_SECRET"),
                "encrypt": True,
                "authentication": os.getenv("FABRIC_AUTHENTICATION", "CLI"),
            }
        }
