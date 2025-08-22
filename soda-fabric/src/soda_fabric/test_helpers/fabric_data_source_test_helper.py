from __future__ import annotations

import os
from typing import Optional

from soda_sqlserver.test_helpers.sqlserver_data_source_test_helper import (
    SqlServerDataSourceTestHelper,
)


class FabricDataSourceTestHelper(SqlServerDataSourceTestHelper):
    def _create_database_name(self) -> Optional[str]:
        return os.getenv("FABRIC_DATABASE", "soda-ci-fabric-warehouse")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: fabric
            name: {self.name}
            connection:
                host: '{os.getenv("FABRIC_HOST", "localhost")}'
                port: '{os.getenv("FABRIC_PORT", "1433")}'
                database: '{os.getenv("FABRIC_DATABASE", "soda-ci-fabric-warehouse")}'
                authentication: '{os.getenv("FABRIC_AUTHENTICATION_TYPE", "activedirectoryserviceprincipal")}'
                client_id: '{os.getenv("FABRIC_CLIENT_ID")}'
                client_secret: '{os.getenv("FABRIC_CLIENT_SECRET")}'
                trust_server_certificate: true
                driver: '{os.getenv("FABRIC_DRIVER", "ODBC Driver 18 for SQL Server")}'
        """
