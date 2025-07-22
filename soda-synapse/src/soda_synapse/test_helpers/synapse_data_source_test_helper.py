from __future__ import annotations

import os
from typing import Optional

from soda_sqlserver.test_helpers.sqlserver_data_source_test_helper import (
    SqlServerDataSourceTestHelper,
)


class SynapseDataSourceTestHelper(SqlServerDataSourceTestHelper):
    def _create_database_name(self) -> Optional[str]:
        return os.getenv("SYNAPSE_DATABASE", "sodatestingsynapse")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                host: '{os.getenv("SYNAPSE_HOST", "localhost")}'
                port: '{os.getenv("SYNAPSE_PORT", "1433")}'
                database: '{os.getenv("SYNAPSE_DATABASE", "sodatestingsynapse")}'
                authentication: '{os.getenv("SYNAPSE_AUTHENTICATION_TYPE", "activedirectoryserviceprincipal")}'
                client_id: '{os.getenv("SYNAPSE_CLIENT_ID")}'
                client_secret: '{os.getenv("SYNAPSE_CLIENT_SECRET")}'
                trust_server_certificate: true
                driver: '{os.getenv("SYNAPSE_DRIVER", "ODBC Driver 18 for SQL Server")}'
                autocommit: true
        """
