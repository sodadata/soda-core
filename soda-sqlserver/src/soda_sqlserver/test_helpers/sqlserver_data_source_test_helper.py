from __future__ import annotations

import os
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper


class SqlServerDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> Optional[str]:
        return os.getenv("SQLSERVER_DATABASE", "master")

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: sqlserver
            name: SQLSERVER_TEST_DS
            connection:
                host: '{os.getenv("SQLSERVER_HOST", "localhost")}'
                port: '{os.getenv("SQLSERVER_PORT", "1433")}'
                database: '{os.getenv("SQLSERVER_DATABASE", "master")}'
                user: '{os.getenv("SQLSERVER_USERNAME", "SA")}'
                password: '{os.getenv("SQLSERVER_PASSWORD", "Password1!")}'
                trust_server_certificate: true
                driver: '{os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")}'
        """

    def drop_test_schema_if_exists(self) -> None:
        # First find all the tables in the schema
        table_names: list[str] = self.query_existing_test_table_names(return_fully_qualified_table_names=True)
        for fully_qualified_table_name in table_names:
            table_identifier = f"{fully_qualified_table_name.database_name}.{fully_qualified_table_name.schema_name}.{fully_qualified_table_name.table_name}"
            self.data_source_impl.execute_update(f"DROP TABLE {table_identifier};")
        # Drop the schema if we found any tables
        if len(table_names) > 0:
            schema_name = self.dataset_prefix[self.data_source_impl.sql_dialect.get_schema_prefix_index()]
            self.data_source_impl.execute_update(f"DROP SCHEMA {schema_name};")
