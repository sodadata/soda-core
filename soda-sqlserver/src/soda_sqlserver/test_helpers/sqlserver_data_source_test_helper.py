from __future__ import annotations

import os
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.sql_ast import DROP_TABLE
from soda_sqlserver.common.data_sources.sqlserver_data_source import (
    SqlServerDataSourceImpl,
    SqlServerSqlDialect,
)


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
            name: {self.name}
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
        """We overwrite this function because the old query in soda-library is a bit unreadable and does not work with Synapse.
        The logic is the same: drop all tables, and then drop the schema if it exists.
        This is a more "manual" approach, but it is more readable and works with Synapse."""
        # First find all the tables in the schema
        table_names: list[str] = self.query_existing_test_tables()
        data_source_impl: SqlServerDataSourceImpl = self.data_source_impl
        dialect: SqlServerSqlDialect = data_source_impl.sql_dialect
        for fully_qualified_table_name in table_names:
            table_identifier = f"{dialect.quote_default(fully_qualified_table_name.database_name)}.{dialect.quote_default(fully_qualified_table_name.schema_name)}.{dialect.quote_default(fully_qualified_table_name.table_name)}"
            drop_table_sql = dialect.build_drop_table_sql(DROP_TABLE(table_identifier))
            self.data_source_impl.execute_update(drop_table_sql)
        # Drop the schema if it exists.
        schema_name = self.dataset_prefix[self.data_source_impl.sql_dialect.get_schema_prefix_index()]
        if self._does_schema_exist(schema_name):
            self.data_source_impl.execute_update(f"DROP SCHEMA {dialect.quote_default(schema_name)};")

    def _does_schema_exist(self, schema_name: str) -> bool:
        """Check if the schema exists in the database."""
        query_result = self.data_source_impl.execute_query(
            f"SELECT name FROM sys.schemas WHERE name = '{schema_name}';"
        )
        return len(query_result.rows) > 0
