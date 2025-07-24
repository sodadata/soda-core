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

    def drop_test_schema_if_exists_sql(self):
        schema_index = self.data_source_impl.sql_dialect.get_schema_prefix_index()
        schema_name = self.dataset_prefix[schema_index]

        # Note: this is a copy from library.
        # However, we only use the drop table statement, as we do not create any primary keys or something like that (at this point)
        return f"""
        /* Drop all tables */
        DECLARE @name VARCHAR(128)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 ORDER BY [name])

        WHILE @name IS NOT NULL
        BEGIN
            SELECT @SQL = 'DROP TABLE [{schema_name}].[' + RTRIM(@name) +']'
            EXEC (@SQL)
            PRINT 'Dropped Table: ' + @name
            SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 AND [name] > @name ORDER BY [name])
        END
        """
