from __future__ import annotations

import logging
import os
from helpers.data_source_fixture import TestTable

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

    def _drop_schema_if_exists_sql(self) -> str:
        return (
            f"IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema_name}') "
            f"BEGIN EXEC('DROP SCHEMA {self.schema_name};') "
            f"END"
        )

    def _drop_all_tables_in_schema_sql(self):
        return """
            DECLARE @sql NVARCHAR(MAX) = N'';

            SELECT @sql += 'DROP TABLE [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '];' + CHAR(13)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @schemaName
            AND TABLE_TYPE = 'BASE TABLE';

            EXEC sp_executesql @sql, N'@schemaName NVARCHAR(MAX)', @schemaName = %s;
            """ % self.schema_name

    def _drop_all_tables_in_schema(self):
        drop_all_tables_in_schema_sql = self._drop_all_tables_in_schema_sql()
        self._update(drop_all_tables_in_schema_sql, self.schema_data_source.connection)

    def _get_dataset_id(self):
        return f"{self.schema_data_source.project_id}.{self.schema_name}"

    def _create_schema_if_not_exists_sql(self):
        return f"""
        IF NOT EXISTS ( SELECT  *
                        FROM    sys.schemas
                        WHERE   name = N'{self.schema_name}' )
        EXEC('CREATE SCHEMA [{self.schema_name}]');
        """

    def _drop_schema_if_exists(self):
        return f"""
        /* Drop all non-system stored procs */
        DECLARE @name VARCHAR(128)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'P' AND category = 0 ORDER BY [name])

        WHILE @name is not null
        BEGIN
            SELECT @SQL = 'DROP PROCEDURE [{self.schema_name}].[' + RTRIM(@name) +']'
            EXEC (@SQL)
            PRINT 'Dropped Procedure: ' + @name
            SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'P' AND category = 0 AND [name] > @name ORDER BY [name])
        END
        GO

        /* Drop all views */
        DECLARE @name VARCHAR(128)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 ORDER BY [name])

        WHILE @name IS NOT NULL
        BEGIN
            SELECT @SQL = 'DROP VIEW [{self.schema_name}].[' + RTRIM(@name) +']'
            EXEC (@SQL)
            PRINT 'Dropped View: ' + @name
            SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 AND [name] > @name ORDER BY [name])
        END
        GO

        /* Drop all functions */
        DECLARE @name VARCHAR(128)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND category = 0 ORDER BY [name])

        WHILE @name IS NOT NULL
        BEGIN
            SELECT @SQL = 'DROP FUNCTION [{self.schema_name}].[' + RTRIM(@name) +']'
            EXEC (@SQL)
            PRINT 'Dropped Function: ' + @name
            SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND category = 0 AND [name] > @name ORDER BY [name])
        END
        GO

        /* Drop all Foreign Key constraints */
        DECLARE @name VARCHAR(128)
        DECLARE @constraint VARCHAR(254)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY TABLE_NAME)

        WHILE @name is not null
        BEGIN
            SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
            WHILE @constraint IS NOT NULL
            BEGIN
                SELECT @SQL = 'ALTER TABLE [{self.schema_name}].[' + RTRIM(@name) +'] DROP CONSTRAINT [' + RTRIM(@constraint) +']'
                EXEC (@SQL)
                PRINT 'Dropped FK Constraint: ' + @constraint + ' on ' + @name
                SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME <> @constraint AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
            END
        SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY TABLE_NAME)
        END
        GO

        /* Drop all Primary Key constraints */
        DECLARE @name VARCHAR(128)
        DECLARE @constraint VARCHAR(254)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_NAME)

        WHILE @name IS NOT NULL
        BEGIN
            SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
            WHILE @constraint is not null
            BEGIN
                SELECT @SQL = 'ALTER TABLE [{self.schema_name}].[' + RTRIM(@name) +'] DROP CONSTRAINT [' + RTRIM(@constraint)+']'
                EXEC (@SQL)
                PRINT 'Dropped PK Constraint: ' + @constraint + ' on ' + @name
                SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' AND CONSTRAINT_NAME <> @constraint AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
            END
        SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_NAME)
        END
        GO

        /* Drop all tables */
        DECLARE @name VARCHAR(128)
        DECLARE @SQL VARCHAR(254)

        SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 ORDER BY [name])

        WHILE @name IS NOT NULL
        BEGIN
            SELECT @SQL = 'DROP TABLE [{self.schema_name}].[' + RTRIM(@name) +']'
            EXEC (@SQL)
            PRINT 'Dropped Table: ' + @name
            SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 AND [name] > @name ORDER BY [name])
        END
        GO
        """

    def _drop_test_table_sql(self, table_name: str) -> str:
        return (
            f"DROP TABLE IF EXISTS {self.schema_name}.{table_name}"
        )

    def drop_test_table(self, table_name: str) -> None:
        drop_test_table_sql = self._drop_test_table_sql(table_name)
        self._update(drop_test_table_sql, self.schema_data_source.connection)

    def ensure_test_table(self, test_table: TestTable) -> str:
        self._drop_schema_if_exists()
        existing_test_table_names = self._get_existing_test_table_names()
        existing_test_table_names_lower = [table_name.lower() for table_name in existing_test_table_names]
        if test_table.unique_table_name.lower() not in existing_test_table_names_lower:
            obsolete_table_names = [
                existing_test_table
                for existing_test_table in existing_test_table_names
                if existing_test_table.lower().startswith(f"sodatest_{test_table.name.lower()}_")
            ]
            if obsolete_table_names:
                for obsolete_table_name in obsolete_table_names:
                    self._drop_test_table(obsolete_table_name)
            self._create_and_insert_test_table(test_table)
            self.data_source.commit()

            # Run table analyze so that internal data source statistics are refreshed before running any tests.
            self.data_source.analyze_table(test_table.unique_table_name)
        return test_table.unique_view_name if test_table.create_view else test_table.unique_table_name

    def _create_view_from_table_sql(self, test_table: TestTable):
        # @TOOD
        pass
