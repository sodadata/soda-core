from __future__ import annotations

import os
from typing import Optional

from helpers.test_table import TestTable
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

    def _insert_test_table_rows_sql(self, test_table: TestTable) -> str:
        if test_table.row_values:

            def literalize_row(row: tuple) -> list[str]:
                return [self.data_source_impl.sql_dialect.literal(value) for value in row]

            literal_row_values = [literalize_row(row_values) for row_values in test_table.row_values]

            def format_literal_row_values(row: list[str]) -> str:
                return ",".join(row)

            rows_sql = " UNION ALL \nSELECT ".join([f"{format_literal_row_values(row)}" for row in literal_row_values])

            rows_sql = f"SELECT {rows_sql}"

            return self._insert_test_table_rows_sql_statement(
                test_table.qualified_name, rows_sql, list(test_table.columns.keys())
            )

    def _insert_test_table_rows_sql_statement(
        self, table_name_qualified_quoted: str, rows_sql: str, columns: list[str] = None
    ) -> str:
        return (
            f"INSERT INTO {table_name_qualified_quoted} ({','.join([self.quote_column(column) for column in columns])}) \n"
            f"{rows_sql};"
        )

    def drop_test_schema_if_exists(self) -> None:
        # First find all the tables in the schema
        table_names: list[str] = self.query_existing_test_table_names(return_fully_qualified_table_names=True)
        for fully_qualified_table_name in table_names:
            table_identifier = f"{fully_qualified_table_name.database_name}.{fully_qualified_table_name.schema_name}.{fully_qualified_table_name.table_name}"
            self.data_source_impl.execute_update(f"DROP TABLE {table_identifier};")
        # Drop the schema
        schema_name = self.dataset_prefix[self.data_source_impl.sql_dialect.get_schema_prefix_index()]
        self.data_source_impl.execute_update(f"DROP SCHEMA {schema_name};")
