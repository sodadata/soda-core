from __future__ import annotations

import os

from helpers.data_source_test_helper import DataSourceTestHelper


class OracleDataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        # Oracle doesn't have the concept of separate databases like other systems
        # The service name or SID effectively serves as the database identifier
        connect_string = os.getenv("ORACLE_CONNECTSTRING")
        if connect_string and "/" in connect_string:
            # Extract service name from connect string like "host:port/service_name"
            service_name = connect_string.split("/")[-1]
            return service_name
        return "oracle_test_db"

    def _create_dataset_prefix(self) -> list[str]:
        schema_name: str = self._create_schema_name()
        return [schema_name] if schema_name else []

    def _create_test_table_sql_statement(self, table_name_qualified_quoted: str, columns_sql: str) -> str:
        """No semicolon at the end of the statement for Oracle"""
        return super()._create_test_table_sql_statement(table_name_qualified_quoted, columns_sql).replace(";", "")

    def _drop_test_table_sql_statement(self, table_name_qualified_quoted: str) -> str:
        """No semicolon at the end of the statement for Oracle"""
        return f"DROP TABLE {table_name_qualified_quoted}"

    # def _create_test_table_sql(self, test_table: TestTable) -> str:
    #     sql_dialect: SqlDialect = self.data_source_impl.sql_dialect
    #     columns_sql: str = ",\n".join(
    #         [
    #             f"  {sql_dialect.quote_default(column.name)} {column.create_table_data_type}"
    #             for column in test_table.columns.values()
    #         ]
    #     )
    #     return self._create_test_table_sql_statement(test_table.qualified_name, columns_sql)

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{os.getenv("ORACLE_USERNAME")}'
                password: '{os.getenv("ORACLE_PASSWORD")}'
                connect_string: '{os.getenv("ORACLE_CONNECTSTRING")}'

        """
