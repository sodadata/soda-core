from __future__ import annotations

import json
import os

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestDataType


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
        # location: '{os.getenv("ORACLE_LOCATION", "US")}'

    # def _get_create_table_sql_type_dict(self) -> dict[str, str]:
    #     return {
    #         TestDataType.TEXT: "VARCHAR2(4000)",
    #         TestDataType.INTEGER: "NUMBER(10)",
    #         TestDataType.DECIMAL: "NUMBER(10,2)",
    #         TestDataType.DATE: "DATE",
    #         TestDataType.TIME: "TIMESTAMP",  # Oracle doesn't have a separate TIME type
    #         TestDataType.TIMESTAMP: "TIMESTAMP",
    #         TestDataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
    #         TestDataType.BOOLEAN: "NUMBER(1)",  # Oracle doesn't have native BOOLEAN
    #     }

    # def _get_contract_data_type_dict(self) -> dict[str, str]:
    #     """
    #     DataSourceTestHelpers can override this method as an easy way
    #     to customize the get_schema_check_sql_type behavior
    #     Oracle reports NUMBER without precision details in schema checks
    #     """
    #     return {
    #         TestDataType.TEXT: "VARCHAR2(4000)",  # Oracle shows precision for VARCHAR2
    #         TestDataType.INTEGER: "NUMBER",  # Oracle reports NUMBER without precision in schema checks
    #         TestDataType.DECIMAL: "NUMBER",  # Oracle reports NUMBER without precision in schema checks
    #         TestDataType.DATE: "DATE",
    #         TestDataType.TIME: "TIMESTAMP",
    #         TestDataType.TIMESTAMP: "TIMESTAMP",
    #         TestDataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
    #         TestDataType.BOOLEAN: "NUMBER",  # Oracle doesn't have native BOOLEAN
    #     }

    # def _create_test_table_sql_statement(self, table_name_qualified_quoted: str, columns_sql: str) -> str:
    #     """Oracle-specific CREATE TABLE - Oracle doesn't like semicolons in some contexts"""
    #     return f"CREATE TABLE {table_name_qualified_quoted} ( \n{columns_sql} \n)"
    
    # def _drop_test_table_sql_statement(self, table_name_qualified_quoted: str) -> str:
    #     """Oracle-specific DROP TABLE - Oracle doesn't like semicolons in some contexts"""
    #     return f"DROP TABLE {table_name_qualified_quoted}"
