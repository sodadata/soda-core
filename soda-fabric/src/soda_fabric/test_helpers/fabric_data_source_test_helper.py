from __future__ import annotations

import logging
import os
from typing import Optional

from soda_core.common.sql_ast import DROP_TABLE, DROP_VIEW
from soda_core.common.statements.metadata_tables_query import (
    FullyQualifiedTableName,
    MetadataTablesQuery,
)
from soda_core.common.statements.table_types import FullyQualifiedViewName, TableType
from soda_sqlserver.test_helpers.sqlserver_data_source_test_helper import (
    SqlServerDataSourceTestHelper,
)

logger = logging.getLogger(__name__)


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

    def drop_schema_if_exists(self, schema: str) -> None:
        """Drop all objects in the schema first, then drop the schema.

        SQL Server/Fabric does not support CASCADE on DROP SCHEMA, so all
        tables and views must be removed before the schema can be dropped.
        """
        try:
            if not self._does_schema_exist(schema):
                return

            dialect = self.data_source_impl.sql_dialect
            database_name = self.extract_database_from_prefix()

            metadata_tables_query: MetadataTablesQuery = self.data_source_impl.create_metadata_tables_query()
            fully_qualified_objects = metadata_tables_query.execute(
                database_name=database_name,
                schema_name=schema,
                types_to_return=[TableType.TABLE, TableType.VIEW],
            )

            # Drop views first
            for obj in fully_qualified_objects:
                if isinstance(obj, FullyQualifiedViewName):
                    view_identifier = f"{dialect.quote_default(obj.database_name)}.{dialect.quote_default(obj.schema_name)}.{dialect.quote_default(obj.view_name)}"
                    self.data_source_impl.execute_update(dialect.build_drop_view_sql(DROP_VIEW(view_identifier)))

            # Then drop tables
            for obj in fully_qualified_objects:
                if isinstance(obj, FullyQualifiedTableName):
                    table_identifier = f"{dialect.quote_default(obj.database_name)}.{dialect.quote_default(obj.schema_name)}.{dialect.quote_default(obj.table_name)}"
                    self.data_source_impl.execute_update(dialect.build_drop_table_sql(DROP_TABLE(table_identifier)))

            # Finally drop the schema
            self.data_source_impl.execute_update(f"DROP SCHEMA {dialect.quote_default(schema)};")
        except Exception as e:
            logger.warning(f"Error dropping test schema: {e}")
