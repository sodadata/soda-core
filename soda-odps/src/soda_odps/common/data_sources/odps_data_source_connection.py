from __future__ import annotations

import logging
from abc import ABC
from typing import Any, Optional

from pydantic import SecretStr
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger = soda_logger


def _get_odps_class():
    """Lazy import of ODPS class to avoid early initialization issues."""
    from odps import ODPS
    return ODPS


class OdpsDataSourceConnection(DataSourceConnection):
    """Connection handler for ODPS (MaxCompute)."""

    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)
        self._odps_client = None
        self._cursor = None

    def _get_config_value(self, config, attr_name: str, default: Any = None) -> Any:
        """Extract and resolve a config attribute value."""
        value = getattr(config, attr_name, default)

        # Handle SecretStr type
        if hasattr(value, 'get_secret_value'):
            return value.get_secret_value()

        # Resolve env var placeholders like ${env.VAR} or ${env.VAR:-default}
        if value and isinstance(value, str) and value.startswith('${'):
            return self._resolve_env_var(value)

        return str(value) if value is not None else None

    def _resolve_env_var(self, value: str) -> str:
        """Resolve environment variable placeholder in value."""
        import re,os

        match = re.search(r'\$\{env\.(\w+)(?::-)?([^}]*?)\}', value)
        if not match:
            return value

        env_var = match.group(1)
        default_val = match.group(2) or None
        return os.getenv(env_var) or default_val or ''

    def _resolve_config_values(self, config) -> dict:
        """Resolve all config values including environment variables."""
        access_id = self._get_config_value(config, 'access_id')
        secret_access_key = self._get_config_value(config, 'secret_access_key')
        project = self._get_config_value(config, 'project')
        endpoint = self._get_config_value(config, 'endpoint')
        tunnel_endpoint = self._get_config_value(config, 'tunnel_endpoint', None)

        return {
            'access_id': access_id,
            'secret_access_key': secret_access_key,
            'project': project,
            'endpoint': endpoint,
            'tunnel_endpoint': tunnel_endpoint,
        }

    def _create_connection(
        self,
        config,
    ):
        ODPS = _get_odps_class()

        values = self._resolve_config_values(config)

        self._odps_client = ODPS(
            access_id=values['access_id'],
            secret_access_key=values['secret_access_key'],
            project=values['project'],
            endpoint=values['endpoint'],
            tunnel_endpoint=values['tunnel_endpoint'],
        )
        # Create a cursor-like wrapper for compatibility
        self._cursor = OdpsCursor(self._odps_client)
        return self._cursor

    def _execute_query_get_result_row_column_name(self, column) -> str:
        """Extract column name from query result."""
        return column[0]  # Column names are in the first element of the tuple

    def close(self):
        """Close the ODPS connection."""
        if self._cursor:
            self._cursor.close()
        self._cursor = None
        self._odps_client = None


class OdpsCursor:
    """Cursor wrapper for ODPS to provide DB-API compatibility."""

    def __init__(self, odps_client):
        self._odps_client = odps_client
        self._last_result = None
        self._description = None
        self._is_open = True

    def cursor(self):
        """Return self as the cursor (DB-API compatibility)."""
        return self

    def execute(self, sql: str):
        """Execute SQL via ODPS."""
        logger.info(f"Executing ODPS SQL: {sql}")

        # Check if this is a DESCRIBE query
        sql_upper = sql.strip().upper()
        is_describe = sql_upper.startswith("DESCRIBE") or sql_upper.startswith("DESC")

        if is_describe:
            # For DESCRIBE queries, use ODPS Table API instead of SQL
            self._execute_describe_via_table_api(sql)
            return

        # Use ODPS run_sql API for regular queries
        try:
            # Enable full table scan on partitioned tables
            hints = {"odps.sql.allow.fullscan": "true"}

            # Create an instance for SQL execution
            instance = self._odps_client.run_sql(sql, hints=hints)
            # Wait for completion
            instance.wait_for_success()
            # Get reader to fetch results
            try:
                with instance.open_reader() as reader:
                    # For regular queries, get rows from reader
                    self._last_result = list(reader)
                    # Get schema from first record if available
                    if self._last_result and len(self._last_result) > 0:
                        # PEP-249 format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
                        self._description = [(str(i), "string", None, None, None, None, None) for i in range(len(self._last_result[0]))]
                    else:
                        self._description = []
            except Exception as e:
                logger.warning(f"ODPS reader error: {e}")
                self._last_result = []
                self._description = []
        except Exception as e:
            logger.warning(f"ODPS SQL execution error: {e}")
            self._last_result = []
            self._description = []

    def _execute_describe_via_table_api(self, sql: str):
        """Execute DESCRIBE query using ODPS Table API instead of SQL."""
        try:
            # Parse table name from DESCRIBE sql
            # Format: DESCRIBE table_name or DESC table_name
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("DESCRIBE"):
                table_expr = sql[8:].strip().rstrip(';').strip()
            else:
                table_expr = sql[4:].strip().rstrip(';').strip()

            # Parse the table name - could be project.schema.table, schema.table, or just table_name
            # In ODPS, the table name is in format: project.schema.table
            parts = table_expr.split('.')
            if len(parts) == 3:
                # project.schema.table - use as is for ODPS Table API
                full_table_name = table_expr
            elif len(parts) == 2:
                # schema.table - prepend current ODPS project
                project = self._odps_client.project
                full_table_name = f"{project}.{table_expr}"
            else:
                # Just table_name - cannot describe without schema info
                raise ValueError(f"Cannot determine schema for table: {table_expr}")

            # Use ODPS Table API to get table schema
            table = self._odps_client.get_table(full_table_name)
            if table is None:
                raise ValueError(f"Table not found: {full_table_name}")

            # Build result from table schema
            # ODPS DESCRIBE returns: column_name, column_type, comment
            self._last_result = []
            self._description = []

            # Add data columns (excluding partition columns)
            for col in table.schema:
                col_name = col.name
                col_type = str(col.type).lower()
                self._last_result.append((col_name, col_type))
                # PEP-249 format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
                self._description.append((col_name, col_type, None, None, None, None, None))

            # Add partition columns from schema.partitions
            for part_col in table.schema.partitions:
                col_name = part_col.name
                col_type = str(part_col.type).lower()
                self._last_result.append((col_name, col_type))
                self._description.append((col_name, col_type, None, None, None, None, None))

            logger.info(f"DESCRIBE returned {len(self._description)} columns")
        except Exception as e:
            logger.warning(f"ODPS DESCRIBE error: {e}")
            self._last_result = []
            self._description = []

    def fetchone(self):
        """Fetch one row."""
        if self._last_result and len(self._last_result) > 0:
            return self._last_result.pop(0)
        return None

    def fetchall(self):
        """Fetch all rows."""
        result = self._last_result if self._last_result else []
        self._last_result = []
        return result

    @property
    def description(self):
        """Return cursor description (column info)."""
        return self._description if self._description else []

    def close(self):
        """Close cursor."""
        self._last_result = None
        self._description = None