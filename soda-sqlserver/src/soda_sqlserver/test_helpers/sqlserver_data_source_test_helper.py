from __future__ import annotations

import logging
import os
import re
from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.sql_ast import DROP_TABLE, DROP_VIEW
from soda_sqlserver.common.data_sources.sqlserver_data_source import (
    SqlServerDataSourceImpl,
    SqlServerSqlDialect,
)

logger = logging.getLogger(__name__)

# Placeholder recorded in snapshots instead of the per-xdist-worker database name,
# so a snapshot recorded on worker gw2 replays on gw0 (and vice versa).
SNAPSHOT_DATABASE_PLACEHOLDER = "__$$__soda_test_database__$$__"


def per_xdist_worker_database_name(base_database_name: str) -> Optional[str]:
    """Database name for this pytest-xdist worker, or ``None`` when not applicable.

    SQL Server's system catalog is *per database*. Under pytest-xdist all workers
    share one SQL Server container: per-worker schemas isolate the user tables, but
    every ``CREATE``/``DROP`` and every ``information_schema`` scan still contends on
    the same catalog rows, and SQL Server resolves the resulting lock cycles by
    killing one side as deadlock victim (SQLSTATE 40001 / error 1205). Giving each
    worker its own database removes the shared catalog, so workers can no longer
    deadlock each other.

    Only applies when ``PYTEST_XDIST_WORKER`` is set (xdist worker process). Opt out
    with ``SODA_TEST_SQLSERVER_DB_PER_WORKER=false`` (e.g. a server where the test
    login cannot ``CREATE DATABASE``).
    """
    worker_id = os.getenv("PYTEST_XDIST_WORKER")
    if not worker_id:
        return None
    if os.getenv("SODA_TEST_SQLSERVER_DB_PER_WORKER", "true").lower() in ("0", "false", "no", "off"):
        return None
    return re.sub("[^0-9a-zA-Z_]+", "_", f"{base_database_name}_{worker_id}")


class SqlServerDataSourceTestHelper(DataSourceTestHelper):
    # Set by _create_database_name() when this helper runs in its own per-worker
    # database; None for the base database (no xdist, opted out, or a subclass such
    # as Fabric/Synapse that overrides _create_database_name with a fixed warehouse).
    _per_worker_database: Optional[str] = None

    def _create_database_name(self) -> Optional[str]:
        base_database_name = os.getenv("SQLSERVER_DATABASE", "master")
        self._per_worker_database = per_xdist_worker_database_name(base_database_name)
        return self._per_worker_database or base_database_name

    def _string_length_sql_function(self) -> str:
        # SqlServer's LEN() trims trailing spaces, under-reporting any
        # payload that ends with spaces. DATALENGTH() returns the raw
        # byte count, which equals char count for varchar columns and
        # is the right answer for the unbounded-column data-length
        # verification.
        return "datalength"

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
                database: '{self.dataset_prefix[0]}'
                user: '{os.getenv("SQLSERVER_USERNAME", "SA")}'
                password: '{os.getenv("SQLSERVER_PASSWORD", "Password1!")}'
                trust_server_certificate: true
                driver: '{os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")}'
        """

    def _snapshot_extra_replacements(self) -> dict[str, str]:
        replacements = dict(super()._snapshot_extra_replacements())
        if self._per_worker_database:
            replacements[SNAPSHOT_DATABASE_PLACEHOLDER] = self._per_worker_database
        return replacements

    def start_test_session_open_connection(self) -> None:
        if self._per_worker_database:
            self._ensure_database_exists(self._per_worker_database)
        super().start_test_session_open_connection()

    def _ensure_database_exists(self, database_name: str) -> None:
        """Create the per-worker database if it does not exist yet.

        ``CREATE DATABASE`` is not allowed inside a transaction, so this uses a
        dedicated autocommit pyodbc connection to the base database (the one the
        test login is configured for) rather than the helper's own connection. The
        database is deliberately not dropped at session end: in CI the container is
        discarded, and locally reusing it is cheaper than recreating it.
        """
        import pyodbc
        from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
            SqlServerDataSourceConnection,
        )

        base_properties = self.data_source_impl.data_source_model.connection_properties.model_copy(
            update={"database": os.getenv("SQLSERVER_DATABASE", "master")}
        )
        connection = pyodbc.connect(
            SqlServerDataSourceConnection.build_connection_string(base_properties),
            timeout=int(base_properties.login_timeout),
            autocommit=True,
        )
        try:
            connection.cursor().execute(f"IF DB_ID(N'{database_name}') IS NULL CREATE DATABASE [{database_name}]")
            logger.info(f"Using per-xdist-worker SQL Server database [{database_name}]")
        finally:
            connection.close()

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

        view_names: list[str] = self.query_existing_test_views()
        for fully_qualified_view_name in view_names:
            view_identifier = f"{dialect.quote_default(fully_qualified_view_name.database_name)}.{dialect.quote_default(fully_qualified_view_name.schema_name)}.{dialect.quote_default(fully_qualified_view_name.view_name)}"
            drop_view_sql = dialect.build_drop_view_sql(DROP_VIEW(view_identifier))
            self.data_source_impl.execute_update(drop_view_sql)

        # Drop the schema if it exists.
        schema_name = self.extract_schema_from_prefix()
        if self._does_schema_exist(schema_name):
            self.data_source_impl.execute_update(f"DROP SCHEMA {dialect.quote_default(schema_name)};")

    def _does_schema_exist(self, schema_name: str) -> bool:
        """Check if the schema exists in the database."""
        query_result = self.data_source_impl.execute_query(
            f"SELECT name FROM sys.schemas WHERE name = '{schema_name}';"
        )
        return len(query_result.rows) > 0
