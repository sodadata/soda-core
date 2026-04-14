from __future__ import annotations

import logging
import socket
import ssl

from databricks import sql
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksConnectionProperties,
)

logger: logging.Logger = soda_logger


def _probe_databricks_reachability(host: str, port: int = 443, timeout: float = 10.0) -> None:
    """Pre-flight TCP+TLS probe to the Databricks workspace.

    Raises immediately if the server is not reachable at the network layer
    (DNS failure, TCP refused, TLS handshake error). This short-circuits
    the databricks-sql-connector's retry loop for connect-phase failures,
    which otherwise exponentially back off for up to ~15 min on a
    misconfigured hostname. See SCS-884 / OBSL-445.

    Reachable-but-unhealthy cases (503 cluster warming, 404, 401/403) are
    intentionally passed through: the connector handles them correctly
    (patient wait on 503 + Retry-After, fast-fail on auth errors).
    """
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(sock, server_hostname=host):
                return
    except (socket.gaierror, socket.timeout, ConnectionError, ssl.SSLError, OSError) as e:
        raise ConnectionError(f"Databricks host {host}:{port} is not reachable: {e}") from e


class DatabricksDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: DatabricksConnectionProperties,
    ):
        connection_kwargs = config.to_connection_kwargs()
        _probe_databricks_reachability(connection_kwargs["server_hostname"])
        return sql.connect(
            user_agent_entry="Soda Core",
            **connection_kwargs,
        )

    def rollback(self) -> None:
        # We do not start any transactions, Databricks default is autocommit.
        pass

    def commit(self) -> None:
        # We do not start any transactions, Databricks default is autocommit.
        pass

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]
