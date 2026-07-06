from __future__ import annotations

import logging
from datetime import timezone, tzinfo

from databricks import sql
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    parse_session_timezone,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksAzureServicePrincipal,
    DatabricksConnectionProperties,
    DatabricksOAuthM2M,
)

logger: logging.Logger = soda_logger


class DatabricksDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: DatabricksConnectionProperties,
    ):
        connection_kwargs = config.to_connection_kwargs()

        # OAuth modes: the SDK builds a credentials_provider callable (handling token
        # acquisition + refresh). The credential fields are stripped from connection_kwargs
        # by the properties class, so they never reach sql.connect as plain kwargs.
        credentials_provider = self._build_credentials_provider(config, connection_kwargs)
        if credentials_provider is not None:
            return sql.connect(
                user_agent_entry="Soda Core",
                credentials_provider=credentials_provider,
                **connection_kwargs,
            )

        # Token (PAT) auth: access_token flows through connection_kwargs unchanged.
        return sql.connect(
            user_agent_entry="Soda Core",
            **connection_kwargs,
        )

    @staticmethod
    def _build_credentials_provider(config: DatabricksConnectionProperties, connection_kwargs: dict):
        """Return an SDK credentials_provider callable for OAuth modes, else None.

        The Databricks SDK derives the token endpoint from the workspace host and handles
        refresh. ``connection_kwargs['server_hostname']`` has had any scheme stripped by the
        properties layer, so re-add ``https://`` for the SDK ``Config``.
        """
        if not isinstance(config, (DatabricksOAuthM2M, DatabricksAzureServicePrincipal)):
            return None

        from databricks.sdk.core import Config
        from databricks.sdk.credentials_provider import (
            azure_service_principal,
            oauth_service_principal,
        )

        host = f"https://{connection_kwargs['server_hostname']}"

        if isinstance(config, DatabricksOAuthM2M):
            sdk_config = Config(
                host=host,
                client_id=config.client_id,
                client_secret=config.client_secret.get_secret_value(),
            )
            return oauth_service_principal(sdk_config)

        # DatabricksAzureServicePrincipal — Entra ID (Azure AD) service principal.
        sdk_config = Config(
            host=host,
            azure_client_id=config.azure_client_id,
            azure_client_secret=config.azure_client_secret.get_secret_value(),
            azure_tenant_id=config.azure_tenant_id,
        )
        return azure_service_principal(sdk_config)

    def _fetch_session_timezone(self) -> tzinfo:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT current_timezone()")
            row = cursor.fetchone()
        if not row:
            return timezone.utc
        return parse_session_timezone(row[0])

    def rollback(self) -> None:
        # We do not start any transactions, Databricks default is autocommit.
        pass

    def commit(self) -> None:
        # We do not start any transactions, Databricks default is autocommit.
        pass

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]
