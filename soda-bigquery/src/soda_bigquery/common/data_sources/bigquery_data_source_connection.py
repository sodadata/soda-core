from __future__ import annotations

import json
import logging
from abc import ABC
from typing import Literal, Optional, Union

from google.api_core.client_info import ClientInfo
from google.auth import default, impersonated_credentials
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.cloud.bigquery.table import Row
from google.oauth2.service_account import Credentials
from pydantic import Field, SecretStr
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


CONTEXT_AUTHENTICATION_DESCRIPTION = "Use context authentication"


class BigQueryConnectionProperties(DataSourceConnectionProperties, ABC):
    project_id: Optional[str] = Field(None, description="BigQuery project ID")
    storage_project_id: Optional[str] = Field(None, description="BigQuery storage project ID")
    location: Optional[str] = Field(None, description="BigQuery location")
    client_options: Optional[dict] = Field(None, description="Client options")
    labels: Optional[dict] = Field({}, description="Labels")
    auth_scopes: Optional[list[str]] = Field(
        [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
        ],
        description="Authentication scopes",
    )
    impersonation_account: Optional[str] = Field(None, description="Impersonation account")
    delegates: Optional[list[str]] = Field(None, description="Delegates")


class BigQueryJSONStringAuth(BigQueryConnectionProperties):
    """BigQuery authentication using JSON string"""

    use_context_auth: Optional[Literal[False]] = Field(False, description=CONTEXT_AUTHENTICATION_DESCRIPTION)
    account_info_json: SecretStr = Field(..., description="Service account JSON as string", min_length=1)


class BigQueryJSONFileAuth(BigQueryConnectionProperties):
    """BigQuery authentication using JSON file path"""

    use_context_auth: Optional[Literal[False]] = Field(False, description=CONTEXT_AUTHENTICATION_DESCRIPTION)
    account_info_json_path: str = Field(..., description="Path to service account JSON file", min_length=1)


class BigQueryContextAuth(BigQueryConnectionProperties):
    """BigQuery authentication using context.

    If use_context_auth is True, then application default credentials will be used.
    The user may optionally provide JSON credentials; they will be ignored.
    """

    use_context_auth: Literal[True] = Field(description=CONTEXT_AUTHENTICATION_DESCRIPTION)


class BigQueryDataSource(DataSourceBase, ABC):
    type: Literal["bigquery"] = Field("bigquery")

    connection_properties: Union[
        BigQueryJSONStringAuth,
        BigQueryJSONFileAuth,
        BigQueryContextAuth,
    ] = Field(..., alias="connection", description="BigQuery connection configuration")


class BigQueryDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _load_project_id_and_credentials(self, config: BigQueryConnectionProperties):
        if isinstance(config, BigQueryContextAuth):
            logger.info("Using application default credentials.")
            self.credentials, self.project_id = default()
            return

        if isinstance(config, BigQueryJSONFileAuth):
            account_info_dict = json.load(open(config.account_info_json_path))
        elif isinstance(config, BigQueryJSONStringAuth):
            account_info_dict = json.loads(config.account_info_json.get_secret_value())
        self.credentials = Credentials.from_service_account_info(
            account_info_dict,
            scopes=config.auth_scopes,
        )
        self.project_id = account_info_dict.get("project_id")

    def _load_optional_impersonated_credentials(self, config: BigQueryConnectionProperties):
        if config.impersonation_account:
            logger.info("Using impersonation of Service Account.")
            if config.delegates:
                logger.info("Using Service Account delegates.")
                delegates = config.delegates
            else:
                delegates = None
            self.credentials = impersonated_credentials.Credentials(
                source_credentials=self.credentials,
                target_principal=str(config.impersonation_account),
                target_scopes=config.auth_scopes,
                delegates=delegates,
            )

    def _apply_optional_params(self, config: BigQueryConnectionProperties):
        # Users can optionally overwrite in the connection properties
        self.project_id = config.project_id if config.project_id else self.project_id
        self.location = config.location
        self.client_options = config.client_options

        # Storage project ID is currently not used, because the project is configured via the DQN in the data contract.
        # When we implement discovery, we'll need to use this value.
        self.storage_project_id = config.storage_project_id if config.storage_project_id else self.project_id

        self.labels = config.labels

    def _create_connection(
        self,
        config: BigQueryConnectionProperties,
    ):
        self._load_project_id_and_credentials(config)
        self._load_optional_impersonated_credentials(config)
        self._apply_optional_params(config)

        client_info = ClientInfo(
            user_agent="soda-core",
        )
        default_query_job_config = bigquery.QueryJobConfig(labels=self.labels)
        self.client = bigquery.Client(
            project=self.project_id,
            credentials=self.credentials,
            default_query_job_config=default_query_job_config,
            client_info=client_info,
            location=config.location,
            client_options=self.client_options,
        )

        return dbapi.Connection(self.client)

    def format_rows(self, rows: list[Row]) -> list[tuple]:
        formatted_rows = [tuple(r.values()) for r in rows]
        return formatted_rows
