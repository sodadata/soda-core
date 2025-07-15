from __future__ import annotations

import json
import logging

from google.api_core.client_info import ClientInfo
from google.auth import default, impersonated_credentials
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.cloud.bigquery.table import Row
from google.oauth2.service_account import Credentials
from soda_bigquery.model.data_source.bigquery_connection_properties import (
    BigQueryConnectionProperties,
    BigQueryContextAuth,
    BigQueryJSONFileAuth,
    BigQueryJSONStringAuth,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


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
        return

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
                target_scopes=self.config.auth_scopes,
                delegates=delegates,
            )

    def _apply_optional_params(self, config: BigQueryConnectionProperties):
        # Users can optionally overwrite in the connection properties
        self.project_id = config.project_id if config.project_id else self.project_id
        self.location = config.location
        self.client_options = config.client_options

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
            user_agent="soda-library",
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
