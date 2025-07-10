from __future__ import annotations

import json
import logging

from google.api_core.client_info import ClientInfo
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials
from soda_bigquery.model.data_source.bigquery_connection_properties import (
    BigQueryConnectionProperties,
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

    def _create_connection(
        self,
        config: BigQueryConnectionProperties,
    ):
        account_info_dict = json.loads(config.account_info_json.get_secret_value())
        credentials = Credentials.from_service_account_info(
            account_info_dict,
            scopes=config.auth_scopes,
        )
        project_id = account_info_dict.get("project_id")

        client_info = ClientInfo(
            user_agent="soda-library",
        )
        self.client = bigquery.Client(
            project=project_id,
            credentials=credentials,
            client_info=client_info,
            location=config.location,
            # client_options=self.client_options,
        )

        return dbapi.Connection(self.client)
