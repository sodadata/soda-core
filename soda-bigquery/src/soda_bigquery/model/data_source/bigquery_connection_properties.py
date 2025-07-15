from abc import ABC
from typing import Literal, Optional

from pydantic import Field, SecretStr
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class BigQueryConnectionProperties(DataSourceConnectionProperties, ABC):
    project_id: Optional[str] = Field(None, description="BigQuery project ID")
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

    use_context_auth: Optional[Literal[False]] = Field(False, description="Use context authentication")
    account_info_json: SecretStr = Field(..., description="Service account JSON as string", min_length=1)


class BigQueryJSONFileAuth(BigQueryConnectionProperties):
    """BigQuery authentication using JSON file path"""

    use_context_auth: Optional[Literal[False]] = Field(False, description="Use context authentication")
    account_info_json_path: str = Field(..., description="Path to service account JSON file", min_length=1)


class BigQueryContextAuth(BigQueryConnectionProperties):
    """BigQuery authentication using context.

    If use_context_auth is True, then application default credentials will be used.
    The user may optionally provide JSON credentials; they will be ignored.
    """

    use_context_auth: Literal[True] = Field(description="Use context authentication")
