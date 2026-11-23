import abc
from typing import Literal

from pydantic import ConfigDict, Field
from soda_core.model.data_source.data_source import DataSourceBase
from soda_databricks.model.data_source.databricks_connection_properties import (  # noqa: F401 — re-exported
    AUTH_TYPE_AZURE_SP,
    AUTH_TYPE_AZURE_WORKLOAD_IDENTITY,
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_TOKEN,
    DatabricksConnection,
)


class DatabricksDataSource(DataSourceBase, abc.ABC):
    # Keeps resolved secrets out of validation errors: a leftover azure_client_secret or access_token on the
    # azure-workload-identity mode fails with extra_forbidden, whose message would otherwise carry the value.
    model_config = ConfigDict(hide_input_in_errors=True)

    type: Literal["databricks"] = Field("databricks")
    # Auth mode is picked by the `auth_type` discriminator on DatabricksConnection (PAT when
    # omitted, for backward compatibility), so validation errors name the selected mode only.
    connection_properties: DatabricksConnection = Field(
        ..., alias="connection", description="Databricks connection configuration"
    )
