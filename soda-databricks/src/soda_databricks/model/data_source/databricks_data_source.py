import abc
from typing import Literal

from pydantic import Field
from soda_core.model.data_source.data_source import DataSourceBase
from soda_databricks.model.data_source.databricks_connection_properties import (  # noqa: F401 — re-exported
    AUTH_TYPE_AZURE_SP,
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_TOKEN,
    DatabricksConnection,
)


class DatabricksDataSource(DataSourceBase, abc.ABC):
    type: Literal["databricks"] = Field("databricks")
    # Auth mode is picked by the `auth_type` discriminator on DatabricksConnection (PAT when
    # omitted, for backward compatibility), so validation errors name the selected mode only.
    connection_properties: DatabricksConnection = Field(
        ..., alias="connection", description="Databricks connection configuration"
    )
