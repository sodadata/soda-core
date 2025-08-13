from __future__ import annotations

import logging
from abc import ABC
from typing import Literal, Optional, Union

from google.cloud.bigquery.table import Row
from pydantic import Field, SecretStr
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


DEFAULT_CATALOG = "awsdatacatalog"


# Currently the only supported authentication method.
class AthenaConnectionProperties(DataSourceConnectionProperties, ABC):
    access_key_id: str = Field(..., description="AWS access key ID")
    secret_access_key: SecretStr = Field(..., description="AWS secret access key")
    region_name: str = Field(..., description="AWS region name")
    staging_dir: str = Field(..., description="S3 staging directory")

    role_arn: Optional[str] = Field(None, description="AWS role ARN")
    catalog: Optional[str] = Field(
        DEFAULT_CATALOG, description="Athena catalog name"
    )  # Catalog name is required to be set
    work_group: Optional[str] = Field(None, description="Athena work group name")
    session_token: Optional[str] = Field(None, description="AWS session token")
    profile_name: Optional[str] = Field(None, description="AWS profile name")


class AthenaDataSource(DataSourceBase, ABC):
    type: Literal["athena"] = Field("athena")

    connection_properties: Union[AthenaConnectionProperties,] = Field(
        ..., alias="connection", description="Athena connection configuration"
    )


class AthenaDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: AthenaConnectionProperties,
    ):
        pass

    # TODO: look at Redshift PR as we can inspire the connection from there
    # try:
    #     self.connection = pyathena.connect(
    #         profile_name=self.aws_credentials.profile_name,
    #         aws_access_key_id=self.aws_credentials.access_key_id,
    #         aws_secret_access_key=self.aws_credentials.secret_access_key,
    #         s3_staging_dir=self.athena_staging_dir,
    #         region_name=self.aws_credentials.region_name,
    #         role_arn=self.aws_credentials.role_arn,
    #         catalog_name=self.catalog,
    #         work_group=self.work_group,
    #         schema_name=self.schema,
    #         aws_session_token=self.aws_credentials.session_token,
    #     )

    #     return self.connection
    # except Exception as e:
    #     raise ConnectionRefusedError(self.type, e)

    def format_rows(self, rows: list[Row]) -> list[tuple]:
        formatted_rows = [tuple(r.values()) for r in rows]
        return formatted_rows
