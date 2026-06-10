from __future__ import annotations

import logging
from abc import ABC
from datetime import timezone, tzinfo
from typing import Literal, Optional, Union

import pyathena
from pydantic import Field, SecretStr, model_validator
from soda_core.common.aws_credentials import AwsCredentials
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


DEFAULT_CATALOG = "awsdatacatalog"


class AthenaConnectionProperties(DataSourceConnectionProperties, ABC):
    # Static AWS credentials are optional. When omitted, the AWS SDK resolves
    # credentials from its default provider chain (e.g. IAM Roles for Service
    # Accounts / web identity federation, instance profiles, profile_name).
    access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    secret_access_key: Optional[SecretStr] = Field(None, description="AWS secret access key")
    region_name: str = Field(..., description="AWS region name")
    staging_dir: str = Field(..., description="S3 staging directory")

    role_arn: Optional[str] = Field(None, description="AWS role ARN")
    catalog: Optional[str] = Field(
        DEFAULT_CATALOG, description="Athena catalog name"
    )  # Catalog name is required to be set
    work_group: Optional[str] = Field(None, description="Athena work group name")
    session_token: Optional[str] = Field(None, description="AWS session token")
    profile_name: Optional[str] = Field(None, description="AWS profile name")

    @model_validator(mode="after")
    def _validate_credentials(self) -> "AthenaConnectionProperties":
        # access_key_id and secret_access_key must be provided together. Providing
        # only one is almost always a misconfiguration rather than an intentional
        # fall-through to the AWS default credential provider chain.
        if bool(self.access_key_id) != bool(self.secret_access_key):
            raise ValueError(
                "access_key_id and secret_access_key must be provided together. "
                "Omit both to use the AWS default credential provider chain "
                "(e.g. IAM Roles for Service Accounts / web identity federation)."
            )
        return self


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
        self.athena_staging_dir = config.staging_dir
        self.catalog = config.catalog
        self.work_group = config.work_group
        self.aws_credentials = AwsCredentials(
            access_key_id=config.access_key_id,
            secret_access_key=(config.secret_access_key.get_secret_value() if config.secret_access_key else None),
            role_arn=config.role_arn,
            session_token=config.session_token,
            region_name=config.region_name,
            profile_name=config.profile_name,
        )

        self.connection = pyathena.connect(
            profile_name=self.aws_credentials.profile_name,
            aws_access_key_id=self.aws_credentials.access_key_id,
            aws_secret_access_key=self.aws_credentials.secret_access_key,
            s3_staging_dir=self.athena_staging_dir,
            region_name=self.aws_credentials.region_name,
            role_arn=self.aws_credentials.role_arn,
            catalog_name=self.catalog,
            work_group=self.work_group,
            aws_session_token=self.aws_credentials.session_token,
        )

        return self.connection

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]  # Column names are in the first element of the tuple

    def _fetch_session_timezone(self) -> tzinfo:
        # Athena always operates in UTC; there is no per-session TZ knob.
        return timezone.utc
