from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Literal, Optional, Union
import boto3


import psycopg2
from pydantic import Field, IPvAnyAddress, SecretStr, field_validator
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.aws_credentials import AwsCredentials

from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)



class RedshiftConnectionProperties(DataSourceConnectionProperties):
    user: str = Field(..., description="Database username")
    host: Union[str, IPvAnyAddress] = Field(..., description="Database host (hostname or IP address)")
    port: Optional[int] = Field(5439, description="Database port (1-65535)", ge=1, le=65535)
    database: str = Field(..., description="Database name", min_length=1, max_length=63)
    connect_timeout: Optional[int] = Field(None, description="Connection timeout")
    keepalives_idle: Optional[int] = Field(None, description="Keepalives idle")
    keepalives_interval: Optional[int] = Field(None, description="Keepalives interval")
    keepalives_count: Optional[int] = Field(None, description="Keepalives count")


class RedshiftUserPassConnection(RedshiftConnectionProperties):
    password: SecretStr = Field(..., description="Database password")


class RedshiftKeyConnection(RedshiftConnectionProperties):
    access_key_id: str = Field(..., description="AWS access key ID")
    secret_access_key: SecretStr = Field(..., description="AWS secret access key")
    session_token: Optional[str] = Field(None, description="AWS session token")
    role_arn: Optional[str] = Field(None, description="AWS role ARN")
    region: Optional[str] = Field("eu-west-1", description="AWS region")
    profile_name: Optional[str] = Field(None, description="AWS profile name")


class RedshiftDataSource(DataSourceBase, ABC):
    type: Literal["redshift"] = Field("redshift")

    connection_properties: Union[
        RedshiftUserPassConnection,
        RedshiftKeyConnection,
    ] = Field(..., alias="connection", description="Redshift connection configuration")



class RedshiftDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def __get_cluster_credentials(self, aws_credentials: AwsCredentials):
        resolved_aws_credentials = aws_credentials.resolve_role(
            role_session_name="soda_redshift_get_cluster_credentials"
        )

        client = boto3.client(
            "redshift",
            region_name=resolved_aws_credentials.region_name,
            aws_access_key_id=resolved_aws_credentials.access_key_id,
            aws_secret_access_key=resolved_aws_credentials.secret_access_key,
            aws_session_token=resolved_aws_credentials.session_token,
        )

        cluster_name = self.host.split(".")[0]
        username = self.username
        db_name = self.database
        cluster_creds = client.get_cluster_credentials(
            DbUser=username, DbName=db_name, ClusterIdentifier=cluster_name, AutoCreate=False, DurationSeconds=3600
        )

        return cluster_creds["DbUser"], cluster_creds["DbPassword"]

    def _create_connection(
        self,
        config: RedshiftConnectionProperties,
    ):

        if isinstance(config, RedshiftUserPassConnection):
            username = config.user
            password = config.password.get_secret_value()
        elif isinstance(config, RedshiftKeyConnection):
            aws_credentials = AwsCredentials(
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key.get_secret_value(),
                role_arn=config.role_arn,
                session_token=config.session_token,
                region_name=config.region,
                profile_name=config.profile_name,
            )
            username, password = self.__get_cluster_credentials(aws_credentials)
        

        keepalives_params = {}
        if config.keepalives_idle:
            keepalives_params["keepalives_idle"] = config.keepalives_idle
        if config.keepalives_interval:
            keepalives_params["keepalives_interval"] = config.keepalives_interval
        if config.keepalives_count:
            keepalives_params["keepalives_count"] = config.keepalives_count

        return psycopg2.connect(
            user=username,
            password=password,
            host=config.host,
            port=config.port,
            connect_timeout=config.connect_timeout,
            database=config.database,            
            **keepalives_params,
        )
