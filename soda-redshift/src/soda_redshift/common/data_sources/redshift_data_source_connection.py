from __future__ import annotations

from abc import ABC
from ipaddress import IPv4Address, IPv6Address
from typing import Literal, Optional, Union

import boto3
import psycopg
from pydantic import Field, IPvAnyAddress, SecretStr
from soda_core.common.aws_credentials import AwsCredentials
from soda_core.common.data_source_connection import DataSourceConnection
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
    cluster_identifier: Optional[str] = Field(None, description="Redshift cluster identifier")


class RedshiftDataSource(DataSourceBase, ABC):
    type: Literal["redshift"] = Field("redshift")

    connection_properties: Union[
        RedshiftUserPassConnection,
        RedshiftKeyConnection,
    ] = Field(..., alias="connection", description="Redshift connection configuration")


class RedshiftDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _extract_cluster_identifier(self):
        if isinstance(self.host, (IPv4Address, IPv6Address)):
            raise ValueError("Cluster identifier is required when using an IP address as host")
        # strip protocol if present
        host = self.host.split("://")[1] if "://" in self.host else self.host
        return host.split(".")[0]

    def _get_cluster_credentials(self, aws_credentials: AwsCredentials, cluster_identifier: Optional[str] = None):
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

        cluster_identifier = self._extract_cluster_identifier() if not cluster_identifier else cluster_identifier

        user = self.user
        db_name = self.database
        # Note  user is used here to get database credentials, therefore it's required even if AWS creds are provided
        cluster_creds = client.get_cluster_credentials(
            DbUser=user, DbName=db_name, ClusterIdentifier=cluster_identifier, AutoCreate=False, DurationSeconds=3600
        )

        return cluster_creds["DbUser"], cluster_creds["DbPassword"]

    def _load_params(self, config: RedshiftConnectionProperties):
        self.user = config.user
        self.host = config.host
        self.port = config.port
        self.database = config.database
        self.connect_timeout = config.connect_timeout

        self.keepalives_params = {}
        if config.keepalives_idle:
            self.keepalives_params["keepalives_idle"] = config.keepalives_idle
        if config.keepalives_interval:
            self.keepalives_params["keepalives_interval"] = config.keepalives_interval
        if config.keepalives_count:
            self.keepalives_params["keepalives_count"] = config.keepalives_count

    def _create_connection(
        self,
        config: RedshiftConnectionProperties,
    ):
        self._load_params(config)

        if isinstance(config, RedshiftUserPassConnection):
            self.password = config.password.get_secret_value()
        elif isinstance(config, RedshiftKeyConnection):
            aws_credentials = AwsCredentials(
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key.get_secret_value(),
                role_arn=config.role_arn,
                session_token=config.session_token,
                region_name=config.region,
                profile_name=config.profile_name,
            )
            self.user, self.password = self._get_cluster_credentials(aws_credentials, config.cluster_identifier)

        # Redshift is case-insensitive by default unless explicitly enabled.
        # It's possible customers may have enabled case-sensitivity in their databases, therefore we enable that in
        # to support databases configured that way.
        options = "-c enable_case_sensitive_identifier=on"
        # Redshift uses UTF-8 by default; Note, this is only required on psycopg3 as it defaults to UNICODE. But this is not supported by Redshift.
        options += " -c client_encoding=utf-8"

        conn = psycopg.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
            dbname=self.database,
            options=options,
            **self.keepalives_params,
        )
        conn.autocommit = True
        return conn
