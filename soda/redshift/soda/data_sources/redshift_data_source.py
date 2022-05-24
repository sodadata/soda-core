import logging
import re
from typing import Dict, List, Optional

import boto3
import psycopg2
from soda.cloud.aws.credentials import AwsCredentials
from soda.common.exceptions import DataSourceConnectionError
from soda.execution.data_source import DataSource

logger = logging.getLogger(__name__)


class DataSourceImpl(DataSource):
    TYPE = "redshift"

    def connect(self, connection_properties):
        self.connection_properties = connection_properties

        try:
            username = connection_properties.get("username")
            password = connection_properties.get("password")
            if not username or not password:
                username, password = self.__get_cluster_credentials(self.__get_aws_credentials)

            conn = psycopg2.connect(
                user=username,
                password=password,
                host=connection_properties.get("host", "localhost"),
                port=connection_properties.get("port", "5439"),
                connect_timeout=connection_properties.get("connection_timeout_sec"),
                database=connection_properties.get("database"),
            )
            return conn
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def __get_aws_credentials(self):
        access_key_id = self.connection_properties.get("access_key_id")
        role_arn = self.connection_properties.get("role_arn")
        profile_name = self.connection_properties.get("profile_name")
        if access_key_id or role_arn or profile_name:
            return AwsCredentials(
                access_key_id=access_key_id,
                secret_access_key=self.connection_properties.get("secret_access_key"),
                role_arn=self.connection_properties.get("role_arn"),
                session_token=self.connection_properties.get("session_token"),
                region_name=self.connection_properties.get("region", "eu-west-1"),
            )

    def __get_cluster_credentials(self, aws_credentials: Dict):
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

    def sql_get_table_names_with_count(
        self, include_tables: Optional[List[str]] = None, exclude_tables: Optional[List[str]] = None
    ) -> str:
        table_filter_expression = self.sql_table_include_exclude_filter(
            '"table"', "tbl_rows", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return f'SELECT "table", tbl_rows \n FROM svv_table_info {where_clause}'

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"{expr} ~ '{regex_pattern}'"

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):
        # TODO add all of these specific statistical aggregate functions: https://docs.aws.amazon.com/redshift/latest/dg/c_Aggregate_Functions.html
        if metric_name in [
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "variance",
            "var_pop",
            "var_samp",
        ]:
            return f"{metric_name.upper()}({expr})"

        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    def expr_avg(self, expr):
        return f"AVG({expr}::real)"

    def regex_replace_flags(self) -> str:
        return ""

    @staticmethod
    def default_casify_table_name(identifier: str) -> str:
        return identifier.lower()

    @staticmethod
    def default_casify_column_name(identifier: str) -> str:
        return identifier.lower()

    @staticmethod
    def default_casify_type_name(identifier: str) -> str:
        return identifier.lower()

    def safe_connection_data(self):
        return [
            self.type,
            self.connection_properties.get("host"),
            self.connection_properties.get("port"),
            self.connection_properties.get("database"),
        ]
