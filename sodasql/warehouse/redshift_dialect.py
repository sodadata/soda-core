#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import boto3
import psycopg2

from sodasql.credentials.aws_credentials import AwsCredentials
from sodasql.credentials.credentials_resolver import CredentialsResolver
from sodasql.scan.parse_logs import ParseLogs
from sodasql.warehouse.dialect import Dialect
from sodasql.warehouse.postgres_dialect import PostgresDialect


class RedshiftDialect(PostgresDialect):

    def __init__(self, warehouse_configuration: dict, parse_logs: ParseLogs):
        super().__init__(warehouse_configuration, parse_logs)
        self.port = warehouse_configuration.get('port', '5432')
        self.aws_credentials = AwsCredentials.from_configuration(warehouse_configuration)

    def create_connection(self):
        if self.password:
            resolved_username = self.username
            resolved_password = self.password
        else:
            resolved_username, resolved_password = self.__get_cluster_credentials()
        return psycopg2.connect(
            user=resolved_username,
            password=resolved_password,
            host=self.host,
            port=self.port,
            database=self.database)

    def __get_cluster_credentials(self):
        resolved_aws_credentials = self.aws_credentials.resolve_role(role_session_name="SodaAgentGetClusterCredentials")

        client = boto3.client('redshift',
                              region_name=resolved_aws_credentials.region_name,
                              aws_access_key_id=resolved_aws_credentials.access_key_id,
                              aws_secret_access_key=resolved_aws_credentials.secret_access_key,
                              aws_session_token=resolved_aws_credentials.session_token)

        cluster_name = self.host.split('.')[0]
        username = self.username
        db_name = self.database
        cluster_creds = client.get_cluster_credentials(DbUser=username,
                                                       DbName=db_name,
                                                       ClusterIdentifier=cluster_name,
                                                       AutoCreate=False,
                                                       DurationSeconds=3600)

        return cluster_creds['DbUser'], cluster_creds['DbPassword']
