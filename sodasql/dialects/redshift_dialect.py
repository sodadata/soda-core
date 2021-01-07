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

from sodasql.dialects.postgres_dialect import PostgresDialect
from sodasql.scan.dialect import REDSHIFT
from sodasql.scan.dialect_parser import DialectParser
from sodasql.scan.parser import Parser


class RedshiftDialect(PostgresDialect):

    def __init__(self, parser: Parser):
        super().__init__(parser)
        self.port = parser.get_str_optional('port', '5439')
        self.aws_credentials = parser.get_aws_credentials_optional()

    def with_database(self, database: str):
        warehouse_configuration = {
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'database': database,
            'schema': self.schema
        }
        if self.aws_credentials:
            warehouse_configuration.update({
                'access_key_id': self.aws_credentials.access_key_id,
                'secret_access_key': self.aws_credentials.secret_access_key,
                'role_arn': self.aws_credentials.role_arn,
                'session_token': self.aws_credentials.session_token,
                'region': self.aws_credentials.region
            })
        return RedshiftDialect(DialectParser(warehouse_configuration))

    def default_connection_properties(self, params: dict):
        return {
            'type': REDSHIFT,
            'access_key_id': 'env_var(ATHENA_ACCESS_KEY_ID)',
            'secret_access_key': 'env_var(ATHENA_SECRET_ACCESS_KEY)',
            'role_arn': 'Eg arn:aws:iam::123456789012:readonly',
            'region': 'Eg eu-west-1',
            'database': params.get('database', 'Eg your_redshift_db')
        }

    def default_env_vars(self, params: dict):
        return {
            'ATHENA_ACCESS_KEY_ID': '...',
            'ATHENA_SECRET_ACCESS_KEY': '...'
        }

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
        resolved_aws_credentials = self.aws_credentials.resolve_role(role_session_name="soda_redshift_get_cluster_credentials")

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

    def qualify_regex(self, regex):
        """
        Redshift regex's required the following transformations:
            - Escape metacharacters.
            - Removal of non-capturing-groups (not allowed).
        """
        return self.escape_regex_metacharacters(regex) \
            .replace('(?:', '(')
