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
from datetime import date

import boto3
import psycopg2

from sodasql.dialects.postgres_dialect import PostgresDialect
from sodasql.scan.dialect import REDSHIFT, KEY_WAREHOUSE_TYPE
from sodasql.scan.dialect_parser import DialectParser
from sodasql.scan.parser import Parser


class RedshiftDialect(PostgresDialect):

    def __init__(self, parser: Parser):
        super().__init__(parser, REDSHIFT)
        if parser:
            self.port = parser.get_str_optional('port', '5439')
            self.aws_credentials = parser.get_aws_credentials_optional()

    def with_database(self, database: str):
        warehouse_connection_dict = {
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'database': database,
            'schema': self.schema
        }
        if self.aws_credentials:
            warehouse_connection_dict.update({
                'access_key_id': self.aws_credentials.access_key_id,
                'secret_access_key': self.aws_credentials.secret_access_key,
                'role_arn': self.aws_credentials.role_arn,
                'session_token': self.aws_credentials.session_token,
                'region': self.aws_credentials.region
            })
        return RedshiftDialect(DialectParser(warehouse_connection_dict))

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: REDSHIFT,
            'host': 'Eg your.redshift.host.com',
            'username': 'env_var(REDSHIFT_USERNAME)',
            'password': 'env_var(REDSHIFT_PASSWORD)',
            'region': 'Eg eu-west-1',
            'database': params.get('database', 'Eg your_redshift_db')
        }

    def default_env_vars(self, params: dict):
        return {
            'REDSHIFT_USERNAME': '...',
            'REDSHIFT_PASSWORD': '...'
        }

    def create_connection(self, *args, **kwargs):
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
            connect_timeout=kwargs.get('connection_timeout_sec', None),
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
        return self.escape_metacharacters(regex)

    def sql_expr_avg(self, expr: str):
        return f"AVG({expr}::DECIMAL(38, 0))"

    def sql_expr_sum(self, expr: str):
        return f"SUM({expr}::DECIMAL(38, 0))"

    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS {self.data_type_decimal})"
        not_number_pattern = self.qualify_regex(r"[^-\d\.\,]")
        comma_pattern = self.qualify_regex(r"\,")
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, '{not_number_pattern}', ''), "\
               f"'{comma_pattern}', '.') AS {self.data_type_decimal})"
