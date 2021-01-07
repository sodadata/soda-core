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

import pyathena

from sodasql.scan.dialect import Dialect, ATHENA
from sodasql.scan.parser import Parser


class AthenaDialect(Dialect):

    def __init__(self, parser: Parser):
        super().__init__()
        self.aws_credentials = parser.get_aws_credentials_optional()
        self.athena_staging_dir = parser.get_str_required_env('staging_dir')
        self.database = parser.get_str_required_env('database')
        self.schema = parser.get_str_required_env('schema')

    def default_connection_properties(self, params: dict):
        return {
            'type': ATHENA,
            'access_key_id': 'env_var(ATHENA_ACCESS_KEY_ID)',
            'secret_access_key': 'env_var(ATHENA_SECRET_ACCESS_KEY)',
            'role_arn': 'Eg arn:aws:iam::123456789012:readonly',
            'region': 'Eg eu-west-1',
            'staging_dir': 'Eg s3://your_bucket_name/your_path',
            'database': params.get('database', 'Eg your_athena_db')
        }

    def default_env_vars(self, params: dict):
        return {
            'ATHENA_ACCESS_KEY_ID': '...',
            'ATHENA_SECRET_ACCESS_KEY': '...'
        }

    def create_connection(self):
        # pyathena.connect will do the role resolving
        # aws_credentials = self.aws_credentials.resolve_role('soda_scan')
        return pyathena.connect(
            aws_access_key_id=self.aws_credentials.access_key_id,
            aws_secret_access_key=self.aws_credentials.secret_access_key,
            s3_staging_dir=self.athena_staging_dir,
            region_name=self.aws_credentials.region_name,
            role_arn=self.aws_credentials.role_arn)

    def sql_columns_metadata_query(self, scan_configuration):
        return (f"SELECT column_name, data_type, is_nullable \n"
                f"FROM information_schema.columns \n"
                f"WHERE table_name = '{scan_configuration.table_name.lower()}' \n"
                f"  AND table_schema = '{self.database.lower()}';")
