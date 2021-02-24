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
import re
from datetime import date

import pyathena

from sodasql.scan.dialect import Dialect, ATHENA, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser


class AthenaDialect(Dialect):

    def __init__(self, parser: Parser):
        super().__init__(ATHENA)
        if parser:
            self.aws_credentials = parser.get_aws_credentials_optional()
            self.athena_staging_dir = parser.get_str_required_env('staging_dir')
            self.database = parser.get_str_required_env('database')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: ATHENA,
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

    def create_connection(self, *args, **kwargs):
        # pyathena.connect will do the role resolving
        # aws_credentials = self.aws_credentials.resolve_role('soda_scan')
        return pyathena.connect(
            aws_access_key_id=self.aws_credentials.access_key_id,
            aws_secret_access_key=self.aws_credentials.secret_access_key,
            s3_staging_dir=self.athena_staging_dir,
            region_name=self.aws_credentials.region_name,
            role_arn=self.aws_credentials.role_arn,
        )

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        # Alternative ( https://github.com/sodadata/soda-sql/pull/98/files )
        # return (f"SHOW tables IN `{self.database.lower()}`;")
        return (f"SELECT table_name \n"
                f"FROM information_schema.tables \n"
                f"WHERE lower(table_schema) = '{self.database.lower()}';")

    def sql_columns_metadata_query(self, table_name: str):
        return (f"SELECT column_name, data_type, is_nullable \n"
                f"FROM information_schema.columns \n"
                f"WHERE table_name = '{table_name.lower()}' \n"
                f"  AND table_schema = '{self.database.lower()}';")

    def qualify_column_name(self, column_name):
        return f'"{column_name}"'

    def qualify_table_name(self, table_name: str) -> str:
        return f'"{self.database}"."{table_name}"'

    def qualify_writable_table_name(self, table_name: str) -> str:
        return f'`{self.database}`.`{table_name}`'

    def sql_expr_avg(self, expr: str):
        return f"AVG(CAST({expr} as DECIMAL(38, 0)))"

    def sql_expr_sum(self, expr: str):
        return f"SUM(CAST ({expr} as DECIMAL(38, 0)))"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE('{date_string}')"

    def is_connection_error(self, exception):
        if exception is None:
            return False
        error_message = str(exception)
        return error_message.find('Could not connect to the endpoint URL') != -1

    def is_authentication_error(self, exception):
        if exception is None:
            return False
        error_message = str(exception)
        return error_message.find('InvalidSignatureException') != -1 or \
               error_message.find('Access denied when writing output') != -1 or \
               error_message.find('The security token included in the request is invalid') != -1
