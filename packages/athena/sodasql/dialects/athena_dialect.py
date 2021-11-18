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
from typing import Union, Optional
from datetime import date
from botocore.exceptions import ConnectionError, ClientError, ValidationError, ParamValidationError

import pyathena

from sodasql.exceptions.exceptions import WarehouseConnectionError
from sodasql.scan.dialect import ATHENA, KEY_WAREHOUSE_TYPE, Dialect
from sodasql.scan.parser import Parser
from sodasql.scan.aws_credentials import AwsCredentials


class AthenaDialect(Dialect):
    @staticmethod
    def get_aws_credentials_optional(parser: Parser):
        access_key_id = parser.get_str_optional_env('access_key_id')
        role_arn = parser.get_str_optional_env('role_arn')
        profile_name = parser.get_str_optional_env('profile_name')
        return AwsCredentials(
            access_key_id=access_key_id,
            secret_access_key=parser.get_credential('secret_access_key'),
            role_arn=role_arn,
            session_token=parser.get_credential('session_token'),
            region_name=parser.get_str_optional_env('region', 'eu-west-1'),
            profile_name=profile_name)

    def __init__(self, parser: Parser):
        super().__init__(ATHENA)

        if parser:
            self.aws_credentials = AthenaDialect.get_aws_credentials_optional(parser)
            self.athena_staging_dir = parser.get_str_required_env('staging_dir')
            self.database = parser.get_str_required_env('database')
            self.catalog = parser.get_str_optional_env('catalog')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: ATHENA,
            'access_key_id': 'env_var(ATHENA_ACCESS_KEY_ID)',
            'secret_access_key': 'env_var(ATHENA_SECRET_ACCESS_KEY)',
            'role_arn': 'Eg arn:aws:iam::123456789012:readonly',
            'region': 'Eg eu-west-1',
            'staging_dir': 'Eg s3://your_bucket_name/your_path',
            'database': params.get('database', 'Eg your_athena_db'),
            'catalog': 'AwsDataCatalog'
        }

    def safe_connection_data(self):
        return [
            self.type,
            self.athena_staging_dir,
        ]

    def default_env_vars(self, params: dict):
        return {
            'ATHENA_ACCESS_KEY_ID': '...',
            'ATHENA_SECRET_ACCESS_KEY': '...'
        }

    def create_connection(self):
        # pyathena.connect will do the role resolving
        # note that aws_credentials can be optional, in which case
        # pyathena will automatically resolve to the user's local
        # AWS configuration or environment vars.
        conn = pyathena.connect(
            profile_name=self.aws_credentials.profile_name if self.aws_credentials else None,
            aws_access_key_id=self.aws_credentials.access_key_id if self.aws_credentials else None,
            aws_secret_access_key=self.aws_credentials.secret_access_key if self.aws_credentials else None,
            s3_staging_dir=self.athena_staging_dir,
            region_name=self.aws_credentials.region_name if self.aws_credentials else None,
            role_arn=self.aws_credentials.role_arn if self.aws_credentials else None,
            catalog_name=self.catalog)
        return conn

    def sql_test_connection(self) -> Union[Exception, bool]:
        conn = self.create_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(self.sql_tables_metadata_query())
        except Exception as e:
            raise WarehouseConnectionError(
                warehouse_type=self.type,
                original_exception=Exception(
                    f'Unable to get tables metadata from database: {self.database}. Exception {e}'))
        return True

    def is_text(self, column_type: str):
        column_type_upper = column_type.upper()
        return (column_type_upper in ['CHAR', 'VARCHAR', 'STRING']
                or re.match(r'^VARCHAR\([0-9]+\)$', column_type_upper))

    def is_number(self, column_type: str):
        column_type_upper = column_type.upper()
        return (column_type_upper in ['TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'REAL',
                                      'DECIMAL']
                or re.match(r'^DECIMAL\([0-9]+(,[0-9]+)?\)$', column_type_upper))

    def is_time(self, column_type: str):
        return column_type.upper() in ['DATE', 'TIMESTAMP']

    def sql_tables_metadata_query(self, limit: Optional[int] = None, filter: str = None):
        # Alternative ( https://github.com/sodadata/soda-sql/pull/98/files )
        # return (f"SHOW tables IN `{self.database.lower()}`;")
        sql = (f"SELECT table_name \n"
               f"FROM information_schema.tables \n"
               f"WHERE lower(table_schema) = '{self.database.lower()}'")
        if limit is not None:
            sql += f"\n LIMIT {limit}"
        return sql + ';'

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
        return f"SUM(CAST({expr} as DECIMAL(38, 0)))"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE('{date_string}')"

    def is_connection_error(self, exception):
        if exception is None:
            return False
        return isinstance(exception, ConnectionError) or \
               isinstance(exception, ClientError) or \
               isinstance(exception, ValidationError) or \
               isinstance(exception, ParamValidationError)

    def is_authentication_error(self, exception):
        if exception is None:
            return False
        return isinstance(exception, pyathena.DatabaseError) or \
               isinstance(exception, pyathena.OperationalError)
