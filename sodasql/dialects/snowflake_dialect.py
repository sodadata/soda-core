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
from snowflake import connector

from sodasql.scan.dialect import Dialect, SNOWFLAKE
from sodasql.scan.parser import Parser
from sodasql.scan.scan_yml import ScanYml


class SnowflakeDialect(Dialect):

    def __init__(self, parser: Parser):
        super().__init__()
        self.account = parser.get_str_required_env('account')
        self.warehouse = parser.get_str_required_env('warehouse')
        self.username = parser.get_str_required_env('username')
        self.password = parser.get_credential('password')
        self.database = parser.get_str_optional_env('database')
        self.schema = parser.get_str_required_env('schema')

    def default_connection_properties(self, params: dict):
        return {
            'type': SNOWFLAKE,
            'username': 'env_var(SNOWFLAKE_USERNAME)',
            'password': 'env_var(SNOWFLAKE_PASSWORD)',
            'account': 'YOURACCOUNT.eu-central-1',
            'database': params.get('database','YOUR_DATABASE'),
            'warehouse': 'YOUR_WAREHOUSE',
            'schema': 'PUBLIC'
        }

    def default_env_vars(self, params: dict):
        return {
            'SNOWFLAKE_USERNAME': params.get('username', 'YOUR_SNOWFLAKE_USERNAME_GOES_HERE'),
            'SNOWFLAKE_PASSWORD': params.get('password', 'YOUR_SNOWFLAKE_PASSWORD_GOES_HERE')
        }

    def create_connection(self):
        return connector.connect(
            user=self.username,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )

    def sql_columns_metadata_query(self, scan_configuration: ScanYml) -> str:
        sql = (f"SELECT column_name, data_type, is_nullable "
                f'FROM "INFORMATION_SCHEMA"."COLUMNS" '
                f"WHERE table_name = '{scan_configuration.table_name.upper()}'")
        if self.database:
            sql += f" \n  AND table_catalog = '{self.database.upper()}'"
        if self.schema:
            sql += f" \n  AND table_schema = '{self.schema.upper()}'"
        return sql

    def qualify_regex(self, regex) -> str:
        return self.escape_regex_metacharacters(regex)
