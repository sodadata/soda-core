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

from sodasql.credentials.credentials_resolver import CredentialsResolver
from sodasql.scan.dialect import Dialect
from sodasql.scan.parse_logs import ParseLogs
from sodasql.scan.scan_configuration import ScanConfiguration


class SnowflakeDialect(Dialect):

    def __init__(self, warehouse_configuration: dict, parse_logs: ParseLogs):
        super().__init__()
        self.account = warehouse_configuration.get('account')
        self.warehouse = warehouse_configuration.get('warehouse')
        self.username = CredentialsResolver.resolve(warehouse_configuration, 'username', parse_logs)
        self.password = CredentialsResolver.resolve(warehouse_configuration, 'password', parse_logs)
        self.database = warehouse_configuration.get('database')
        self.schema = warehouse_configuration.get('schema')

    def create_connection(self):
        return connector.connect(
            user=self.username,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )

    def sql_columns_metadata_query(self, scan_configuration: ScanConfiguration) -> str:
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
