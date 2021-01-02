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

import psycopg2

from sodasql.scan.dialect import Dialect
from sodasql.scan.parse_logs import ParseConfiguration
from sodasql.scan.scan_configuration import ScanConfiguration


class PostgresDialect(Dialect):

    def __init__(self, warehouse_cfg: ParseConfiguration):
        super().__init__()
        self.host = warehouse_cfg.get_str_optional('host', 'localhost')
        self.port = warehouse_cfg.get_str_optional('port', '5432')
        self.username = warehouse_cfg.get_str_optional('username', None)
        self.password = warehouse_cfg.get_str_optional('password', None)
        self.database = warehouse_cfg.get_str_optional('database', None)
        self.schema = warehouse_cfg.get_str_optional('schema', None)

    def sql_connection_test(self):
        pass

    def create_connection(self):
        return psycopg2.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            options=f'-c search_path={self.schema}' if self.schema else None)

    def sql_columns_metadata_query(self, scan_configuration: ScanConfiguration) -> str:
        sql = (f"SELECT column_name, data_type, is_nullable \n"
               f"FROM information_schema.columns \n"
               f"WHERE lower(table_name) = '{scan_configuration.table_name}'")
        if self.database:
            sql += f" \n  AND table_catalog = '{self.database}'"
        if self.schema:
            sql += f" \n  AND table_schema = '{self.schema}'"
        return sql

    def qualify_table_name(self, table_name: str) -> str:
        if self.schema:
            return f'"{self.schema}"."{table_name}"'
        return f'"{table_name}"'

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"{expr} ~* '{self.qualify_regex(pattern)}'"
