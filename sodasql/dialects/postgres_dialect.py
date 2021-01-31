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

from sodasql.scan.dialect import Dialect, POSTGRES, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser


class PostgresDialect(Dialect):

    def __init__(self, parser: Parser, type: str = POSTGRES):
        super().__init__(type)
        self.host = parser.get_str_optional_env('host', 'localhost')
        self.port = parser.get_str_optional_env('port', '5432')
        self.username = parser.get_str_required_env('username')
        self.password = parser.get_credential('password')
        self.database = parser.get_str_required_env('database')
        self.schema = parser.get_str_required_env('schema')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: POSTGRES,
            'host': 'localhost',
            'username': 'env_var(POSTGRES_USERNAME)',
            'password': 'env_var(POSTGRES_PASSWORD)',
            'database': params.get('database', 'your_database'),
            'schema': 'public'
        }

    def default_env_vars(self, params: dict):
        return {
            'POSTGRES_USERNAME': params.get('username', 'Eg johndoe'),
            'POSTGRES_PASSWORD': params.get('password', 'Eg abc123')
        }

    def sql_tables_metadata_query(self, filter: str = None):
        return (f"SELECT table_name \n" 
                f"FROM information_schema.tables \n" 
                f"WHERE lower(table_schema)='{self.schema.lower()}'")

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

    def sql_columns_metadata_query(self, table_name: str) -> str:
        sql = (f"SELECT column_name, data_type, is_nullable \n"
               f"FROM information_schema.columns \n"
               f"WHERE lower(table_name) = '{table_name}'")
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

    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS {self.decimal_column_type})"
        not_number_pattern = self.qualify_regex(r"[^-\d\.\,]")
        comma_pattern = self.qualify_regex(r"\,")
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, '{not_number_pattern}', '', 'g'), "\
               f"'{comma_pattern}', '.', 'g') AS {self.decimal_column_type})"
