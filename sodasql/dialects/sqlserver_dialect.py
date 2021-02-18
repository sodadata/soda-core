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

import pyodbc

from sodasql.scan.dialect import Dialect, SQLSERVER, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser

"""
Connecting to Microsoft SQL Example with pyodbc

server = '<server>.database.windows.net'
database = '<database>'
username = '<username>'
password = '<password>'   
driver= '{ODBC Driver 17 for SQL Server}'


with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT TOP 3 name, collation_name FROM sys.databases")
        row = cursor.fetchone()
        while row:
            print (str(row[0]) + " " + str(row[1]))
            row = cursor.fetchone()

"""


class SQLServerDialect(Dialect):

    def __init__(self, parser: Parser = None, type: str = SQLSERVER):
        super().__init__(type)
        if parser:
            self.host = parser.get_str_optional_env('host', 'localhost')
            self.port = parser.get_str_optional_env('port', '1433')
            self.driver = parser.get_str_optional_env('driver', 'ODBC Driver 17 for SQL Server')
            self.username = parser.get_str_required_env('username')
            self.password = parser.get_credential('password')
            self.database = parser.get_str_required_env('database')
            self.schema = parser.get_str_required_env('schema')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: SQLSERVER,
            'host': 'localhost',
            'username': 'env_var(SQLSERVER_USERNAME)',
            'password': 'env_var(SQLSERVER_PASSWORD)',
            'database': params.get('database', 'your_database'),
            'schema': 'public'
        }

    def default_env_vars(self, params: dict):
        return {
            'SQLSERVER_USERNAME': params.get('username', 'Eg johndoe'),
            'SQLSERVER_PASSWORD': params.get('password', 'Eg abc123')
        }

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        return (f"SELECT TABLE_NAME \n"
                f"FROM information_schema.tables \n"
                f"WHERE lower(table_schema)='{self.schema.lower()}'")

    def sql_connection_test(self):
        pass

    def create_connection(self):
        return pyodbc.connect(
            'DRIVER={' + self.driver +
            '};SERVER=' + self.host +
            ';PORT=' + self.port +
            ';DATABASE=' + self.database +
            ';UID=' + self.username +
            ';PWD=' + self.password)

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

    def sql_expr_length(self, expr):
        return f'LEN({expr})'

    def sql_expr_variance(self, expr: str):
        return f'VAR({expr})'

    def sql_expr_stddev(self, expr: str):
        return f'STDEV({expr})'

    # TODO REGEX not supported by SQL SERVER
    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS {self.data_type_decimal})"
        not_number_pattern = self.qualify_regex(r"[^-\d\.\,]")
        comma_pattern = self.qualify_regex(r"\,")
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, '{not_number_pattern}', '', 'g'), " \
               f"'{comma_pattern}', '.', 'g') AS {self.data_type_decimal})"
