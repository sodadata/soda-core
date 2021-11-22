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
import logging

import mysql.connector
from typing import Optional

from sodasql.exceptions.exceptions import WarehouseConnectionError
from sodasql.scan.dialect import Dialect, MYSQL, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser

logger = logging.getLogger(__name__)


class MySQLDialect(Dialect):

    def __init__(self, parser: Parser = None, type: str = MYSQL):
        super().__init__(type)
        if parser:
            self.host = parser.get_str_optional_env('host', 'localhost')
            self.port = parser.get_str_optional_env('port', '3306')
            self.username = parser.get_str_required_env('username')
            self.password = parser.get_credential('password')
            self.database = parser.get_str_required_env('database')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: MYSQL,
            'host': 'localhost',
            'port': '3306',
            'username': 'env_var(MYSQL_USERNAME)',
            'password': 'env_var(MYSQL_PASSWORD)',
            'database': params.get('database', 'your_database')
        }

    def safe_connection_data(self):
        return [
            self.type,
            self.host,
            self.port,
            self.database,
        ]

    def default_env_vars(self, params: dict):
        return {
            'MYSQL_USERNAME': params.get('username', 'Eg johndoe'),
            'MYSQL_PASSWORD': params.get('password', 'Eg abc123')
        }

    def sql_tables_metadata_query(self, limit: Optional[int] = None, filter: str = None):
        sql = (f"SELECT TABLE_NAME \n"
               f"FROM information_schema.tables \n"
               f"WHERE lower(table_schema)='{self.database.lower()}'")
        if limit is not None:
            sql += f"\n LIMIT {limit}"
        return sql

    def sql_connection_test(self):
        pass

    def create_connection(self):
        try:
            conn = mysql.connector.connect(user=self.username,
                                           password=self.password,
                                           host=self.host,
                                           database=self.database)
            return conn
        except Exception as e:
            self.try_to_raise_soda_sql_exception(e)

    def sql_test_connection(self) -> bool:
        return True

    def sql_columns_metadata_query(self, table_name: str) -> str:
        sql = (f"SELECT column_name, data_type, is_nullable \n"
               f"FROM information_schema.columns \n"
               f"WHERE lower(table_name) = '{table_name}'")
        if self.database:
            sql += f" \n  AND table_schema = '{self.database}'"
        return sql

    def is_text(self, column_type: str):
        return column_type.upper() in ['CHAR', 'VARCHAR', 'BINARY', 'VARBINARY', 'BLOB', 'TEXT', 'ENUM', 'SET']

    def is_number(self, column_type: str):
        return column_type.upper() in ['INTEGER', 'INT', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'BIGINT',
                                       'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL', 'DOUBLE PRECISION',
                                       'DEC', 'FIXED']

    def is_time(self, column_type: str):
        return column_type.upper() in [
            'TIMESTAMP', 'DATE', 'DATETIME',
            'YEAR', 'TIME'
        ]

    def qualify_table_name(self, table_name: str) -> str:
        return f'{table_name}'

    def qualify_column_name(self, column_name: str):
        return f'{column_name}'

    def sql_expr_count_conditional(self, condition: str):
        return f'COUNT(CASE WHEN {condition} THEN 1 END) AS _'

    def qualify_regex(self, regex) -> str:
        return self.escape_metacharacters(regex)

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"{expr} regexp '{self.qualify_regex(pattern)}'"

    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS {self.data_type_decimal})"
        not_number_pattern = self.qualify_regex(r"[^-[0-9]\\.\\,]")
        comma_pattern = self.qualify_regex(r"\\,")
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, '{not_number_pattern}', ''), " \
               f"'{comma_pattern}', '\\.') AS {self.data_type_decimal})"
