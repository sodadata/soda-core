#  (c) 2021 Walt Disney Parks and Resorts U.S., Inc.
#  (c) 2021 Soda Data NV.
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
import logging
import trino

from sodasql.scan.dialect import Dialect, TRINO, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser

logger = logging.getLogger(__name__)


class TrinoDialect(Dialect):
    reserved_keywords = [
        "ALTER",
        "AND",
        "AS",
        "BETWEEN",
        "BY",
        "CASE",
        "CAST",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CUBE",
        "CURRENT_CATALOG",
        "CURRENT_DATE",
        "CURRENT_PATH",
        "CURRENT_ROLE",
        "CURRENT_SCHEMA",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DEALLOCATE",
        "DELETE",
        "DESCRIBE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "END",
        "ESCAPE",
        "EXCEPT",
        "EXECUTE",
        "EXISTS",
        "EXTRACT",
        "FALSE",
        "FOR",
        "FROM",
        "FULL",
        "GROUP",
        "GROUPING",
        "HAVING",
        "IN",
        "INNER",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "JOIN",
        "LEFT",
        "LIKE",
        "LISTAGG",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "NATURAL",
        "NORMALIZE",
        "NOT",
        "NULL",
        "ON",
        "OR",
        "ORDER",
        "OUTER",
        "PREPARE",
        "RECURSIVE",
        "RIGHT",
        "ROLLUP",
        "SELECT",
        "SKIP",
        "TABLE",
        "THEN",
        "TRUE",
        "UESCAPE",
        "UNION",
        "UNNEST",
        "USING",
        "VALUES",
        "WHEN",
        "WHERE",
        "WITH",
    ]

    def __init__(self, parser: Parser):
        super().__init__(TRINO)
        if parser:
            self.host = parser.get_str_optional_env('host', 'localhost')
            self.port = parser.get_str_optional_env('port', '443')
            self.http_scheme = parser.get_str_optional_env('http_scheme', 'https')
            self.username = parser.get_str_required_env('username')
            self.password = parser.get_credential('password')
            self.catalog = parser.get_str_required_env('catalog')
            self.schema = parser.get_str_required_env('schema')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: TRINO,
            'host': 'localhost',
            'port': '443',
            'http_scheme': 'https',
            'catalog': params.get('catalog', 'YOUR_CATALOG').lower(),
            'schema': params.get('schema', 'YOUR_DATABASE').lower(),
            'username': 'env_var(TRINO_USERNAME)',
            'password': 'env_var(TRINO_PASSWORD)',
        }

    def default_env_vars(self, params: dict):
        return {
            'TRINO_USERNAME': params.get('username', 'YOUR_TRINO_USERNAME_GOES_HERE'),
            'TRINO_PASSWORD': params.get('password', 'YOUR_TRINO_PASSWORD_GOES_HERE')
        }

    def create_connection(self):
        try:
            conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                catalog=self.catalog,
                schema=self.schema,
                http_scheme=self.http_scheme,
                auth=trino.auth.BasicAuthentication(self.username, self.password)
            )
            return conn
        except Exception as e:
            self.try_to_raise_soda_sql_exception(e)

    def sql_tables_metadata_query(self, limit: int = 10, filter: str = None):
        sql = (f"SELECT table_name \n"
               f"FROM {self.catalog}.information_schema.tables \n"
               f"WHERE lower(table_schema) = '{self.schema}' \n"
               f"  AND lower(table_catalog) = '{self.catalog}'")
        return sql

    def sql_columns_metadata_query(self, table_name: str) -> str:
        sql = (f"SELECT column_name, data_type, is_nullable \n"
               f"FROM {self.catalog}.information_schema.columns \n"
               f"WHERE lower(table_name) = '{table_name.lower()}' \n"
               f"  AND lower(table_catalog) = '{self.catalog}' \n"
               f"  AND lower(table_schema) = '{self.schema}'")
        return sql

    def is_text(self, column_type: str):
        column_type_upper = column_type.upper()
        return (column_type_upper in ['VARCHAR', 'CHAR', 'VARBINARY', 'JSON']
                or re.match(r'^(VAR)?CHAR\([0-9]+\)$', column_type_upper))

    def is_number(self, column_type: str):
        column_type_upper = column_type.upper()
        return (column_type_upper in ['BOOLEAN',
                                      'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT',
                                      'DOUBLE', 'REAL', 'DECIMAL']
                or re.match(r'^DECIMAL\([0-9]+(,[0-9]+)?\)$', column_type_upper))

    def is_time(self, column_type: str):
        column_type_upper = column_type.upper()
        return (column_type_upper in ['DATE', 'TIME', 'TIMESTAMP',
                                      'TIME WITH TIME ZONE', 'INTERVAL YEAR TO MONTH', 'INTERVAL DAY TO SECOND']
                or re.match(r'^TIMESTAMP\([0-9]+\)$', column_type_upper))

    def qualify_table_name(self, table_name: str) -> str:
        return f'"{self.catalog}"."{self.schema}"."{table_name}"'

    def qualify_column_name(self, column_name: str, source_type: str = None):
        if source_type is not None and re.match(r'^CHAR\([0-9]+\)$', source_type.upper()):
            return f'CAST({column_name} AS VARCHAR)'
        elif column_name in self.reserved_keywords:
            return f'"{column_name}"'
        return column_name

    def is_connection_error(self, exception):
        logger.error(exception)
        if exception is None or exception.errno is None:
            return False
        return isinstance(exception, trino.exceptions.HttpError) or \
               isinstance(exception, trino.exceptions.Http503Error) or \
               isinstance(exception, trino.exceptions.TrinoError) or \
               isinstance(exception, trino.exceptions.TimeoutError)

    def is_authentication_error(self, exception):
        logger.error(exception)
        if exception is None or exception.errno is None:
            return False
        return True
