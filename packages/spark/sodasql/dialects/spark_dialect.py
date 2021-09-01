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

from pyhive import hive
from pyhive.exc import Error
from thrift.transport.TTransport import TTransportException
import logging
from typing import Optional

from sodasql.scan.dialect import Dialect, SPARK, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser

logger = logging.getLogger(__name__)


class SparkDialect(Dialect):
    data_type_decimal = "DECIMAL"

    def __init__(self, parser: Parser):
        super().__init__(SPARK)
        if parser:
            self.host = parser.get_str_required('host')
            self.port = parser.get_int_optional('port', '10000')
            self.username = parser.get_credential('username')
            self.password = parser.get_credential('password')
            self.database = parser.get_str_optional('database', 'default')
            self.auth_method = parser.get_str_optional('authentication', None)
            self.configuration = parser.get_dict_optional('configuration')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: SPARK,
            'host': 'localhost',
            'port': 10000,
            'username': 'env_var(HIVE_USERNAME)',
            'password': 'env_var(HIVE_PASSWORD)',
            'database': params.get('database', 'your_database')
        }

    def default_env_vars(self, params: dict):
        return {
            'HIVE_USERNAME': params.get('username', 'hive_username_goes_here'),
            'HIVE_PASSWORD': params.get('password', 'hive_password_goes_here')
        }

    def sql_tables_metadata_query(self, limit: Optional[int] = None, filter: str = None):
        """Implements sql_tables_metadata instead."""
        pass

    def sql_tables_metadata(self, limit: str = 10, filter: str = None):
        # TODO Implement limit
        with self.create_connection().cursor() as cursor:
            cursor.execute(f"SHOW TABLES FROM {self.database};")
            return [(row[1],) for row in cursor.fetchall()]

    def create_connection(self, *args, **kwargs):
        try:
            conn = hive.connect(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                auth=self.auth_method)
            return conn
        except Exception as e:
            self.try_to_raise_soda_sql_exception(e)

    def sql_test_connection(self) -> bool:
        conn = self.create_connection()
        cursor = conn.cursor()
        tables = cursor.fetchall()
        if tables:
            for (table_name,) in cursor:
                test_query = self.__query_table(table_name)
                try:
                    cursor.execute(test_query)
                except Exception as e:
                    raise Exception(
                        f'Unable to query table: {table_name} from the database: {self.database}. Exception: {e}')
        else:
            logger.warning(f'{self.database} does not contain any tables.')
        return True

    def sql_columns_metadata(self, table_name: str):
        with self.create_connection().cursor() as cursor:
            cursor.execute(f"DESCRIBE TABLE {self.database}.{table_name}")
            return [(row[0], row[1], "YES") for row in cursor.fetchall()]

    def sql_columns_metadata_query(self, table_name: str) -> str:
        # hive_version < 3.x does not support information_schema.columns
        return ''

    def is_time(self, column_type: str):
        return column_type.upper() in ("DATE", "TIMESTAMP")

    def is_text(self, column_type: str):
        return (
            column_type.upper() in ['CHAR', 'STRING'] or
            column_type.upper().startswith('VARCHAR')
        )

    def is_number(self, column_type: str):
        return column_type.upper() in [
            'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
            'FLOAT', 'DOUBLE', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC']

    def qualify_table_name(self, table_name: str) -> str:
        return f'{self.database}.{table_name}'

    def qualify_writable_table_name(self, table_name: str) -> str:
        return self.qualify_table_name(table_name)

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"cast({expr} as string) rlike '{self.qualify_regex(pattern)}'"

    def sql_expr_stddev(self, expr: str):
        return f'STDDEV_POP({expr})'

    def qualify_regex(self, regex) -> str:
        return self.escape_metacharacters(regex).replace("'", "\\'")

    def is_connection_error(self, exception):
        if exception is None:
            return False
        return isinstance(exception, Error)

    def is_authentication_error(self, exception):
        if exception is None:
            return False
        return isinstance(exception, TTransportException)
