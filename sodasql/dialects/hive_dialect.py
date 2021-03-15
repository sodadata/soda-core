from pyhive import hive
from pyhive.exc import Error
from thrift.transport.TTransport import TTransportException

from sodasql.scan.dialect import Dialect, HIVE, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser
import json


class HiveDialect(Dialect):
    data_type_decimal = "DECIMAL"

    def __init__(self, parser: Parser):
        super().__init__(HIVE)
        if parser:
            self.host = parser.get_str_required('host')
            self.port = parser.get_int_optional('port', '10000')
            self.username = parser.get_str_required_env('username')
            self.password = parser.get_str_required_env('password')
            self.database = parser.get_str_optional('database', 'default')
            self.configuration = parser.get_dict_optional('configuration')

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: HIVE,
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

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        return (f"use {self.database.lower()};\n"
                f"show tables;")

    def create_connection(self, *args, **kwargs):
        self.configuration['hive.ddl.output.format'] = 'json'
        try:
            conn = hive.connect(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                # https://github.com/jaegertracing/jaeger-client-python/issues/151
                configuration={key: str(value)
                               for key, value in self.configuration.items()},
                auth=None)
            return conn
        except Exception as e:
            self.try_to_raise_soda_sql_exception(e)

    def sql_columns_metadata(self, table_name: str):
        # getting columns info from hive which version <3.x needs to be parsed
        column_tuples = []
        cursor = self.create_connection().cursor()
        try:
            cursor.execute(f"describe {self.database}.{table_name}")
            result = cursor.fetchall()[0][0]
            if result is not None:
                result_json = json.loads(result)
                for column in result_json['columns']:
                    column_tuples.append(
                        (column['name'], column['type'], 'YES'))
            return column_tuples
        finally:
            cursor.close()

    def sql_columns_metadata_query(self, table_name: str) -> str:
        # hive_version < 3.x does not support information_schema.columns
        return ''

    def qualify_table_name(self, table_name: str) -> str:
        return f'{self.database}.{table_name}'

    def qualify_writable_table_name(self, table_name: str) -> str:
        return self.qualify_table_name(table_name)

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"cast({expr} as string) rlike '{self.qualify_regex(pattern)}'"

    def is_text(self, column_type: str):
        for text_type in self._get_text_types():
            if column_type and text_type.upper() in column_type.upper():
                return True
        return False

    def _get_text_types(self):
        return ['CHAR', 'VARCHAR', 'STRING']

    def is_number(self, column_type: str):
        for number_type in self._get_number_types():
            if column_type and number_type.upper() in column_type.upper():
                return True
        return False

    def _get_number_types(self):
        return [
            'TINYINT',
            'SMALLINT',
            'INT',
            'BIGINT',
            'FLOAT',
            'DOUBLE',
            'DOUBLE PRECISION',
            'DECIMAL',
            'NUMERIC']

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
