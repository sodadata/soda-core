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

import pyodbc
from collections import namedtuple
from enum import Enum
from pyhive import hive
from pyhive.exc import Error
from thrift.transport.TTransport import TTransportException
import logging
from typing import Any, Dict, List, Optional

from sodasql.exceptions.exceptions import WarehouseConnectionError
from sodasql.__version__ import SODA_SQL_VERSION
from sodasql.scan.dialect import Dialect, SPARK, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser

logger = logging.getLogger(__name__)
ColumnMetadata = namedtuple("ColumnMetadata", ["name", "data_type", "is_nullable"])


def hive_connection_function(
    username: str,
    password: str,
    host: str,
    port: str,
    database: str,
    auth_method: str,
    **kwargs,
) -> hive.Connection:
    """
    Connect to hive.

    Parameters
    ----------
    username : str
        The user name
    password : str
        The password
    host: str
        The host.
    port : str
        The port
    database : str
        The databse
    auth_method : str
        The authentication method

    Returns
    -------
    out : hive.Connection
        The hive connection
    """
    connection = hive.connect(
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        auth=auth_method
    )
    return connection


def _build_odbc_connnection_string(**kwargs: Any) -> str:
    return ";".join([f"{k}={v}" for k, v in kwargs.items()])


def odbc_connection_function(
    driver: str,
    host: str,
    port: str,
    token: str,
    organization: str,
    cluster: str,
    server_side_parameters: Dict[str, str],
    **kwargs,
) -> pyodbc.Connection:
    """
    Connect to hive.

    Parameters
    ----------
    driver : str
        The path to the driver
    host: str
        The host.
    port : str
        The port
    token : str
        The login token
    organization : str
        The organization
    cluster : str
        The cluster
    server_side_parameters : Dict[str]
        The server side parameters

    Returns
    -------
    out : pyobc.Connection
        The connection
    """
    http_path = f"/sql/protocolv1/o/{organization}/{cluster}"
    user_agent_entry = f"soda-sql-spark/{SODA_SQL_VERSION} (Databricks)"

    connection_str = _build_odbc_connnection_string(
        DRIVER=driver,
        HOST=host,
        PORT=port,
        UID="token",
        PWD=token,
        HTTPPath=http_path,
        AuthMech=3,
        SparkServerType=3,
        ThriftTransport=2,
        SSL=1,
        UserAgentEntry=user_agent_entry,
        LCaseSspKeyName=0 if server_side_parameters else 1,
        **server_side_parameters,
    )
    connection = pyodbc.connect(connection_str, autocommit=True)
    return connection


class SparkConnectionMethod(str, Enum):
    HIVE = "hive"
    ODBC = "odbc"


class SparkDialect(Dialect):
    data_type_decimal = "DECIMAL"

    def __init__(self, parser: Parser):
        super().__init__(SPARK)
        if parser:
            self.method = parser.get_str_optional('method', 'hive')
            self.host = parser.get_str_required('host')
            self.port = parser.get_int_optional('port', '10000')
            self.username = parser.get_credential('username')
            self.password = parser.get_credential('password')
            self.database = parser.get_str_optional('database')
            self.auth_method = parser.get_str_optional('authentication', None)
            self.configuration = parser.get_dict_optional('configuration', {})
            self.driver = parser.get_str_optional('driver', None)
            self.token = parser.get_credential('token')
            self.organization = parser.get_str_optional('organization', None)
            self.cluster = parser.get_str_optional('cluster', None)
            self.server_side_parameters = {
                f"SSP_{k}": f"{{{v}}}"
                for k, v in parser.get_dict_optional("server_side_parameters", {})
            }

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: SPARK,
            'host': 'localhost',
            'port': 10000,
            'username': 'env_var(HIVE_USERNAME)',
            'password': 'env_var(HIVE_PASSWORD)',
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
            'HIVE_USERNAME': params.get('username', 'hive_username_goes_here'),
            'HIVE_PASSWORD': params.get('password', 'hive_password_goes_here')
        }

    def sql_tables_metadata_query(self, limit: Optional[int] = None, filter: str = None):
        """Implements sql_tables_metadata instead."""
        pass

    def sql_tables_metadata(self, limit: str = 10, filter: str = None):
        # TODO Implement limit
        if self.database is None:
            raise NotImplementedError("Cannot query for tables when database is not given.")

        with self.create_connection().cursor() as cursor:
            cursor.execute(f"SHOW TABLES FROM {self.database}")
            return [(row[1],) for row in cursor.fetchall()]

    def create_connection(self, *args, **kwargs):
        if self.method == SparkConnectionMethod.HIVE:
            connection_function = hive_connection_function
        elif self.method == SparkConnectionMethod.ODBC:
            connection_function = odbc_connection_function
        else:
            raise NotImplementedError(f"Unknown Spark connection method {self.method}")

        try:
            connection = connection_function(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                auth_method=self.auth_method,
                driver=self.driver,
                token=self.token,
                organization=self.organization,
                cluster=self.cluster,
                server_side_parameters=self.server_side_parameters,
            )
        except Exception as e:
            self.try_to_raise_soda_sql_exception(e)
        else:
            return connection

    def sql_test_connection(self) -> bool:
        return True

    def show_columns(self, table_name: str) -> List[str]:
        """
        Show columns in table.

        Parameters
        ----------
        table_name : str
            The table to show columns in.

        Returns
        -------
        out : List[str]
            The column names.
        """
        qualified_table_name = self.qualify_table_name(table_name)
        with self.create_connection().cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS IN {qualified_table_name}")
            columns = cursor.fetchall()
        return [column[0] for column in columns]

    def describe_column(
        self, table_name: str, column_name: str) -> ColumnMetadata:
        """
        Describe a column.

        Parameters
        ----------
        table_name: str
            The table name.
        column_name: str
            The column name.

        Returns
        -------
        out : ColumnMetadata
            The column metadata.
        """
        qualified_table_name = self.qualify_table_name(table_name)
        with self.create_connection().cursor() as cursor:
            cursor.execute(f"DESCRIBE TABLE {qualified_table_name} {column_name}")
            data_type = cursor.fetchall()[1][1]
        return ColumnMetadata(column_name, data_type, is_nullable="YES")

    def sql_columns_metadata(self, table_name: str) -> List[ColumnMetadata]:
        """
        Get the metadata for all columns.

        Parameters
        ----------
        table_name : str
            The table name.

        Returns
        -------
        out : List[ColumnMetada]
            The metadata about each column.
        """
        columns = self.show_columns(table_name)
        columns_metadata = [
            self.describe_column(table_name, column)
            for column in columns
        ]
        return columns_metadata

    def sql_columns_metadata_query(self, table_name: str) -> str:
        # hive_version < 3.x does not support information_schema.columns
        return ''

    def is_time(self, column_type: str):
        return column_type.upper() in ("DATE", "TIMESTAMP")

    def is_text(self, column_type: str):
        return column_type \
            .upper(). \
            startswith(('CHAR', 'STRING', 'VARCHAR'))

    def is_number(self, column_type: str):
        # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
        # TINYINT is treated as ByteType
        # startswith INTEGER is redundant, here for the sake of completeness same goes for DECIMAL
        return column_type \
            .upper() \
            .startswith(('TINYINT',
                         'SHORT', 'SMALLINT',
                         'INT', 'INTEGER',
                         'LONG', 'BIGINT',
                         'FLOAT', 'REAL',
                         'DOUBLE',
                         'DEC', 'DECIMAL', 'NUMERIC'))

    def qualify_table_name(self, table_name: str) -> str:
        if self.database is None:
            qualified_table_name = table_name
        else:
            qualified_table_name = f'{self.database}.{table_name}'
        return qualified_table_name

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
