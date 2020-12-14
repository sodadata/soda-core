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
from typing import List

import psycopg2

from sodasql.credentials.credentials_resolver import CredentialsResolver
from sodasql.scan.column import Column
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.sql_store.sql_store import SqlStore


class PostgresSqlStore(SqlStore):

    def __init__(self, connection_dict: dict):
        super().__init__(connection_dict)
        self.host = connection_dict.get('host', 'localhost')
        self.port = connection_dict.get('port', '5432')
        self.username = CredentialsResolver.resolve(connection_dict, 'username')
        self.password = CredentialsResolver.resolve(connection_dict, 'password')
        self.database = connection_dict.get('database')
        self.schema = connection_dict.get('schema')

    def create_connection(self):
        return psycopg2.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database)

    def sql_columns_metadata_query(self, scan_configuration: ScanConfiguration) -> str:
        sql = (f"SELECT column_name, data_type, is_nullable \n"
               f"FROM information_schema.columns \n"
               f"WHERE lower(table_name) = '{scan_configuration.table_name}'")
        if self.database:
            sql += f" \nAND table_catalog = '{self.database}'"
        if self.schema:
            sql += f" \nAND table_schema = '{self.schema}'"
        return sql

    def qualify_table_name(self, table_name: str) -> str:
        if self.schema:
            return f'"{self.schema}"."{table_name}"'
        return f'"{table_name}"'
