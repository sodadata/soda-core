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
from datetime import datetime

from tabulate import tabulate

from sodasql.scan.parse_logs import ParseLogs
from sodasql.warehouse.dialect import Dialect


class Warehouse:

    def __init__(self, warehouse_configuration: dict):
        self.parse_logs: ParseLogs = ParseLogs()
        self.warehouse_configuration = warehouse_configuration
        self.name: str = warehouse_configuration.get('name')
        self.dialect: Dialect = Dialect.create(warehouse_configuration, self.parse_logs)
        self.connection = self.dialect.create_connection()

    def execute_query_one(self, sql):
        cursor = self.connection.cursor()
        try:
            logging.debug(f'Executing SQL query: \n{sql}')
            start = datetime.now()
            cursor.execute(sql)
            row_tuple = cursor.fetchone()
            delta = datetime.now() - start
            logging.debug(f'SQL took {str(delta)}')
            return row_tuple
        finally:
            cursor.close()

    def execute_query_all(self, sql):
        cursor = self.connection.cursor()
        try:
            logging.debug(f'Executing SQL query: \n{sql}')
            start = datetime.now()
            cursor.execute(sql)
            rows = cursor.fetchall()
            delta = datetime.now() - start
            logging.debug(f'SQL took {str(delta)}')
            return rows
        finally:
            cursor.close()

    def create_scan(self, scan_configuration):
        return self.dialect.create_scan(self, scan_configuration)

    def close(self):
        self.connection.close()

