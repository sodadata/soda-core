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

from sodasql.scan.db import sql_update
from tests.common.warehouse_fixture import WarehouseFixture


class RedshiftFixture(WarehouseFixture):

    original_dialect = None
    original_connection = None

    def create_database(self):
        self.database = self.create_unique_database_name()

        self.original_connection = self.warehouse.connection
        self.original_dialect = self.warehouse.dialect
        self.original_connection.set_isolation_level(0)
        sql_update(self.original_connection, f'CREATE DATABASE {self.database}')

        self.warehouse.dialect = self.warehouse.dialect.with_database(self.database)
        self.warehouse.connection = self.warehouse.dialect.create_connection()
        self.warehouse.dialect.sql_test_connection()

    def drop_database(self):
        try:
            self.warehouse.connection.close()
        except Exception as e:
            logging.debug(f'Closing connection failed: {str(e)}')
        sql_update(self.original_connection, f'DROP DATABASE {self.database}')



