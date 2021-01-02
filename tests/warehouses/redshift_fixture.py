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
from sodasql.scan.db import sql_update
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_configuration import WarehouseConfiguration
from tests.common.warehouse_fixture import WarehouseFixture


class RedshiftFixture(WarehouseFixture):

    original_connection = None

    def create_database(self):
        self.database = self.create_unique_database_name()

        self.original_connection = self.warehouse.connection
        self.original_connection.set_isolation_level(0)
        sql_update(self.original_connection, f'CREATE DATABASE {self.database}')

        warehouse_configuration = WarehouseConfiguration()
        warehouse_configuration.properties = self.warehouse.warehouse_properties.copy()
        warehouse_configuration.properties['database'] = self.database
        warehouse_configuration.name = 'test_db'
        warehouse_configuration.dialect = self.warehouse.dialect
        warehouse_configuration.dialect.database = self.database
        warehouse = Warehouse(warehouse_configuration)
        self.warehouse.connection = warehouse.connection

    def drop_database(self):
        self.warehouse.connection.close()
        sql_update(self.original_connection, f'DROP DATABASE {self.database}')



