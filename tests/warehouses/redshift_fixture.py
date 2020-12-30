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
from tests.common.warehouse_fixture import WarehouseFixture


class RedshiftFixture(WarehouseFixture):

    def initialize_warehouse_configuration(self, warehouse_configuration: dict):
        warehouse_configuration['database'] = 'dev'

    def create_database(self):
        self.database = self.setup_create_unique_database_name('soda_test')
        self.warehouse.warehouse_configuration['database'] = self.database
        self.warehouse.connection.set_isolation_level(0)
        sql_update(self.warehouse.connection, f'CREATE DATABASE {self.database}')
        self.warehouse.connection.set_isolation_level(1)

    def drop_database(self):
        self.warehouse.connection.set_isolation_level(0)
        sql_update(self.warehouse.connection, f'DROP DATABASE {self.database}')
        self.warehouse.connection.set_isolation_level(1)



