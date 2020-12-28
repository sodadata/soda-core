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

from sodasql.scan.warehouse import Warehouse
from tests.common.warehouse_fixture import WarehouseFixture


class PostgresFixture(WarehouseFixture):

    def initialize_warehouse_configuration(self, warehouse_configuration: dict):
        self.database = warehouse_configuration['database']

    def initialize_warehouse(self, warehouse: Warehouse):
        self.warehouse = warehouse

    def cleanup(self):
        pass
