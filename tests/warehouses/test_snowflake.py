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
from typing import Optional

from sodasql.warehouse.db import sql_update
from sodasql.warehouse.warehouse import Warehouse
from tests.common.all_warehouse_tests import AllWarehouseTests


class TestSnowflake(AllWarehouseTests):

    warehouse = None
    database: Optional[str] = None

    def setup_get_test_profile_target(self):
        return 'snowflake'

    def setup_create_warehouse(self, warehouse_configuration: dict) -> Warehouse:
        warehouse = super().setup_create_warehouse(warehouse_configuration)
        sql_update(warehouse.connection,
                   f'CREATE DATABASE IF NOT EXISTS {self.database}')
        TestSnowflake.warehouse = warehouse
        TestSnowflake.database = self.database
        return warehouse

    @classmethod
    def tearDownClass(cls) -> None:
        sql_update(cls.warehouse.connection,
                   f'DROP DATABASE IF EXISTS {TestSnowflake.database} CASCADE')
