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
from typing import Optional, List
from unittest import skip

from sodasql.warehouse.warehouse import Warehouse
from tests.common.all_warehouse_tests import AllWarehouseTests
from tests.common.sql_test_case import SqlTestCase


class TestRedshift(AllWarehouseTests):

    database: Optional[str] = None

    def setup_get_test_profile_target(self):
        return 'snowflake'

    def setup_init_warehouse(self):
        super().setup_init_warehouse()
        TestRedshift.database = self.database
        self.execute_sql_update(f'CREATE DATABASE IF NOT EXISTS {self.database}')

    @classmethod
    def tearDownClass(cls) -> None:
        cls.execute_sql_update(f'DROP DATABASE IF EXISTS {cls.database} CASCADE')
