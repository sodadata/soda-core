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

from os import path
from typing import List

from tests.common.sql_test_case import TARGET_ATHENA
from tests.common.sql_test_suite import SqlTestSuite


class AthenaSuite(SqlTestSuite):

    def setUp(self) -> None:
        self.target = TARGET_ATHENA
        super().setUp()

    # def sql_create_table(self, columns: List[str], table_name: str):
    #     columns_sql = ", ".join(columns)
    #     database_location = self.warehouse_fixture_cache_by_target[TARGET_ATHENA].database_location
    #     table_location = path.join(self.warehouse.dialect.athena_staging_dir, database_location, table_name)
    #     return f"CREATE EXTERNAL TABLE " \
    #            f"{self.warehouse.dialect.qualify_writable_table_name(table_name)} ( \n " \
    #            f"{columns_sql} ) \n " \
    #            f"LOCATION '{table_location}';"
