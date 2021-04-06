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
from sodasql.scan.db import sql_updates
from tests.common.warehouse_fixture import WarehouseFixture


class SnowflakeFixture(WarehouseFixture):

    def create_database(self):
        self.database = self.create_unique_database_name()
        self.warehouse.dialect.database = self.database
        quoted_database_name = self.dialect.quote_identifier_declaration(self.database)
        sql_updates(self.warehouse.connection, [
            f'CREATE DATABASE IF NOT EXISTS {quoted_database_name}',
            f'USE DATABASE {quoted_database_name}'])
        self.warehouse.connection.commit()

