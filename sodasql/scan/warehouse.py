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

from sodasql.scan.db import sql_fetchone, sql_fetchall, sql_fetchone_description, sql_fetchall_description
from sodasql.scan.dialect import Dialect
from sodasql.scan.warehouse_yml import WarehouseYml


class Warehouse:

    def __init__(self, warehouse_yml: WarehouseYml, *args, **kwargs):
        self.name = warehouse_yml.name
        self.dialect: Dialect = warehouse_yml.dialect
        self.connection = self.dialect.create_connection(*args, **kwargs)

    def sql_fetchone(self, sql) -> tuple:
        return sql_fetchone(self.connection, sql)

    def sql_fetchone_description(self, sql) -> tuple:
        return sql_fetchone_description(self.connection, sql)

    def sql_fetchall(self, sql) -> List[tuple]:
        return sql_fetchall(self.connection, sql)

    def sql_fetchall_description(self, sql) -> tuple:
        return sql_fetchall_description(self.connection, sql)

    def create_scan(self, *args, **kwargs):
        return self.dialect.create_scan(self, *args, **kwargs)

    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logging.debug(f'Closing connection failed: {str(e)}')

