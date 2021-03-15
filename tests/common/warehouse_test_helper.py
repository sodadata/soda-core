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
import random
import re
import socket
import string
from typing import Optional, List

from sodasql.scan.db import sql_update, sql_updates
from sodasql.scan.warehouse import Warehouse


class WarehouseTestHelper:

    @classmethod
    def create(cls, target: str):
        from tests.common.sql_test_case import TARGET_SNOWFLAKE, TARGET_POSTGRES, TARGET_REDSHIFT, TARGET_ATHENA, \
            TARGET_BIGQUERY
        if target == TARGET_POSTGRES:
            from tests.warehouses.postgres_fixture import PostgresFixture
            return PostgresFixture(target)
        # elif target == TARGET_SNOWFLAKE:
        #     from tests.warehouses.snowflake_fixture import SnowflakeFixture
        #     return SnowflakeFixture(target)
        # elif target == TARGET_REDSHIFT:
        #     from tests.warehouses.redshift_fixture import RedshiftFixture
        #     return RedshiftFixture(target)
        # elif target == TARGET_ATHENA:
        #     from tests.warehouses.athena_fixture import AthenaFixture
        #     return AthenaFixture(target)
        # elif target == TARGET_BIGQUERY:
        #     from tests.warehouses.bigquery_fixture import BigQueryFixture
        #     return BigQueryFixture(target)
        raise RuntimeError(f'Invalid target {target}')

    def __init__(self, target: str) -> None:
        super().__init__()
        self.target: str = target
        self.connection = None
        self.warehouse: Optional[Warehouse] = None
        self.database: Optional[str] = None

    def create_database(self):
        self.database = self.create_unique_database_name()
        self.warehouse.dialect.database = self.database
        sql_updates(self.warehouse.connection, [
            f'CREATE DATABASE IF NOT EXISTS {self.database}',
            f'USE DATABASE {self.database}'])
        self.warehouse.connection.commit()

    def drop_database(self):
        sql_update(self.warehouse.connection,
                   f'DROP DATABASE IF EXISTS {self.database} CASCADE')
        self.warehouse.connection.commit()

    def sql_create_table(self, columns: List[str], table_name: str):
        columns_sql = ", ".join(columns)
        return f"CREATE TABLE " \
               f"{self.warehouse.dialect.qualify_writable_table_name(table_name)} ( \n " \
               f"{columns_sql} );"

    @classmethod
    def create_unique_database_name(cls):
        prefix: str = 'soda_test'
        normalized_hostname = re.sub(r"(?i)[^a-zA-Z0-9]", "_", socket.gethostname()).lower()
        random_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
        return f"{prefix}_{normalized_hostname}_{random_suffix}"

    def tear_down(self):
        logging.debug('Rolling back transaction on warehouse connection')
        self.warehouse.connection.rollback()
