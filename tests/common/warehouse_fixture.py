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
import os
import random
import re
import socket
import string
from typing import Optional, List

import yaml

from sodasql.scan.db import sql_update, sql_updates
from sodasql.scan.dialect import Dialect
from sodasql.scan.dialect_parser import DialectParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml


class WarehouseFixture:

    @classmethod
    def create(cls, target: str):
        from tests.common.sql_test_case import TARGET_SNOWFLAKE, TARGET_POSTGRES, TARGET_REDSHIFT, TARGET_ATHENA, \
            TARGET_BIGQUERY, TARGET_HIVE, TARGET_MYSQL, TARGET_SPARK
        if target == TARGET_POSTGRES:
            from tests.warehouses.postgres_fixture import PostgresFixture
            return PostgresFixture(target)
        elif target == TARGET_SNOWFLAKE:
            from tests.warehouses.snowflake_fixture import SnowflakeFixture
            return SnowflakeFixture(target)
        elif target == TARGET_REDSHIFT:
            from tests.warehouses.redshift_fixture import RedshiftFixture
            return RedshiftFixture(target)
        elif target == TARGET_ATHENA:
            from tests.warehouses.athena_fixture import AthenaFixture
            return AthenaFixture(target)
        elif target == TARGET_BIGQUERY:
            from tests.warehouses.bigquery_fixture import BigQueryFixture
            return BigQueryFixture(target)
        elif target == TARGET_HIVE:
            from tests.warehouses.hive_fixture import HiveFixture
            return HiveFixture(target)
        elif target == TARGET_MYSQL:
            from tests.warehouses.mysql_fixture import MySQLFixture
            return MySQLFixture(target)
        elif target == TARGET_SPARK:
            from tests.warehouses.spark_fixture import SparkFixture
            return SparkFixture(target)
        raise RuntimeError(f'Invalid target {target}')

    def __init__(self, target: str) -> None:
        super().__init__()
        self.target: str = target
        self.dialect = self.create_dialect(self.target)
        self.warehouse_yml = WarehouseYml(dialect=self.dialect, name='test_warehouse')
        self.warehouse: Optional[Warehouse] = Warehouse(self.warehouse_yml)
        self.database: Optional[str] = None
        self.create_database()

    def create_dialect(cls, target: str) -> Dialect:
        tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        test_warehouse_cfg_path = f'{tests_dir}/warehouses/{target}_cfg.yml'
        with open(test_warehouse_cfg_path) as f:
            warehouse_configuration_dict = yaml.load(f, Loader=yaml.SafeLoader)
            dialect_parser = DialectParser(warehouse_configuration_dict)
            dialect_parser.assert_no_warnings_or_errors()
            return dialect_parser.dialect

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
