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
import json
import logging
import os
import random
import string
from typing import List, Optional
from unittest import TestCase

import yaml

from sodasql.common.logging_helper import LoggingHelper
from sodasql.scan.db import sql_update, sql_updates
from sodasql.scan.dialect import Dialect
from sodasql.scan.dialect_parser import DialectParser
from sodasql.scan.env_vars import EnvVars
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.scan_yml_parser import KEY_TABLE_NAME, ScanYmlParser
from sodasql.scan.sql_metric_yml_parser import SqlMetricYmlParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml
from tests.common.warehouse_fixture import WarehouseFixture

LoggingHelper.configure_for_test()

TARGET_POSTGRES = 'postgres'
TARGET_SNOWFLAKE = 'snowflake'
TARGET_REDSHIFT = 'redshift'
TARGET_ATHENA = 'athena'
TARGET_BIGQUERY = 'bigquery'


def equals_ignore_case(left, right):
    if not isinstance(left, str):
        return False
    if not isinstance(right, str):
        return False
    return left.lower() == right.lower()


class SqlTestCase(TestCase):

    warehouse_cache_by_target = {}
    warehouse_fixture_cache_by_target = {}
    warehouses_close_enabled = True

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.warehouse: Optional[Warehouse] = None
        # self.target controls the warehouse on which the test is executed
        # It is initialized in method setup_get_warehouse
        self.target: Optional[str] = None

        EnvVars.load_env_vars('test')

    def setUp(self) -> None:
        logging.debug(f'\n\n--- {str(self)} ---')
        super().setUp()
        self.warehouse = self.setup_get_warehouse()
        self.test_table_name = self.generate_test_table_name()

    def setup_get_warehouse(self):
        """self.target may be initialized by a test suite"""
        if self.target is None:
            self.target = os.getenv('SODA_TEST_TARGET', TARGET_POSTGRES)

        warehouse = SqlTestCase.warehouse_cache_by_target.get(self.target)
        if warehouse is None:
            logging.debug(f'Creating warehouse {self.target}')
            warehouse_fixture = WarehouseFixture.create(self.target)
            dialect = self.create_dialect(self.target)

            warehouse_configuration = WarehouseYml(dialect=dialect)
            warehouse = Warehouse(warehouse_configuration)
            warehouse_fixture.warehouse = warehouse
            warehouse_fixture.create_database()
            SqlTestCase.warehouse_cache_by_target[self.target] = warehouse
            SqlTestCase.warehouse_fixture_cache_by_target[self.target] = warehouse_fixture

        return warehouse

    @classmethod
    def create_dialect(cls, target: str) -> Dialect:
        tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        test_warehouse_cfg_path = f'{tests_dir}/warehouses/{target}_cfg.yml'
        with open(test_warehouse_cfg_path) as f:
            warehouse_configuration_dict = yaml.load(f, Loader=yaml.FullLoader)
            dialect_parser = DialectParser(warehouse_configuration_dict)
            dialect_parser.assert_no_warnings_or_errors()
            return dialect_parser.dialect

    def tearDown(self) -> None:
        if self.warehouse.connection:
            warehouse_fixture: WarehouseFixture = SqlTestCase.warehouse_fixture_cache_by_target[self.target]
            warehouse_fixture.tear_down()

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.warehouses_close_enabled:
            cls.teardown_close_warehouses()

    @classmethod
    def teardown_close_warehouses(cls):
        for target in SqlTestCase.warehouse_cache_by_target:
            warehouse_fixture: WarehouseFixture = SqlTestCase.warehouse_fixture_cache_by_target[target]
            warehouse_fixture.drop_database()
        SqlTestCase.warehouse_cache_by_target = {}

    def sql_update(self, sql: str) -> int:
        return sql_update(self.warehouse.connection, sql)

    def sql_updates(self, sqls: List[str]):
        return sql_updates(self.warehouse.connection, sqls)

    def sql_create_table(self, table_name: str, columns: List[str], rows: List[str]):
        joined_columns = ", ".join(columns)
        joined_rows = ", ".join(rows)
        table_name = self.warehouse.dialect.qualify_table_name(table_name)
        self.sql_updates([
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} ( {joined_columns} )",
            f"INSERT INTO {table_name} VALUES {joined_rows}"])

    def scan(self,
             scan_configuration_dict: Optional[dict] = None,
             sql_metric_dicts: Optional[List[dict]] = None,
             variables: Optional[dict] = None) -> ScanResult:
        if not scan_configuration_dict:
            scan_configuration_dict = {
                KEY_TABLE_NAME: self.test_table_name
            }
        logging.debug('Scan configuration \n'+json.dumps(scan_configuration_dict, indent=2))
        scan_configuration_parser = ScanYmlParser(scan_configuration_dict, 'Test scan')
        scan_configuration_parser.assert_no_warnings_or_errors()

        sql_metrics = []
        if sql_metric_dicts:
            for i in range(len(sql_metric_dicts)):
                sql_metric_parser = SqlMetricYmlParser(sql_metric_dicts[i], f'sql-metric-{i}')
                sql_metric_parser.assert_no_warnings_or_errors()
                sql_metrics.append(sql_metric_parser.sql_metric)

        scan = self.warehouse.create_scan(scan_yml=scan_configuration_parser.scan_yml,
                                          sql_metrics=sql_metrics,
                                          variables=variables)
        return scan.execute()

    def assertMeasurements(self, scan_result, column: str, expected_metrics_present):
        metrics_present = [measurement.metric for measurement in scan_result.measurements
                           if equals_ignore_case(measurement.column_name, column)]
        self.assertEqual(set(metrics_present), set(expected_metrics_present))

    def assertMeasurementsPresent(self, scan_result, column: str, expected_metrics_present):
        metrics_present = [measurement.metric for measurement in scan_result.measurements
                           if equals_ignore_case(measurement.column_name, column)]
        metrics_expected_and_not_present = [expected_metric for expected_metric in expected_metrics_present
                                            if expected_metric not in metrics_present]
        self.assertEqual(set(), set(metrics_expected_and_not_present))

    def assertMeasurementsAbsent(self, scan_result, column: str, expected_metrics_absent: list):
        metrics_present = [measurement.metric for measurement in scan_result.measurements
                           if equals_ignore_case(measurement.column_name, column)]
        metrics_present_and_expected_absent = set(expected_metrics_absent) & set(metrics_present)
        self.assertEqual(set(), metrics_present_and_expected_absent)

    @staticmethod
    def generate_test_table_name():
        """
        We need to generate a different table name for each test, otherwise we exceed the daily rate limits for table
        operation on BigQuery.
        """
        return 'test_table_' + ''.join([random.choice(string.ascii_lowercase) for _ in range(5)])
