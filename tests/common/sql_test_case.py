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
from typing import List, Optional
from unittest import TestCase

import yaml

from sodasql.common.logging_helper import LoggingHelper
from sodasql.scan.db import sql_update, sql_updates
from sodasql.scan.dialect import Dialect
from sodasql.scan.dialect_parser import DialectParser
from sodasql.scan.env_vars import EnvVars
from sodasql.scan.metric import Metric
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.scan_yml_parser import KEY_TABLE_NAME, ScanYmlParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml
from tests.common.mock_soda_server_client import MockSodaServerClient
from tests.common.warehouse_fixture import WarehouseFixture

LoggingHelper.configure_for_test()

TARGET_POSTGRES = 'postgres'
TARGET_SNOWFLAKE = 'snowflake'
TARGET_REDSHIFT = 'redshift'
TARGET_ATHENA = 'athena'
TARGET_BIGQUERY = 'bigquery'
TARGET_SQLSERVER = 'sqlserver'
TARGET_HIVE = 'hive'


def equals_ignore_case(left, right):
    if not isinstance(left, str):
        return False
    if not isinstance(right, str):
        return False
    return left.lower() == right.lower()


class SqlTestCase(TestCase):
    warehouse_fixtures_by_target = {}
    warehouses_close_enabled = True

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.warehouse: Optional[Warehouse] = None
        self.dialect: Optional[Dialect] = None
        # self.target controls the warehouse on which the test is executed
        # It is initialized in method setup_get_warehouse
        self.target: Optional[str] = None
        # Tests must explicitly enable mock soda server client by calling self.use_mock_soda_server_client()
        self.mock_soda_server_client = None

        EnvVars.load_env_vars('test')

    def setUp(self) -> None:
        logging.debug(f'\n\n--- {str(self)} ---')
        super().setUp()
        self.warehouse_fixture: Optional[WarehouseFixture] = self.setup_get_warehouse_fixture()
        self.warehouse = self.warehouse_fixture.warehouse
        self.dialect = self.warehouse.dialect
        self.default_test_table_name = self.generate_test_table_name()

    def use_mock_soda_server_client(self):
        self.mock_soda_server_client = MockSodaServerClient()

    def setup_get_warehouse_fixture(self):
        """self.target may be initialized by a test suite"""
        if self.target is None:
            self.target = os.getenv('SODA_TEST_TARGET', TARGET_POSTGRES)

        warehouse_fixture = SqlTestCase.warehouse_fixtures_by_target.get(self.target)
        if warehouse_fixture is None:
            logging.debug(f'Creating warehouse {self.target}')

            warehouse_fixture = WarehouseFixture.create(self.target)
            SqlTestCase.warehouse_fixtures_by_target[self.target] = warehouse_fixture

        return warehouse_fixture

    @classmethod
    def create_dialect(cls, target: str) -> Dialect:
        tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        test_warehouse_cfg_path = f'{tests_dir}/warehouses/{target}_cfg.yml'
        with open(test_warehouse_cfg_path) as f:
            warehouse_configuration_dict = yaml.load(f, Loader=yaml.SafeLoader)
            dialect_parser = DialectParser(warehouse_configuration_dict)
            dialect_parser.assert_no_warnings_or_errors()
            return dialect_parser.dialect

    def tearDown(self) -> None:
        if self.warehouse.connection:
            self.warehouse_fixture.tear_down()

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.warehouses_close_enabled:
            cls.teardown_close_warehouses()

    @classmethod
    def teardown_close_warehouses(cls):
        for target in SqlTestCase.warehouse_fixtures_by_target:
            warehouse_fixture: WarehouseFixture = SqlTestCase.warehouse_fixtures_by_target[target]
            warehouse_fixture.drop_database()
        SqlTestCase.warehouse_fixtures_by_target = {}

    def sql_update(self, sql: str) -> int:
        return sql_update(self.warehouse.connection, sql)

    def sql_updates(self, sqls: List[str]):
        return sql_updates(self.warehouse.connection, sqls)

    def sql_recreate_table(self, columns: List[str], rows: List[str] = None, table_name: str = None):
        table_name = table_name if table_name else self.default_test_table_name
        self.sql_update(f"DROP TABLE IF EXISTS {self.warehouse.dialect.qualify_writable_table_name(table_name)}")
        self.sql_update(self.sql_create_table(columns, table_name))
        if rows:
            joined_rows = ", ".join(rows)
            self.sql_update(f"INSERT INTO {self.warehouse.dialect.qualify_table_name(table_name)} VALUES {joined_rows}")
        self.warehouse.connection.commit()

    def sql_create_table(self, columns: List[str], table_name: str):
        return self.warehouse_fixture.sql_create_table(columns=columns, table_name=table_name)

    def scan(self,
             scan_yml_dict: Optional[dict] = None,
             variables: Optional[dict] = None) -> ScanResult:
        if not scan_yml_dict:
            scan_yml_dict = {}
        if KEY_TABLE_NAME not in scan_yml_dict:
            scan_yml_dict[KEY_TABLE_NAME] = self.default_test_table_name
        logging.debug('Scan configuration \n' + json.dumps(scan_yml_dict, indent=2))
        scan_configuration_parser = ScanYmlParser(scan_yml_dict, 'test-scan')
        scan_configuration_parser.assert_no_warnings_or_errors()

        scan = self.warehouse.create_scan(scan_yml=scan_configuration_parser.scan_yml,
                                          variables=variables,
                                          soda_server_client=self.mock_soda_server_client)
        scan.close_warehouse = False
        return scan.execute()

    def execute_metric(self, warehouse: Warehouse, metric: dict, scan_dict: dict = None):
        dialect = warehouse.dialect
        if not scan_dict:
            scan_dict = {}
        if KEY_TABLE_NAME not in scan_dict:
            scan_dict[KEY_TABLE_NAME] = self.default_test_table_name
        scan_configuration_parser = ScanYmlParser(scan_dict, 'test-scan')
        scan_configuration_parser.assert_no_warnings_or_errors()
        scan = warehouse.create_scan(scan_yml=scan_configuration_parser.scan_yml)
        scan.close_warehouse = False
        scan.execute()

        fields: List[str] = []
        group_by_column_names: List[str] = metric.get('groupBy')
        if group_by_column_names:
            for group_by_column in group_by_column_names:
                fields.append(dialect.qualify_column_name(group_by_column))

        column_name: str = metric.get('columnName')
        qualified_column_name = dialect.qualify_column_name(column_name)

        metric_type = metric['type']
        if metric_type == Metric.ROW_COUNT:
            fields.append('COUNT(*)')
        if metric_type == Metric.MIN:
            fields.append(f'MIN({qualified_column_name})')
        elif metric_type == Metric.MAX:
            fields.append(f'MAX({qualified_column_name})')
        elif metric_type == Metric.SUM:
            fields.append(f'SUM({qualified_column_name})')

        sql = 'SELECT \n  ' + ',\n  '.join(fields) + ' \n' \
                                                     'FROM ' + scan.qualified_table_name

        where_clauses = []

        metric_filter = metric.get('filter')
        if metric_filter:
            where_clauses.append(dialect.sql_expression(metric_filter))

        scan_column: ScanColumn = scan.scan_columns.get(column_name)
        if scan_column and scan_column.non_missing_and_valid_condition:
            where_clauses.append(scan_column.non_missing_and_valid_condition)

        if where_clauses:
            sql += '\nWHERE ' + '\n      AND '.join(where_clauses)

        if group_by_column_names:
            sql += '\nGROUP BY ' + ', '.join(group_by_column_names)

        return warehouse.sql_fetchall(sql)

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

    def qualify_string(self, value: str):
        return self.warehouse.dialect.qualify_string(value)

    def generate_test_table_name(self):
        """
        Overridden in sql_test_suite
        """
        return 'test_table'

    def assertAllNumeric(self, values):
        self.assertTrue(
            all(isinstance(x, int) or isinstance(x, float) for x in values))
