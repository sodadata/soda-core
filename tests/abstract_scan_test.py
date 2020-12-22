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
from typing import List
from unittest import TestCase

from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.scan_result import ScanResult
from tests.logging_helper import LoggingHelper
from sodasql.warehouse.warehouse import Warehouse


LoggingHelper.configure_for_test()


class AbstractScanTest(TestCase):

    warehouse: Warehouse = None
    default_test_table_name = 'test_table'

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.connection = None

    def setUp(self) -> None:
        logging.debug(f'\n\n--- {str(self)} ---')
        super().setUp()
        warehouse_configuration = self.get_warehouse_configuration()
        if AbstractScanTest.warehouse is not None \
                and AbstractScanTest.warehouse.warehouse_configuration != warehouse_configuration:
            AbstractScanTest.warehouse.close()
            AbstractScanTest.warehouse = None
        if AbstractScanTest.warehouse is None:
            AbstractScanTest.warehouse = Warehouse(warehouse_configuration)
        self.warehouse = AbstractScanTest.warehouse
        self.connection = self.warehouse.connection

    def get_warehouse_configuration(self):
        return {
            'name': 'test-postgres-store',
            'type': 'postgres',
            'host': 'localhost',
            'port': '5432',
            'username': 'sodalite',
            'database': 'sodalite',
            'schema': 'public'}

    def tearDown(self) -> None:
        self.connection.rollback()

    def sql_update(self, sql: str):
        assert self.connection, 'self.connection not initialized'
        cursor = self.connection.cursor()
        try:
            logging.debug(f'Test SQL update: {sql}')
            return cursor.execute(sql)
        finally:
            cursor.close()

    def sql_updates(self, sqls: List[str]):
        for sql in sqls:
            self.sql_update(sql)

    def create_table(self, table_name: str, columns: List[str], rows: List[str]):
        joined_columns = ", ".join(columns)
        joined_rows = ", ".join(rows)
        self.sql_updates([
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} ( {joined_columns} )",
            f"INSERT INTO {table_name} VALUES {joined_rows}"])

    def scan(self, scan_configuration_dict: dict) -> ScanResult:
        logging.debug('Scan configuration \n'+json.dumps(scan_configuration_dict, indent=2))

        scan_configuration: ScanConfiguration = ScanConfiguration(scan_configuration_dict)
        scan = self.warehouse.create_scan(scan_configuration)
        if scan.configuration.parse_logs.has_warnings_or_errors():
            raise AssertionError(
                'Scan configuration errors: \n  ' +
                ('\n  '.join([str(log) for log in scan.configuration.parse_logs.logs])))
        for log in scan.configuration.parse_logs.logs:
            logging.info(str(log))
        return scan.execute()

    def assertMeasurements(self, scan_result, column: str, expected_metrics_present):
        metrics_present = [measurement.metric for measurement in scan_result.measurements if measurement.column_name == column]
        self.assertEqual(set(metrics_present), set(expected_metrics_present))

    def assertMeasurementsPresent(self, scan_result, column: str, expected_metrics_present):
        metrics_present = [measurement.metric for measurement in scan_result.measurements if measurement.column_name == column]
        metrics_expected_and_not_present = [expected_metric for expected_metric in expected_metrics_present if expected_metric not in metrics_present]
        self.assertEqual(set(), set(metrics_expected_and_not_present))

    def assertMeasurementsAbsent(self, scan_result, column: str, expected_metrics_absent: list):
        metrics_present = [measurement.metric for measurement in scan_result.measurements if measurement.column_name == column]
        metrics_present_and_expected_absent = set(expected_metrics_absent) & set(metrics_present)
        self.assertEqual(set(), metrics_present_and_expected_absent)
