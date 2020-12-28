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

from sodasql.cli.profile_reader import Profile
from sodasql.scan.db import sql_update, sql_updates
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.warehouse import Warehouse
from tests.common.env_vars_helper import EnvVarsHelper
from tests.common.logging_helper import LoggingHelper
from tests.common.warehouse_fixture import WarehouseFixture

LoggingHelper.configure_for_test()
EnvVarsHelper.load_test_environment_properties()


TARGET_POSTGRES = 'postgres'
TARGET_SNOWFLAKE = 'snowflake'
TARGET_REDSHIFT = 'redshift'
TARGET_ATHENA = 'athena'
TARGET_BIGQUERY = 'bigquery'


class SqlTestCase(TestCase):

    warehouse_cache_by_target = {}
    warehouse_fixture_cache_by_target = {}
    warehouses_close_enabled = True
    default_test_table_name = 'test_table'

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.warehouse: Optional[Warehouse] = None
        self.target: Optional[str] = None

    def setUp(self) -> None:
        logging.debug(f'\n\n--- {str(self)} ---')
        super().setUp()
        self.warehouse = self.setup_get_warehouse()

    def setup_get_warehouse(self):
        """self.target may be initialized by a test suite"""
        if self.target is None:
            self.target = os.getenv('SODA_TEST_TARGET', TARGET_POSTGRES)

        warehouse = SqlTestCase.warehouse_cache_by_target.get(self.target)
        if warehouse is None:
            logging.debug(f'Creating warehouse {self.target}')
            warehouse_configuration = self.setup_get_warehouse_configuration(self.target)
            warehouse_fixture = WarehouseFixture.create(self.target)
            warehouse_fixture.initialize_warehouse_configuration(warehouse_configuration)
            warehouse = self.setup_create_warehouse(warehouse_configuration)
            warehouse_fixture.initialize_warehouse(warehouse)
            SqlTestCase.warehouse_cache_by_target[self.target] = warehouse
            SqlTestCase.warehouse_fixture_cache_by_target[self.target] = warehouse_fixture

        return warehouse

    def setup_get_warehouse_configuration(self, target: str):
        if target == TARGET_POSTGRES:
            return {
                'name': 'test_postgres_warehouse',
                'type': 'postgres',
                'host': 'localhost',
                'port': '5432',
                'username': 'sodasql',
                'database': 'sodasql',
                'schema': 'public'}

        profile = Profile('test', target)
        if profile.parse_logs.has_warnings_or_errors() \
                and 'No such file or directory' in profile.parse_logs.logs[0].message:
            logging.error(f'{Profile.USER_HOME_PROFILES_YAML_LOCATION} not found, creating default initial version...')
            initial_profile = {
                'test': {
                    'target': 'redshift',
                    'outputs': {
                        'redshift': {
                            'type': 'redshift',
                            'host': '***',
                            'port': '5439',
                            'username': '***',
                            'database': '***',
                            'schema': 'public'},
                        'athena': {
                            'type': 'athena',
                            'database': '***',
                            'access_key_id': '***',
                            'secret_access_key': '***',
                            # role_arn: ***
                            # region: eu-west-1
                            'work_dir': '***'},
                        'bigquery': {
                            'type': 'bigquery',
                            'account_info': '***',
                            'dataset': '***'},
                        'snowflake': {
                            'type': 'snowflake',
                            'username': '***',
                            'password': '***',
                            'account': '***',
                            'warehouse': 'DEMO_WH',
                            'database': '***',
                            'schema': 'PUBLIC'}
                    }}}
            with open(Profile.USER_HOME_PROFILES_YAML_LOCATION, 'w') as yaml_file:
                yaml.dump(initial_profile, yaml_file, default_flow_style=False)
            raise AssertionError(f'{Profile.USER_HOME_PROFILES_YAML_LOCATION} not found. '
                                 f'Default initial version was created. '
                                 f'Update credentials for profile test, '
                                 f'target {target} in that file and retry.')
        profile.parse_logs.assert_no_warnings_or_errors(Profile.USER_HOME_PROFILES_YAML_LOCATION)
        return profile.configuration

    def setup_create_warehouse(self, warehouse_configuration: dict) -> Warehouse:
        warehouse = Warehouse(warehouse_configuration)
        warehouse.parse_logs.assert_no_warnings_or_errors('Test warehouse')
        return warehouse

    def tearDown(self) -> None:
        logging.debug('Rolling back transaction on warehouse connection')
        self.warehouse.connection.rollback()

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.warehouses_close_enabled:
            cls.teardown_close_warehouses()

    @classmethod
    def teardown_close_warehouses(cls):
        for target in SqlTestCase.warehouse_cache_by_target:
            warehouse_fixture: WarehouseFixture = cls.warehouse_fixture_cache_by_target[target]
            warehouse_fixture.cleanup()

    def sql_update(self, sql: str) -> int:
        return sql_update(self.warehouse.connection, sql)

    def sql_updates(self, sqls: List[str]):
        return sql_updates(self.warehouse.connection, sqls)

    def sql_create_table(self, table_name: str, columns: List[str], rows: List[str]):
        joined_columns = ", ".join(columns)
        joined_rows = ", ".join(rows)
        self.sql_updates([
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} ( {joined_columns} )",
            f"INSERT INTO {table_name} VALUES {joined_rows}"])

    def scan(self, scan_configuration_dict: dict) -> ScanResult:
        logging.debug('Scan configuration \n'+json.dumps(scan_configuration_dict, indent=2))
        scan_configuration: ScanConfiguration = ScanConfiguration(scan_configuration_dict)
        scan_configuration.parse_logs.assert_no_warnings_or_errors('Test scan')
        scan = self.warehouse.create_scan(scan_configuration)
        return scan.execute()

    def assertMeasurements(self, scan_result, column: str, expected_metrics_present):
        metrics_present = [measurement.metric for measurement in scan_result.measurements
                           if measurement.column_name == column]
        self.assertEqual(set(metrics_present), set(expected_metrics_present))

    def assertMeasurementsPresent(self, scan_result, column: str, expected_metrics_present):
        metrics_present = [measurement.metric for measurement in scan_result.measurements
                           if measurement.column_name == column]
        metrics_expected_and_not_present = [expected_metric for expected_metric in expected_metrics_present
                                            if expected_metric not in metrics_present]
        self.assertEqual(set(), set(metrics_expected_and_not_present))

    def assertMeasurementsAbsent(self, scan_result, column: str, expected_metrics_absent: list):
        metrics_present = [measurement.metric for measurement in scan_result.measurements
                           if measurement.column_name == column]
        metrics_present_and_expected_absent = set(expected_metrics_absent) & set(metrics_present)
        self.assertEqual(set(), metrics_present_and_expected_absent)
