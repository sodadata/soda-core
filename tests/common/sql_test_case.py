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

from sodasql.profiles.profiles import Profile
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.scan_result import ScanResult
from tests.common.env_vars_helper import EnvVarsHelper
from tests.common.logging_helper import LoggingHelper
from sodasql.warehouse.warehouse import Warehouse


LoggingHelper.configure_for_test()


class SqlTestCase(TestCase):

    warehouse: Warehouse = None
    default_test_table_name = 'test_table'

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.warehouse: Optional[Warehouse] = None

    def setUp(self) -> None:
        logging.debug(f'\n\n--- {str(self)} ---')
        super().setUp()

        test_profile_target = self.setup_get_test_profile_target()
        self.warehouse_configuration = self.setup_get_warehouse_configuration('test', test_profile_target)
        if SqlTestCase.warehouse is not None \
                and SqlTestCase.warehouse.warehouse_configuration != self.warehouse_configuration:
            SqlTestCase.warehouse.close()
            SqlTestCase.warehouse = None
        if SqlTestCase.warehouse is None:
            SqlTestCase.warehouse = self.setup_create_warehouse(self.warehouse_configuration)
            self.setup_init_warehouse()
        self.warehouse = SqlTestCase.warehouse

    def setup_get_test_profile_target(self):
        EnvVarsHelper.load_test_environment_properties()
        return os.getenv('SODA_TEST_TARGET', 'local_postgres')

    def setup_get_warehouse_configuration(self, profile_name: str, profile_target_name: str):
        if profile_name == 'test' and profile_target_name == 'local_postgres':
            return {
                'name': 'test_postgres_warehouse',
                'type': 'postgres',
                'host': 'localhost',
                'port': '5432',
                'username': 'sodasql',
                'database': 'sodasql',
                'schema': 'public'}
        return self.setup_read_warehouse_configuration_from_profile_yaml(profile_name, profile_target_name)

    def setup_read_warehouse_configuration_from_profile_yaml(self, profile_name: str, profile_target_name: str):
        profile = Profile(profile_name, profile_target_name)
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
                                 f'Update credentials  for profile {profile_name}, '
                                 f'target {profile_target_name} in that file and retry.')
        profile.parse_logs.assert_no_warnings_or_errors(Profile.USER_HOME_PROFILES_YAML_LOCATION)
        if profile.configuration.get('name') is None:
            profile.configuration['name'] = f'{profile_name}_{profile_target_name}_warehouse'
        return profile.configuration

    def setup_create_warehouse(self, warehouse_configuration: dict) -> Warehouse:
        warehouse = Warehouse(warehouse_configuration)
        warehouse.parse_logs.assert_no_warnings_or_errors('Test warehouse')
        return warehouse

    def setup_init_warehouse(self):
        pass

    def tearDown(self) -> None:
        self.warehouse.connection.rollback()

    @classmethod
    def execute_sql_update(cls, sql: str):
        connection = SqlTestCase.warehouse.connection
        assert connection, 'connection not initialized'
        cursor = connection.cursor()
        try:
            logging.debug(f'Test SQL update: {sql}')
            result = cursor.execute(sql)
            connection.commit()
            return result
        finally:
            cursor.close()

    @classmethod
    def execute_sql_updates(cls, sqls: List[str]):
        for sql in sqls:
            cls.execute_sql_update(sql)

    def create_table(self, table_name: str, columns: List[str], rows: List[str]):
        joined_columns = ", ".join(columns)
        joined_rows = ", ".join(rows)
        self.execute_sql_updates([
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} ( {joined_columns} )",
            f"INSERT INTO {table_name} VALUES {joined_rows}"])

    def scan(self, scan_configuration_dict: dict) -> ScanResult:
        logging.debug('Scan configuration \n'+json.dumps(scan_configuration_dict, indent=2))

        scan_configuration: ScanConfiguration = ScanConfiguration(scan_configuration_dict)
        scan_configuration.parse_logs.assert_no_warnings_or_errors('Test scan')
        scan = self.warehouse.create_scan(scan_configuration)
        for log in scan.configuration.parse_logs.logs:
            logging.info(str(log))
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
