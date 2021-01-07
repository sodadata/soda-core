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
from pathlib import Path
from typing import Optional

import yaml

from tests.common.logging_helper import LoggingHelper


LoggingHelper.configure_for_cli()


class IndentingDumper(yaml.Dumper):
    """
    yaml.dump hack to get indentation.
    see also https://stackoverflow.com/questions/25108581/python-yaml-dump-bad-indentation
    """
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentingDumper, self).increase_indent(flow, False)


class CliImpl:

    INSTANCE = None # initialized after the class

    @classmethod
    def _log_version(cls):
        cls.log('Soda CLI version 2.0.0 beta')

    def create(self,
               warehouse_dir: str,
               warehouse_type: str,
               warehouse_name: Optional[str],
               database: Optional[str],
               username: Optional[str],
               password: Optional[str]):
        try:
            """
            Creates a project directory and ensures a profile is present
            """
            self._log_version()

            expanded_warehouse_dir = os.path.expanduser(warehouse_dir)
            if not warehouse_name:
                warehouse_dir_parent, warehouse_dir_name = os.path.split(expanded_warehouse_dir)
                warehouse_name = warehouse_name if warehouse_name else warehouse_dir_name

            from sodasql.scan.dialect import Dialect, ALL_WAREHOUSE_TYPES
            dialect = Dialect.create_for_warehouse_type(warehouse_type)

            if not dialect:
                self.log(f"Invalid warehouse type {warehouse_type}, use one of {str(ALL_WAREHOUSE_TYPES)}")
                return 1

            warehouse_path = Path(expanded_warehouse_dir)
            if warehouse_path.exists():
                self.log(f"Warehouse directory {warehouse_dir} already exists")
            else:
                self.log(f"Creating warehouse directory {warehouse_dir} ...")
                warehouse_path.mkdir(parents=True, exist_ok=True)

            if not warehouse_path.is_dir():
                self.log(f"Warehouse path {warehouse_dir} is not a directory")
                return 1

            configuration_params = {}
            if isinstance(database, str):
                configuration_params['database'] = database
            if isinstance(username, str):
                configuration_params['username'] = username
            if isinstance(password, str):
                configuration_params['password'] = password
            connection_properties = dialect.default_connection_properties(configuration_params)
            warehouse_env_vars = dialect.default_env_vars(configuration_params)

            warehouse_yml_file = os.path.join(expanded_warehouse_dir, 'warehouse.yml')
            warehouse_yml_file_log = os.path.join(warehouse_dir, 'warehouse.yml')
            warehouse_yml_path = Path(warehouse_yml_file)
            if warehouse_yml_path.exists():
                self.log(f"Warehouse configuration file {warehouse_yml_file_log} already exists")
            else:
                self.log(f"Creating warehouse configuration file {warehouse_yml_file_log} ...")
                with open(warehouse_yml_file, 'w+') as f:
                    warehouse_dict = {
                        'name': warehouse_name,
                        'connection': connection_properties
                    }
                    yaml.dump(warehouse_dict, f, default_flow_style=False, sort_keys=False)
                os.chmod(warehouse_yml_file, 0o777)

            dot_soda_dir = os.path.join(Path.home(), '.soda')
            dot_soda_path = Path(dot_soda_dir)
            if not dot_soda_path.exists():
                dot_soda_path.mkdir(parents=True, exist_ok=True)

            env_vars_file = os.path.join(dot_soda_dir, 'env_vars.yml')
            env_vars_path = Path(env_vars_file)
            env_vars_exists = env_vars_path.exists()
            if env_vars_exists:
                try:
                    with open(env_vars_path) as f:
                        existing_env_vars_yml_dict = yaml.load(f, Loader=yaml.FullLoader)
                        if isinstance(existing_env_vars_yml_dict, dict) and warehouse_name in existing_env_vars_yml_dict:
                            self.log(f"Warehouse section {warehouse_name} already exists in {env_vars_path}.  Skipping...")
                            warehouse_env_vars = None
                except Exception as e:
                    self.log(f"Couldn't read {env_vars_file}: {str(e)}")
                    warehouse_env_vars = None

            if warehouse_env_vars:
                warehouse_env_vars_dict = {
                    warehouse_name: warehouse_env_vars
                }

                env_vars_mode = 'a' if env_vars_exists else 'w+'
                with open(env_vars_path, env_vars_mode) as f:
                    if env_vars_exists:
                        self.log(f"Adding env vars for {warehouse_name} to {env_vars_path}")
                        f.write('\n')
                    else:
                        self.log(f"Creating {env_vars_path} with example env vars in section {warehouse_name}")
                    yaml.dump(warehouse_env_vars_dict, f, default_flow_style=False, sort_keys=False)
                if not env_vars_exists:
                    os.chmod(env_vars_file, 0o777)

            self.log(f"Review warehouse.yml by running command")
            self.log(f"  open {warehouse_yml_file}")
            self.log(f"Review section {warehouse_name} in ~/.soda/env_vars.yml by running command")
            self.log(f"  open ~/.soda/env_vars.yml")
            self.log(f"Then run")
            self.log(f"  soda init {warehouse_dir}")
        except Exception as e:
            self.exception(f'Exception: {str(e)}')
            return 1

    def init(self, warehouse_dir: str):
        """
        Finds tables in the warehouse and based on the contents, creates initial scan.yml files.
        """
        try:
            self._log_version()

            expanded_warehouse_dir = os.path.expanduser(warehouse_dir)

            self.log(f'Initializing {warehouse_dir} ...')

            from sodasql.scan.warehouse_configuration_parser import WarehouseConfigurationParser
            soda_project_parser = WarehouseConfigurationParser(warehouse_dir=expanded_warehouse_dir)
            soda_project_parser.log()
            soda_project_parser.assert_no_warnings_or_errors()

            from sodasql.scan.warehouse_configuration import WarehouseConfiguration
            soda_project: WarehouseConfiguration = soda_project_parser.warehouse_configuration

            from sodasql.scan.warehouse import Warehouse
            warehouse: Warehouse = Warehouse(soda_project.dialect)

            self.log('Querying warehouse for tables')
            rows = warehouse.sql_fetchall(soda_project.dialect.sql_tables_metadata_query())
            first_table_name = rows[0][0] if len(rows) > 0 else None
            for row in rows:
                table_name = row[0]
                table_dir = os.path.join(expanded_warehouse_dir, table_name)
                table_dir_log = os.path.join(warehouse_dir, table_name)
                table_dir_path = Path(table_dir)
                if not table_dir_path.exists():
                    self.log(f'Creating table directory {table_dir_log}')
                    table_dir_path.mkdir(parents=True, exist_ok=True)
                else:
                    self.log(f'Directory {table_dir_log} aleady exists')

                table_scan_yaml_file = os.path.join(table_dir, 'scan.yml')
                table_scan_yaml_file_log = os.path.join(table_dir_log, 'scan.yml')
                table_scan_yaml_path = Path(table_scan_yaml_file)

                if table_scan_yaml_path.exists():
                    self.log(f"Scan file {table_scan_yaml_file_log} already exists")
                else:
                    self.log(f"Creating {table_scan_yaml_file_log} ...")
                    with open(table_scan_yaml_file, 'w+') as f:
                        scan_yaml_dict = {
                            'table_name': table_name,
                            'metrics': [
                                'row_count',
                                'missing_count', 'missing_percentage', 'values_count', 'values_percentage',
                                'valid_count', 'valid_percentage', 'invalid_count', 'invalid_percentage',
                                'min', 'max', 'avg', 'sum', 'min_length', 'max_length', 'avg_length'
                            ]
                        }
                        yaml.dump(scan_yaml_dict, f, sort_keys=False, Dumper=IndentingDumper, default_flow_style=False)
                    os.chmod(table_scan_yaml_file, 0o777)

            self.log(f"Next run 'soda scan {warehouse_dir} {first_table_name}' to calculate measurements and run tests")

        except Exception as e:
            self.exception(f'Exception: {str(e)}')
            return 1
        finally:
            if warehouse:
                warehouse.close()

    def scan(self,
             soda_project_dir: str,
             table: str,
             timeslice: Optional[str] = None,
             timeslice_variables: Optional[str] = None,
             target: Optional[str] = None) -> int:
        """
        Scans a table by executing queries, computes measurements and runs tests
        """

        try:
            soda_project_dir = os.path.expanduser(soda_project_dir)

            self._log_version()
            self.log(f'Scanning {table} in {soda_project_dir} ...')

            from sodasql.scan.warehouse_configuration_parser import WarehouseConfigurationParser
            soda_project_parser = WarehouseConfigurationParser(warehouse_dir=soda_project_dir)
            soda_project_parser.log()
            soda_project_parser.assert_no_warnings_or_errors()

            from sodasql.scan.warehouse_configuration import WarehouseConfiguration
            soda_project: WarehouseConfiguration = soda_project_parser.warehouse_configuration

            from sodasql.scan.scan_configuration_parser import ScanConfigurationParser
            scan_configuration_parser = ScanConfigurationParser(soda_project_dir=soda_project_dir, table_dir_name=table)
            scan_configuration_parser.log()
            scan_configuration_parser.assert_no_warnings_or_errors()

            scan_configuration = scan_configuration_parser.scan_configuration

            from sodasql.scan.warehouse import Warehouse
            warehouse: Warehouse = Warehouse(soda_project.dialect)

            from sodasql.scan.scan import Scan
            from sodasql.scan.scan_result import ScanResult
            scan: Scan = Scan(warehouse=warehouse,
                              scan_configuration=scan_configuration,
                              soda_client=None)

            scan_result: ScanResult = scan.execute()

            for measurement in scan_result.measurements:
                self.log(measurement)
            for test_result in scan_result.test_results:
                self.log(test_result)

            self.log(f'{len(scan_result.measurements)} measurements computed')
            self.log(f'{len(scan_result.test_results)} tests executed')
            if scan_result.has_failures():
                self.log(f'{scan_result.failures_count()} tests failed!')
                return scan_result.failures_count()
            else:
                self.log(f'All is good. No tests failed.')
                return 0

        except Exception as e:
            self.exception(f'Scan failed: {str(e)}')
            return 1
        finally:
            if warehouse:
                warehouse.close()

    @classmethod
    def log(cls, message: str):
        logging.info(message)

    @classmethod
    def exception(cls, message: str):
        logging.exception(message)


CliImpl.INSTANCE = CliImpl()
