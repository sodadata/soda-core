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


class CLI:

    @classmethod
    def _log_version(cls):
        cls.log('Soda CLI version 2.0.0 beta')

    def create(self,
               soda_project_dir: str,
               warehouse_type: str):
        try:
            """
                    Creates a project directory and ensures a profile is present
                    """
            self._log_version()

            expanded_soda_project_dir = os.path.expanduser(soda_project_dir)
            project_dir_parent, project_dir_name = os.path.split(expanded_soda_project_dir)

            from sodasql.scan.dialect import Dialect, ALL_WAREHOUSE_TYPES
            dialect = Dialect.create_for_warehouse_type(warehouse_type)

            if not dialect:
                self.log(f"Invalid warehouse type {warehouse_type}, use one of {str(ALL_WAREHOUSE_TYPES)}")
                return 1

            project_dir_path = Path(expanded_soda_project_dir)
            if project_dir_path.exists():
                self.log(f"Soda project dir {soda_project_dir} already exists")
            else:
                self.log(f"Creating project dir {soda_project_dir} ...")
                project_dir_path.mkdir(parents=True, exist_ok=True)

            if not project_dir_path.is_dir():
                self.log(f"Project dir {soda_project_dir} is not a directory")
                return 1

            project_name = f'{project_dir_name}_{warehouse_type}'

            soda_project_file = os.path.join(expanded_soda_project_dir, 'soda.yml')
            warehouse_configuration = {}
            project_env_vars = {}
            dialect.default_configuration(warehouse_configuration, project_env_vars)

            project_file_path = Path(soda_project_file)
            if project_file_path.exists():
                self.log(f"Project file {soda_project_file} already exists")
            else:
                self.log(f"Creating project file {soda_project_file} ...")
                with open(soda_project_file, 'w+') as f:
                    soda_project_dict = {
                        'name': project_name,
                        'warehouse': warehouse_configuration
                    }
                    yaml.dump(soda_project_dict, f, default_flow_style=False, sort_keys=False)
                os.chmod(soda_project_file, 0o777)

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
                        if isinstance(existing_env_vars_yml_dict, dict) and project_name in existing_env_vars_yml_dict:
                            self.log(f"Project {project_name} already exists in {env_vars_path}.  Skipping...")
                            project_env_vars = None
                except Exception as e:
                    self.log(f"Couldn't read  {env_vars_path}: {str(e)}")
                    project_env_vars = None

            if project_env_vars:
                project_env_vars_dict = {
                    project_name: project_env_vars
                }

                env_vars_mode = 'a' if env_vars_exists else 'w+'
                with open(env_vars_path, env_vars_mode) as f:
                    if env_vars_exists:
                        self.log(f"Adding env vars for {project_name} to {env_vars_path}")
                        f.write('\n')
                    else:
                        self.log(f"Creating {env_vars_path} with example env vars")
                    yaml.dump(project_env_vars_dict, f, default_flow_style=False, sort_keys=False)
                if not env_vars_exists:
                    os.chmod(env_vars_file, 0o777)

                self.log(f"Please review and update the '{project_name}' environment variables in ~/.soda/env_vars.yml")
                self.log(f"Then run 'soda init {soda_project_dir}'")
        except Exception as e:
            self.exception(f'Exception: {str(e)}')
            return 1

    def init(self, soda_project_dir: str):
        """
        Analyses the warehouse tables and creates scan.yml files in your project dir
        """
        try:
            self._log_version()

            expanded_soda_project_dir = os.path.expanduser(soda_project_dir)

            self.log(f'Initializing {expanded_soda_project_dir} ...')

            from sodasql.scan.soda_project_parser import SodaProjectParser
            soda_project_parser = SodaProjectParser(soda_project_dir=expanded_soda_project_dir)
            soda_project_parser.log()
            soda_project_parser.assert_no_warnings_or_errors()

            from sodasql.scan.soda_project import SodaProject
            soda_project: SodaProject = soda_project_parser.soda_project

            from sodasql.scan.warehouse import Warehouse
            warehouse: Warehouse = Warehouse(soda_project.dialect)

            self.log('Querying warehouse for tables')
            rows = warehouse.sql_fetchall(soda_project.dialect.sql_tables_metadata_query())
            if len(rows) > 0:
                first_table_name = rows[0][0]
            for row in rows:
                table_name = row[0]
                table_dir = os.path.join(expanded_soda_project_dir, table_name)
                table_dir_path = Path(table_dir)
                if not table_dir_path.exists():
                    self.log(f'Creating table directory {table_dir}')
                    table_dir_path.mkdir(parents=True, exist_ok=True)
                else:
                    self.log(f'Directory {table_dir_path} aleady exists')

                table_scan_yaml_file = os.path.join(table_dir_path, 'scan.yml')
                table_scan_yaml_path = Path(table_scan_yaml_file)

                if table_scan_yaml_path.exists():
                    self.log(f"Scan file {table_scan_yaml_file} already exists")
                else:
                    self.log(f"Creating {table_scan_yaml_file} ...")
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

            self.log(f"Next run 'soda scan {soda_project_dir} {first_table_name}' to calculate measurements and run tests")

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

            from sodasql.scan.soda_project_parser import SodaProjectParser
            soda_project_parser = SodaProjectParser(soda_project_dir=soda_project_dir)
            soda_project_parser.log()
            soda_project_parser.assert_no_warnings_or_errors()

            from sodasql.scan.soda_project import SodaProject
            soda_project: SodaProject = soda_project_parser.soda_project

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

            if scan_result.has_failures():
                self.log(f'Tests failed: {scan_result.failures_count()}')
                return scan_result.failures_count()
            else:
                self.log(f'All good. {len(scan_result.measurements)} measurements computed. No tests failed.')
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


class CliImpl:
    """
    Enables to configure a different cli impl for testing purposes
    """
    cli = CLI()
