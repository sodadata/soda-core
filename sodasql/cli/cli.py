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
import os
from pathlib import Path
from typing import Optional

import click
import yaml

from sodasql.scan.dialect import Dialect
from sodasql.scan.scan import Scan
from sodasql.scan.scan_parse import ScanParse
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_parse import WarehouseParse


class CLI:

    @classmethod
    def _log_version(cls):
        click.echo('Soda CLI version 2.0.0 beta')

    def create(self,
               project_dir: str,
               warehouse_type: str,
               profile: Optional[str] = 'default_profile',
               target: Optional[str] = 'default_target'):
        """
        Creates a project directory and ensures a profile is present
        """
        self._log_version()

        if not Dialect.is_valid_warehouse_type(warehouse_type):
            click.echo(f"Invalid warehouse type {warehouse_type}")
            return 1

        project_dir_path = Path(project_dir)
        if project_dir_path.exists():
            click.echo(f"Project dir {project_dir} already exists")
        else:
            click.echo(f"Creating project dir {project_dir} ...")
            project_dir_path.mkdir(parents=True, exist_ok=True)

        if not project_dir_path.is_dir():
            click.echo(f"Project dir {project_dir} is not a directory")
            return 1

        project_file = os.path.join(project_dir, 'soda_project.yaml')
        project_file_path = Path(project_file)
        if project_file_path.exists():
            click.echo(f"Project file {project_file} already exists")
        else:
            click.echo(f"Creating project file {project_file} ...")
            with open(project_file, 'w') as f:
                f.write(
                    f'name: {os.path.basename(project_dir)}\n'
                    f'soda_host: cloud.soda.io \n'
                    f'soda_api_key_secret: Create an account on cloud.soda.io and put your soda api key secret here \n')

        profile_dir = os.path.join(project_dir, profile)
        profile_dir_path = Path(profile_dir)
        if profile_dir_path.exists():
            click.echo(f"Project dir {profile_dir_path} already exists")
        else:
            click.echo(f"Creating profile dir {profile_dir_path} ...")
            profile_dir_path.mkdir(parents=True, exist_ok=True)

        dot_soda_dir = os.path.join(Path.home(), '.soda')
        dot_soda_path = Path(dot_soda_dir)
        if not dot_soda_path.exists():
            dot_soda_path.mkdir(parents=True, exist_ok=True)

        profiles_path = os.path.join(dot_soda_dir, 'profiles.yml')
        profiles_path = Path(profiles_path)
        profiles_exists = profiles_path.exists()
        if profiles_exists:
            with open(profiles_path) as f:
                profiles = f.read()
                profiles_dict = yaml.load(profiles, Loader=yaml.FullLoader)
                if profile in profiles_dict:
                    click.echo(f"Profile {profile} already exists in {profile_dir_path}.  Skipping...")
                    return 1

        profile_yaml_dict = {
            profile: {
               'outputs': {
                   'target': target,
                   target:
                       Dialect.create_default_configuration_dict(warehouse_type) }}}

        profiles_mode = 'a' if profiles_exists else 'w'
        with open(profiles_path, profiles_mode) as f:
            if profiles_exists:
                click.echo(f"Adding profile {profile} to existing {profiles_path}")
                f.write('\n')
            else:
                click.echo(f"Creating {profiles_path} with initial profile {profile}")
            yaml.dump(profile_yaml_dict, f)

    def init(self,
             project_dir: str,
             profile: Optional[str] = 'default',
             target: Optional[str] = None):
        """
        Analyses the warehouse tables and creates scan.yml files in your project dir
        """
        self._log_version()
        click.echo(f'Initializing {project_dir} on {profile} {target} ...')

        warehouse: Warehouse = self._read_warehouse(profile, target)

    def scan(self,
             project_dir: str,
             table: str,
             profile: Optional[str] = 'default',
             target: Optional[str] = None) -> int:
        """
        Scans a table by executing queries, computes measurements and runs tests
        """

        self._log_version()
        click.echo(f'Scanning {table} with {project_dir} on {profile}{f" {target}" if target else ""} ...')

        warehouse_parse = self._parse_warehouse(profile, target)
        warehouse_parse.parse_logs.log()
        scan_parse = self._parse_scan(project_dir, profile, table)
        scan_parse.parse_logs.log()

        if warehouse_parse.parse_logs.has_warnings_or_errors() \
                or scan_parse.parse_logs.has_warnings_or_errors():
            return 1

        warehouse: Warehouse = Warehouse(warehouse_parse.warehouse_configuration)
        try:
            scan: Scan = Scan(warehouse, scan_parse.scan_configuration)
            scan_result: ScanResult = scan.execute()
            for measurement in scan_result.measurements:
                click.echo(measurement)
            for test_result in scan_result.test_results:
                click.echo(test_result)
        finally:
            warehouse.close()

        return 1 if scan_result.has_failures() else 0

    def verify(self,
               project_dir: str,
               table: str,
               profile: Optional[str] = 'default',
               target: Optional[str] = None):
        """
        Dry run to verify if the configuration is ok. No connection is made to the warehouse.
        """
        self._log_version()
        warehouse: Warehouse = self._read_warehouse(profile, target)
        scan = self._read_scan(project_dir, table, profile, target)

    def _parse_warehouse(self, profile, target):
        warehouse_parse = WarehouseParse(profile, target)
        warehouse_parse.parse_logs.log()
        return warehouse_parse

    def _parse_scan(self, project_dir, profile, table):
        scan_parse = ScanParse(project_dir, profile, table)
        scan_parse.parse_logs.log()
        return scan_parse


class CliImpl:
    """
    Enables to configure a different cli impl for testing purposes
    """
    cli = CLI()
