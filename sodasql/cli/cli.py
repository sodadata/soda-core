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

from typing import Optional

import click

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
               profile: Optional[str] = 'default',
               target: Optional[str] = None):
        """
        Creates a project directory and ensures a profile is present
        """
        self._log_version()
        click.echo(f"Creating ./{project_dir}/soda_project.yaml ...")
        click.echo(f"Creating ./{project_dir}/{profile}/ ...")
        click.echo(f"Creating ~/.soda/profiles.yml ...")
        click.echo(f"Creating profile {profile} in ~/.soda/profiles.yml ...")

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
