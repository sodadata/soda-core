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

import click
import json
import logging

from .utils import ProfilesReader
from .utils import ScanConfigurationReader
from sodasql.scan.warehouse import Warehouse
from sodasql.scan import parse_logs


@click.group()
def main():
    pass


@main.command()
def initialize():
    click.echo('Initialize')


@main.command()
@click.argument('dir')
def check(directory: str):
    click.echo(f'Checking configuration directory {directory}')


@main.command()
@click.argument('directory')
@click.argument('warehouse')
@click.argument('table')
@click.option('-p', '--profile', type=str, required=False)
def scan(directory: str, warehouse: str, table: str, profile):
    profiles_reader = ProfilesReader(profile)
    _log_messages(profiles_reader.parse_logs)
    warehouse_configuration = profiles_reader.configuration
    scan_configuration_reader = ScanConfigurationReader(warehouse, table, directory)
    _log_messages(scan_configuration_reader.parse_logs)
    scan_configuration = scan_configuration_reader.configuration
    warehouse = Warehouse(warehouse_configuration)
    scan_result = warehouse.create_scan(scan_configuration).execute()
    print('RESULTS:', scan_result.test_results)
    print('MEASUREMENTS:')
    for measurement in scan_result.measurements:
        print(measurement)


def _log_messages(logs):
    for log in logs.logs:
        if log.level in [parse_logs.ERROR, parse_logs.WARNING]:
            logging.error(log)
        else:
            logging.info(log)
