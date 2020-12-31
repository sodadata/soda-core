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
from .utils import CommandHelper
from sodasql.scan.warehouse import Warehouse
from sodasql.scan import parse_logs


@click.group()
def main():
    pass


@main.command()
@click.argument('project_directory')
@click.argument('warehouse_name')
@click.option('-w', '--warehouse-type', type=str, required=False,
              help='Warehouse type, e.g., "postgres", "snowflake", etc')
@click.option('-p', '--profile', required=False, default='default',
              help='Runs init using specific profile.')
def init(project_directory: str, warehouse_name: str, warehouse_type: str, profile: str):
    """
    Initializes ~/.soda/profiles.yml file with the given warehouse.
    """
    CommandHelper.init(project_directory, warehouse_name, warehouse_type, profile)


@main.command()
@click.option('-w', '--warehouse-name', required=False, help='Warehouse to verify.')
@click.option('-p', '--profile', required=False, default='default',
              help='Runs init using specific profile.')
def verify(warehouse_name: str, profile: str):
    """
    Verifies that warehouse(s) can connect.
    """
    CommandHelper.verify(warehouse_name, profile)


@main.command()
@click.argument('project_directory')
@click.argument('warehouse_name')
@click.option('-p', '--profile', required=False, default='default',
              help='Runs create using specific profile.')
def create(project_directory: str, warehouse_name: str, profile: str):
    """
    Creates scan configurations.
    """
    CommandHelper.create(project_directory, warehouse_name, profile)


@main.command()
@click.argument('project_directory')
@click.argument('warehouse_name')
@click.argument('table')
@click.option('-p', '--profile', required=False,  default='default',
              help='Runs scan using specific profile.')
def scan(project_directory: str, warehouse_name: str, table: str, profile: str):
    """
    Scans a table.
    """
    CommandHelper.scan(project_directory, warehouse_name, table, profile)
