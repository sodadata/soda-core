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
import sys

import click

from .cli import CLI, CliImpl


@click.group()
def main():
    pass


@main.command()
@click.argument('project_directory')
@click.argument('warehouse_name')
@click.option('-w', '--warehouse-type', type=str, required=False,
              help='Warehouse type, e.g., "postgres", "snowflake", etc')
@click.option('-p', '--profile', required=False, default='default',
              help='Analyses the warehouse tables and creates scan.yml files in your project dir')
def init(project_directory: str, warehouse_name: str, warehouse_type: str, profile: str):
    CliImpl.cli.init(project_directory, warehouse_name, warehouse_type, profile)


@main.command(help='Dry run to verify if the configuration is ok. No connection is made to the warehouse.')
@click.option('-w', '--warehouse-name', required=False, help='Warehouse to verify.')
@click.option('-p', '--profile', required=False, default='default')
def verify(warehouse_name: str, profile: str):
    CliImpl.cli.verify(warehouse_name, profile)


@main.command(help='Creates a project directory and ensures a profile is present')
@click.argument('project_dir')
@click.argument('warehouse_type')
@click.argument('profile')
@click.option('-t', '--target', required=False, default=None, help='The target eg dev vs prod.  See target docs')
def create(project_dir: str, warehouse_type: str, profile: str):
    CliImpl.cli.create(project_dir, warehouse_type, profile)


@main.command()
@click.argument('project_directory')
@click.argument('warehouse_name')
@click.argument('table')
@click.option('-p', '--profile', required=False,  default='default',
              help='Scans a table by executing queries, computes measurements and runs tests')
def scan(project_directory: str, warehouse_name: str, table: str, profile: str):
    exit_code = CliImpl.cli.scan(project_directory, warehouse_name, table, profile)
    sys.exit(exit_code)
