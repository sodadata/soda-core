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


@main.command(help='Creates a new project directory and prepares credentials in your ~/.soda/env_vars.yml '
                   'Nothing will be overwritten or removed, only added if it does not exist yet. '
                   'PROJECT_DIR is a soda directory that groups all soda related yaml config files in a specific '
                   'folder structure. '
                   'WAREHOUSE_TYPE is one of {postgres, snowflake, redshift, bigquery, athena}')
@click.argument('project_dir')
@click.argument('warehouse_type')
def create(project_dir: str, warehouse_type: str):
    CliImpl.cli.create(project_dir, warehouse_type)


@main.command(help='Initializes scan.yml files based on profiling tables found in the warehouse. '
                   'PROJECT_DIR is the soda project directory containing a soda_project.yml file')
@click.argument('soda_project_dir')
def init(soda_project_dir: str):
    CliImpl.cli.init(soda_project_dir)


@main.command(help='Computes all measurements and runs all tests on one table.  Exit code 0 means no tests have failed.'
                   'Non zero exist code means tests have failed.  If the project has a Soda cloud account configured, '
                   'measurements will be uploaded. '
                   'PROJECT_DIR is the soda project directory containing a soda_project.yml file '
                   'TABLE is the name of the table to be scanned')
@click.argument('project_dir')
@click.argument('table')
@click.option('--timeslice', required=False, default=None, help='The timeslice')
def scan(project_dir: str, table: str, timeslice: str = None, timeslice_variables: dict = None):
    exit_code = CliImpl.cli.scan(project_dir, table, timeslice, timeslice_variables)
    sys.exit(exit_code)
