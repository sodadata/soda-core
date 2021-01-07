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
from typing import Optional

import click

from sodasql.cli.cli_impl import CliImpl


@click.group()
def main():
    pass


@main.command(help='Creates a new warehouse directory and prepares credentials in your ~/.soda/env_vars.yml '
                   'Nothing will be overwritten or removed, only added if it does not exist yet. '
                   'WAREHOUSE_DIR is a directory that contains all soda related yaml config files for one warehouse. '
                   'WAREHOUSE_TYPE is one of {postgres, snowflake, redshift, bigquery, athena}')
@click.argument('warehouse_dir')
@click.argument('warehouse_type')
@click.option('-n', '--warehouse_name',     required=False, default=None, help='The warehouse name')
@click.option('-d', '--database', required=False, default=None, help='The database name to use for the connection')
@click.option('-u', '--username', required=False, default=None, help='The username to use for the connection')
@click.option('-p', '--password', required=False, default=None, help='The password to use for the connection')
def create(warehouse_dir: str,
           warehouse_type: str,
           warehouse_name: Optional[str],
           database: Optional[str],
           username: Optional[str],
           password: Optional[str]):
    CliImpl.INSTANCE.create(warehouse_dir, warehouse_type, warehouse_name, database, username, password)


@main.command(help='Finds tables in the warehouse and based on the contents, creates initial scan.yml files.'
                   'WAREHOUSE_DIR is the warehouse directory containing a warehouse.yml file')
@click.argument('warehouse_dir')
def init(warehouse_dir: str):
    CliImpl.INSTANCE.init(warehouse_dir)


@main.command(help='Computes all measurements and runs all tests on one table.  Exit code 0 means all tests passed.'
                   'Non zero exist code means tests have failed or an exception occured.  '
                   'If the project has a Soda cloud account configured, '
                   'measurements and test results will be uploaded. '
                   'WAREHOUSE_DIR is the warehouse directory containing a warehouse.yml file '
                   'TABLE is the name of the table to be scanned')
@click.argument('warehouse_dir')
@click.argument('table')
@click.option('--timeslice', required=False, default=None, help='The timeslice')
def scan(warehouse_dir: str, table: str, timeslice: str = None, timeslice_variables: dict = None):
    exit_code = CliImpl.INSTANCE.scan(warehouse_dir, table, timeslice, timeslice_variables)
    sys.exit(exit_code)
