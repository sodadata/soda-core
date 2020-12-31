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
    CommandHelper.scan(directory, warehouse, table, profile)
