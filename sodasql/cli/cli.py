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


@click.group()
def cli():
    pass

@cli.command()
def initialize():
    click.echo('Initialize')

@cli.command()
@click.argument('dir')
def check(dir: str):
    click.echo(f'Checking configuration directory {dir}')

@cli.command()
@click.argument('dir')
@click.argument('store')
@click.argument('table')
def scan(dir: str, store: str, table):
    click.echo(f'Scan dir={dir} store={store} table={table}')


if __name__ == '__main__':
    cli()
