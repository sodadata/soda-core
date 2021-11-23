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
import re
import sys
from datetime import datetime, timezone
from math import ceil
from typing import Optional

import click
import yaml

from sodasql.__version__ import SODA_SQL_VERSION
from sodasql.cli.indenting_yaml_dumper import IndentingDumper
from sodasql.common.logging_helper import LoggingHelper
from sodasql.dataset_analyzer import DatasetAnalyzer
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.metric import Metric
from sodasql.scan.scan_builder import ScanBuilder
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml_parser import (WarehouseYmlParser,
                                               read_warehouse_yml_file)

from sodasql.telemetry.soda_tracer import soda_trace, span_setup_function_args
from sodasql.telemetry.soda_telemetry import soda_telemetry

LoggingHelper.configure_for_cli()
logger = logging.getLogger(__name__)


@click.group(help=f"Soda CLI version {SODA_SQL_VERSION}")
def main():
    pass


@main.command(short_help='Create a template warehouse.yml file')
@click.argument('warehouse_type')
@click.option('-f', '--file',
              required=False,
              default='warehouse.yml',
              help='The destination filename for the warehouse configuration details. This can be a relative path.')
@click.option('-d', '--database', required=False, default=None, help='The database name to use for the connection')
@click.option('-u', '--username', required=False, default=None,
              help='The username to use for the connection, through env_var(...)')
@click.option('-p', '--password', required=False, default=None,
              help='The password to use for the connection, through env_var(...)')
@click.option('-w', '--warehouse', required=False, default=None, help='The warehouse name')
@soda_trace
def create(warehouse_type: str,
           file: Optional[str],
           warehouse: Optional[str],
           database: Optional[str],
           username: Optional[str],
           password: Optional[str]):
    """
    Creates a new warehouse.yml file and prepares credentials in your ~/.soda/env_vars.yml
    Nothing will be overwritten or removed, only added if it does not exist yet.

    WAREHOUSE_TYPE is one of {postgres, snowflake, redshift, bigquery, athena}
    """

    soda_telemetry.set_attribute('cli_command_name', 'create')
    soda_telemetry.set_attribute('datasource_type', warehouse_type)

    span_setup_function_args(
        {
            'command_argument': {'datasource_type': warehouse_type},
            'command_option':
                {
                    'file': file,
                    'using_warehouse': bool(warehouse),
                    'using_database': bool(database),
                    'using_username': bool(username)
                },
        },
    )

    try:
        """
        Creates a warehouse.yml file
        """
        logger.info(f"Soda CLI version {SODA_SQL_VERSION}")
        file_system = FileSystemSingleton.INSTANCE

        from sodasql.scan.dialect import ALL_WAREHOUSE_TYPES, Dialect
        dialect = Dialect.create_for_warehouse_type(warehouse_type)
        if not dialect:
            logger.info(
                f"Invalid warehouse type {warehouse_type}, use one of {str(ALL_WAREHOUSE_TYPES)}")
            sys.exit(1)

        configuration_params = {}
        if isinstance(database, str):
            configuration_params['database'] = database
        if isinstance(username, str):
            configuration_params['username'] = username
        if isinstance(password, str):
            configuration_params['password'] = password
        connection_properties = dialect.default_connection_properties(configuration_params)
        warehouse_env_vars_dict = dialect.default_env_vars(configuration_params)

        if file_system.file_exists(file):
            logger.info(f"Warehouse file {file} already exists")
        else:
            logger.info(f"Creating warehouse YAML file {file} ...")
            file_system.mkdirs(file_system.dirname(file))

            if not warehouse:
                warehouse = warehouse_type

            warehouse_dict = {
                'name': warehouse,
                'connection': connection_properties
            }
            warehouse_yml_str = yaml.dump(warehouse_dict, default_flow_style=False, sort_keys=False)
            file_system.file_write_from_str(file, warehouse_yml_str)

        dot_soda_dir = file_system.join(file_system.user_home_dir(), '.soda')
        if not file_system.file_exists(dot_soda_dir):
            file_system.mkdirs(dot_soda_dir)

        env_vars_file = file_system.join(dot_soda_dir, 'env_vars.yml')
        env_vars_yml_str = ''
        env_vars_file_exists = file_system.file_exists(env_vars_file)
        if env_vars_file_exists:
            env_vars_yml_str = file_system.file_read_as_str(env_vars_file)
            existing_env_vars_yml_dict = yaml.load(env_vars_yml_str, Loader=yaml.SafeLoader)
            if isinstance(existing_env_vars_yml_dict, dict) and warehouse in existing_env_vars_yml_dict:
                logger.info(f"Warehouse section {warehouse} already exists in {env_vars_file}.  Skipping...")
                warehouse_env_vars_dict = None

        if warehouse_env_vars_dict:
            warehouse_env_vars_dict = {
                warehouse: warehouse_env_vars_dict
            }

            if len(env_vars_yml_str) > 0:
                env_vars_yml_str += '\n'

            env_vars_yml_str += yaml.dump(warehouse_env_vars_dict,
                                          default_flow_style=False,
                                          sort_keys=False)

            if env_vars_file_exists:
                logger.info(
                    f"Adding env vars for {warehouse} to {env_vars_file}")
            else:
                logger.info(
                    f"Creating {env_vars_file} with example env vars in section {warehouse}")

            file_system.file_write_from_str(env_vars_file, env_vars_yml_str)

        logger.info(f"Review warehouse.yml by running command")
        logger.info(f"  cat {file}")
        if warehouse_env_vars_dict:
            logger.info(
                f"Review section {warehouse} in ~/.soda/env_vars.yml by running command")
            logger.info(f"  cat ~/.soda/env_vars.yml")
        logger.info(f"Then run the soda analyze command")
    except Exception as e:
        logger.exception(f'Exception: {str(e)}')
        logger.info("If you think this is a bug in Soda SQL, please open an issue at: "
                    "https://github.com/sodadata/soda-sql/issues/new/choose")
        sys.exit(1)


def create_table_filter_regex(table_filter):
    if not isinstance(table_filter, str):
        return None
    regex_parts = []
    for table_filter_part in table_filter.split(','):
        regex_part = create_table_filter_regex_part(table_filter_part)
        regex_parts.append(regex_part)
    return '(' + ('|'.join(regex_parts)) + ')'


def create_table_filter_regex_part(table_filter):
    table_filter_regex = ''
    for c in table_filter:
        if c == '*':
            table_filter_regex += '.*'
        else:
            table_filter_regex += re.escape(c)
    return table_filter_regex


def matches_table_include(table_name: str, table_include_pattern):
    return table_include_pattern is None or re.match(table_include_pattern, table_name, re.IGNORECASE)


def matches_table_exclude(table_name: str, table_exclude_pattern):
    return table_exclude_pattern is None or not re.match(table_exclude_pattern, table_name, re.IGNORECASE)


@main.command(short_help='Analyze tables and scaffold SCAN YAML')
@click.argument('warehouse_file', required=False, default='warehouse.yml')
@click.option('-i', '--include',
              required=False,
              help='Table name includes filter, case insensitive, comma separated list, use * as a wild card')
@click.option('-e', '--exclude',
              required=False,
              help='Table name exclusion filter, case insensitive, comma separated list, use * as a wild card')
@click.option('-l', '--limit',
              type=int,
              required=False,
              help='Limit the number of tables analyzed. This option is ignored for Hive and Spark dialects'
              )
@soda_trace
def analyze(warehouse_file: str, include: str, exclude: str, limit: int):
    """
    Analyzes tables in the warehouse and creates scan YAML files based on the data in the table. By default it creates
    files in a subdirectory called "tables" on the same level as the warehouse file.

    WAREHOUSE_FILE contains the connection details to the warehouse. This file can be created using the `soda create` command.
    The warehouse file argument is optional and defaults to 'warehouse.yml'.
    """
    logger.info(SODA_SQL_VERSION)
    file_system = FileSystemSingleton.INSTANCE
    warehouse = None

    soda_telemetry.set_attribute('cli_command_name', 'analyze')

    span_setup_function_args(
        {
            'command_argument': {'warehouse_file': warehouse_file},
            'command_option':
                {
                    'include': include,
                    'exclude': exclude,
                    'limit': limit
                },
        }
    )

    try:
        logger.info(f'Analyzing {warehouse_file} ...')

        warehouse_yml_dict = read_warehouse_yml_file(warehouse_file)
        warehouse_yml_parser = WarehouseYmlParser(warehouse_yml_dict, warehouse_file)
        warehouse = Warehouse(warehouse_yml_parser.warehouse_yml)

        logger.info('Querying warehouse for tables')
        warehouse_dir = file_system.dirname(warehouse_file)

        file_system = FileSystemSingleton.INSTANCE

        def fileify(name: str):
            return re.sub(r'[^A-Za-z0-9_.]+', '_', name).lower()

        table_dir = file_system.join(warehouse_dir, 'tables')
        if not file_system.file_exists(table_dir):
            logger.info(f'Creating tables directory {table_dir}')
            file_system.mkdirs(table_dir)
        else:
            logger.info(f'Directory {table_dir} already exists')

        first_table_scan_yml_file = None

        dialect = warehouse.dialect

        if hasattr(dialect, "sql_tables_metadata"):
            rows = dialect.sql_tables_metadata()
        else:
            tables_metadata_query = dialect.sql_tables_metadata_query(limit=limit)
            rows = warehouse.sql_fetchall(tables_metadata_query)

        table_include_regex = create_table_filter_regex(include)
        table_exclude_regex = create_table_filter_regex(exclude)

        for row in rows:
            table_name = row[0]

            if (matches_table_include(table_name, table_include_regex)
                and matches_table_exclude(table_name, table_exclude_regex)):
                dataset_analyzer = DatasetAnalyzer()
                dataset_analyze_results = dataset_analyzer.analyze(warehouse, table_name)

                table_scan_yaml_file = file_system.join(table_dir, f'{fileify(table_name)}.yml')

                if not first_table_scan_yml_file:
                    first_table_scan_yml_file = table_scan_yaml_file

                if file_system.file_exists(table_scan_yaml_file):
                    logger.info(f"Scan file {table_scan_yaml_file} already exists")
                else:
                    logger.info(f"Creating {table_scan_yaml_file} ...")
                    from sodasql.scan.scan_yml_parser import (KEY_METRICS,
                                                              KEY_TABLE_NAME,
                                                              KEY_TESTS,
                                                              KEY_COLUMNS,
                                                              COLUMN_KEY_VALID_FORMAT,
                                                              COLUMN_KEY_TESTS)
                    scan_yaml_dict = {
                        KEY_TABLE_NAME: table_name,
                        KEY_METRICS:
                            [Metric.ROW_COUNT] +
                            Metric.METRIC_GROUPS[Metric.METRIC_GROUP_MISSING] +
                            Metric.METRIC_GROUPS[Metric.METRIC_GROUP_VALIDITY] +
                            Metric.METRIC_GROUPS[Metric.METRIC_GROUP_LENGTH] +
                            Metric.METRIC_GROUPS[Metric.METRIC_GROUP_STATISTICS],
                        KEY_TESTS: [
                            'row_count > 0'
                        ]
                    }

                    columns = {}
                    for column_analysis_result in dataset_analyze_results:
                        if column_analysis_result.validity_format:
                            column_yml = {
                                COLUMN_KEY_VALID_FORMAT: column_analysis_result.validity_format
                            }
                            values_count = column_analysis_result.values_count
                            valid_count = column_analysis_result.valid_count
                            if valid_count > (values_count * .8):
                                valid_percentage = valid_count * 100 / values_count
                                invalid_threshold = (100 - valid_percentage) * 1.1
                                invalid_threshold_rounded = ceil(invalid_threshold)
                                invalid_comparator = '==' if invalid_threshold_rounded == 0 else '<='
                                column_yml[COLUMN_KEY_TESTS] = [
                                    f'invalid_percentage {invalid_comparator} {str(invalid_threshold_rounded)}'
                                ]
                                columns[column_analysis_result.column_name] = column_yml

                    if columns:
                        scan_yaml_dict[KEY_COLUMNS] = columns

                    scan_yml_str = yaml.dump(scan_yaml_dict,
                                             sort_keys=False,
                                             Dumper=IndentingDumper,
                                             default_flow_style=False)
                    file_system.file_write_from_str(table_scan_yaml_file, scan_yml_str)
            else:
                logger.info(f"Skipping table {table_name}")

        logger.info(
            f"Next run 'soda scan {warehouse_file} {first_table_scan_yml_file}' to calculate measurements and run tests")

    except Exception as e:
        logger.exception(f'Exception: {str(e)}')
        logger.info("If you think this is a bug in Soda SQL, please open an issue at"
                    "https://github.com/sodadata/soda-sql/issues/new/choose")
        sys.exit(1)

    finally:
        if warehouse and warehouse.connection:
            try:
                warehouse.connection.close()
            except Exception as e:
                logger.debug(f'Closing connection failed: {str(e)}')


@main.command(short_help='Compute metrics and run tests for a given table')
@click.argument('warehouse_yml_file')
@click.argument('scan_yml_file')
@click.option('-v', '--variables',
              required=False,
              default=None,
              multiple=True,
              help='Variables like -v start=2020-04-12.  Put values with spaces in single or double quotes.')
@click.option('-t', '--time',
              required=False,
              default=datetime.now(tz=timezone.utc).isoformat(timespec='seconds'),
              help='The scan time in ISO8601 format like eg 2021-04-28T09:00:00+02:00')
@click.option('--offline',
              required=False,
              is_flag=True,
              default=False,
              help='Run scan offline, do not push to Soda Cloud even if it is configured')
@click.option('-ni', '--non-interactive',
              is_flag=True,
              default=False,
              help='Use this flag if you want to skip confirmations and run the scan.')
@click.option('-srf', '--scan-results-file',
              required=False,
              default=None,
              help='Specify the file path where the scan results as json will be stored')
@click.option('-frd', '--failed-rows-dir',
              required=False,
              help='Specify the directory to store the failed rows')
@soda_trace
def scan(scan_yml_file: str, warehouse_yml_file: str, variables: tuple, time: str, offline: bool,
         non_interactive: bool = False, scan_results_file: Optional[str] = None, failed_rows_dir: Optional[str] = None):
    """
    Computes all measurements and runs all tests on one table.  Exit code 0 means all tests passed.
    Non zero exit code means tests have failed or an exception occurred.
    If the warehouse YAML file has a Soda cloud account configured, measurements and test results will be uploaded.

    WAREHOUSE_YML_FILE is the warehouse YAML file containing connection details.

    SCAN_YML_FILE is the scan YAML file that contains the metrics and tests for a table to run.
    """
    logger.info(SODA_SQL_VERSION)

    soda_telemetry.set_attribute('cli_command_name', 'analyze')

    span_setup_function_args(
        {
            'command_argument':
                {
                    'warehouse_yml_file': warehouse_yml_file,
                    'scan_yml_file': scan_yml_file
                },
            'command_option':
                {
                    'variables': variables,
                    'time': time,
                    'offline': offline,
                    'non_interactive': non_interactive,
                    'scan_results_file': scan_results_file
                },
        }
    )

    if offline:
        logger.info('Running in offline mode, scan results will NOT be pushed to Soda Cloud.')

    try:
        variables_dict = {}
        if variables:
            for variable in variables:
                assign_index = variable.find('=')
                if 0 < assign_index < len(variable) - 1:
                    variable_name = variable[0:assign_index]
                    variable_value = variable[assign_index + 1:]
                    variables_dict[variable_name] = variable_value
            logger.debug(f'Variables {variables_dict}')

        scan_builder = ScanBuilder()
        scan_builder.warehouse_yml_file = warehouse_yml_file
        scan_builder.scan_yml_file = scan_yml_file
        datetime.fromisoformat(time)
        scan_builder.time = time
        scan_builder.non_interactive = non_interactive
        scan_builder.scan_results_json_path = scan_results_file
        # TODO Failed Rows Dir
        scan_builder.failed_rows_dir = failed_rows_dir

        if non_interactive and not time == datetime.now(tz=timezone.utc).isoformat(timespec='seconds'):
            logging.warning(f'You are using the --time option with the following value: {time}, meaning that the '
                            f'actual date of the scan is being altered manually.')
            answer = input("Are you sure you wish to continue with the --time option? Press 'y' to continue... ")
            if answer != 'y':
                sys.exit(1)

        logger.info(f'Scanning {scan_yml_file} ...')

        scan_builder.variables = variables_dict
        scan = scan_builder.build(offline=offline)
        if not scan:
            logger.error(f'Could not read scan configurations. Aborting before scan started.')
            sys.exit(1)

        from sodasql.scan.scan_result import ScanResult
        scan_result: ScanResult = scan.execute()

        logger.info(f'Scan summary ------')
        logger.info(f'{len(scan_result.measurements)} measurements computed')
        logger.info(f'{len(scan_result.test_results)} tests executed')

        if scan_result.has_test_failures():
            logger.info(f'{scan_result.get_test_failures_count()} of {len(scan_result.test_results)} tests failed:')
            for test_result in scan_result.test_results:
                if not test_result.passed:
                    logger.info(f'  {test_result}')

        if scan_result.has_errors():
            logger.info(f'Errors occurred!')
            for error in scan_result.get_errors():
                logger.error(f'  {error}')

        if scan_result.is_passed():
            logger.info(f'All is good. No tests failed.')
        exit_code = 0 if scan_result.is_passed() else 1
        logger.info(f'Exiting with code {exit_code}')
        sys.exit(exit_code)

    except Exception as e:
        logger.exception(f'Scan failed: {str(e)}')
        logger.info("If you think this is a bug in Soda SQL, please open an issue at: "
                    "https://github.com/sodadata/soda-sql/issues/new/choose")
        logger.info(f'Exiting with code 1')
        sys.exit(1)
