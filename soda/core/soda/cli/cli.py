#  Copyright 2022 Soda
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
import sys
from typing import List, Optional, Tuple

import click
from ruamel.yaml import YAML
from ruamel.yaml.main import round_trip_dump
from soda.common.file_system import file_system
from soda.common.logs import configure_logging
from soda.scan import Scan
from soda.telemetry.soda_telemetry import SodaTelemetry
from soda.telemetry.soda_tracer import soda_trace, span_setup_function_args

from ..__version__ import SODA_CORE_VERSION

soda_telemetry = SodaTelemetry.get_instance()


@click.version_option(package_name="soda-core", prog_name="soda-core")
@click.group(help=f"Soda Core CLI version {SODA_CORE_VERSION}")
def main():
    pass


if __name__ == "__main__":
    main()


@main.command(
    short_help="runs a scan",
)
@click.option("-d", "--data-source", envvar="SODA_DATA_SOURCE", required=True, multiple=False, type=click.STRING)
@click.option(
    "-s",
    "--scan-definition",
    envvar="SODA_SCAN_DEFINITION",
    required=False,
    multiple=False,
    type=click.STRING,
    default="Soda Core CLI",
)
@click.option("-v", "--variable", required=False, default=None, multiple=True, type=click.STRING)
@click.option(
    "-c",
    "--configuration",
    required=False,
    multiple=True,
    type=click.STRING,
)
@click.option("-V", "--verbose", is_flag=True)
@click.argument("sodacl_paths", nargs=-1, type=click.STRING)
@soda_trace
def scan(
    sodacl_paths: List[str],
    data_source: str,
    scan_definition: Optional[str],
    configuration: List[str],
    variable: List[str],
    verbose: Optional[bool],
):
    """
    soda scan will

      * Parse the SodaCL files and report any errors

      * Build and execute queries for the checks

      * Evaluate the checks

      * Produce a summary on the console

      * If configured, send results to Soda Cloud

    option -d --data-source is the name of the data source in the configuration.  It's required.

    option -c --configuration is the configuration file containing the data source definitions.
    If not provided, the default ~/.soda/configuration.yml is used.

    option -v --variable pass a variable to the scan.  Variables are optional and multiple variables
    can be specified : -var "today=2020-04-12" -var "yesterday=2020-04-11"

    option -s --scan-definition is used By Soda Cloud (only if configured) to correlate subsequent scans and
    show check history over time. Scans normally happen as part of a schedule. It's optional. The default
    is "Soda Core CLI", which is usually sufficient when testing the CLI and Soda Cloud connection.

    option -V --verbose activates more verbose logging, including the queries that are executed.

    [SODACL_PATHS] is a list of file paths that can be either a SodaCL file or a directory.
    Directories are scanned recursive and will add all files ending with .yml

    Example:

    soda scan -d snowflake_customer_data -v TODAY=2022-03-11 -V ./snfk/pipeline_customer_checks.yml
    """

    configure_logging()

    fs = file_system()

    soda_telemetry.set_attribute("cli_command_name", "scan")

    span_setup_function_args(
        {
            "command_argument": {
                "scan_definition": scan_definition,
            },
            "command_option": {
                "sodacl_paths": len(sodacl_paths),
                "variables": len(variable),
                "configuration_paths": len(configuration),
                "offline": False,  # TODO: change after offline mode is supported.
                "non_interactive": False,  # TODO: change after non interactive mode is supported.
                "verbose": verbose,
            },
        }
    )

    scan = Scan()

    if verbose:
        scan.set_verbose()

    if isinstance(data_source, str):
        scan.set_data_source_name(data_source)

    if isinstance(scan_definition, str):
        scan.set_scan_definition_name(scan_definition)

    if configuration:
        for configuration_path in configuration:
            if not fs.exists(configuration_path):
                scan._logs.error(f"Configuration path '{configuration_path}' does not exist")
            else:
                scan.add_configuration_yaml_files(configuration_path)
    else:
        default_configuration_file_path = "~/.soda/configuration.yml"
        if fs.is_file(default_configuration_file_path):
            scan.add_configuration_yaml_file(default_configuration_file_path)
        elif not fs.exists(default_configuration_file_path):
            scan._logs.warning("No configuration file specified nor found on ~/.soda/configuration.yml")

    if sodacl_paths:
        for sodacl_path_element in sodacl_paths:
            scan.add_sodacl_yaml_files(sodacl_path_element)
    else:
        scan._logs.warning("No SodaCL files specified")

    if variable:
        variables_dict = dict([tuple(v.split("=")) for v in variable])
        scan.add_variables(variables_dict)

    sys.exit(scan.execute())


@main.command(
    short_help="updates a DRO in the distribution reference file",
)
@click.option("-d", "--data-source", envvar="SODA_DATA_SOURCE", required=True, multiple=False, type=click.STRING)
@click.option(
    "-c",
    "--configuration",
    required=False,
    multiple=False,
    type=click.STRING,
)
@click.option(
    "-n",
    "--name",
    required=False,
    multiple=False,
    type=click.STRING,
)
@click.option("-V", "--verbose", is_flag=True)
@click.argument("distribution_reference_file", type=click.STRING)
def update_dro(
    distribution_reference_file: str,
    data_source: str,
    configuration: str,
    name: Optional[str],
    verbose: Optional[bool],
):
    """
    soda update-dro will
      * Read the configuration and instantiate a connection to the data source
      * Read the definition properties in the distribution reference file
      * Update bins, labels and/or weights under key "reference distribution" in the distribution reference file

    option -d --data-source is the name of the data source in the configuration.  It's required.

    option -c --configuration is the configuration file containing the data source definitions.  The default
    is ~/.soda/configuration.yml is used.


    option -V --verbose activates more verbose logging, including the queries that are executed.

    [DISTRIBUTION_REFERENCE_FILE] is a distribution reference file

    Example:

    soda update-dro -d snowflake_customer_data ./customers_size_distribution_reference.yml
    """

    configure_logging()

    fs = file_system()

    distribution_reference_yaml_str = fs.file_read_as_str(distribution_reference_file)

    if not distribution_reference_yaml_str:
        logging.error(f"Could not read file {distribution_reference_file}")
        return

    yaml = YAML()
    try:
        distribution_reference_dict = yaml.load(distribution_reference_yaml_str)
    except BaseException as e:
        logging.error(f"Could not parse distribution reference file {distribution_reference_file}: {e}")
        return

    named_format = all(isinstance(value, dict) for value in distribution_reference_dict.values())
    unnamed_format = not any(
        isinstance(value, dict) and key != "distribution_reference"
        for key, value in distribution_reference_dict.items()
    )
    correct_format = named_format or unnamed_format
    if not correct_format:
        logging.error(
            f"""Incorrect distribution reference file format in "{distribution_reference_file}". If you want to use multiple DROs in a single distribution"""
            f""" reference file please make sure that they are all named. For more info see the documentation"""
            f""" \nhttps://docs.soda.io/soda-cl/distribution.html#generate-a-distribution-reference-object-dro."""
        )
        return

    if name and named_format:
        distribution_dict = distribution_reference_dict.get(name)
        if not distribution_dict:
            logging.error(
                f"""The dro name "{name}" that you provided does not exist in your distribution reference file "{distribution_reference_file}". """
                f"""For more information see the documentation:\nhttps://docs.soda.io/soda-cl/distribution.html#generate-a-distribution-reference-object-dro."""
            )
            return
    elif named_format:
        logging.error(
            f"""The distribution reference file "{distribution_reference_file}" that you used contains named DROs, but you did not provide"""
            f""" a DRO name with the -n argument. Please run soda update with -n "dro_name" to indicate which DRO you want to update."""
            f""" For more info see the documentation:\nhttps://docs.soda.io/soda-cl/distribution.html#generate-a-distribution-reference-object-dro."""
        )
        return
    else:
        distribution_dict = distribution_reference_dict

    dataset_name = distribution_dict.get("dataset")
    if not dataset_name:
        dataset_name = distribution_dict.pop("table")
        distribution_dict["dataset"] = dataset_name

    if not dataset_name:
        logging.error(f"Missing key 'dataset' in distribution reference file {distribution_reference_file}")

    column_name = distribution_dict.get("column")
    if not column_name:
        logging.error(f"Missing key 'column' in distribution reference file {distribution_reference_file}")

    distribution_type = distribution_dict.get("distribution_type")
    if not distribution_type:
        logging.error(f"Missing key 'distribution_type' in distribution reference file {distribution_reference_file}")

    filter = distribution_dict.get("filter")
    filter_clause = ""
    if filter is not None:
        filter_clause = f"WHERE {filter}"

    if dataset_name and column_name and distribution_type:
        query = f"SELECT {column_name} FROM {dataset_name} {filter_clause}"
        logging.info(f"Querying column values to build distribution reference:\n{query}")

        scan = Scan()
        scan.add_configuration_yaml_files(configuration)
        data_source_scan = scan._get_or_create_data_source_scan(data_source_name=data_source)
        if data_source_scan:
            rows = __execute_query(data_source_scan.data_source.connection, query)

            # TODO document what the supported data types are per data source type. And ensure proper Python data type conversion if needed
            column_values = [row[0] for row in rows]

            from soda.scientific.distribution.comparison import RefDataCfg
            from soda.scientific.distribution.generate_dro import DROGenerator

            dro = DROGenerator(RefDataCfg(distribution_type=distribution_type), column_values).generate()
            distribution_dict["distribution_reference"] = dro.dict()
            if "distribution reference" in distribution_dict:
                # To clean up the file and don't leave the old syntax
                distribution_dict.pop("distribution reference")

            new_file_content = round_trip_dump(distribution_reference_dict)

            fs.file_write_from_str(path=distribution_reference_file, file_content_str=new_file_content)


def __execute_query(connection, sql: str) -> List[Tuple]:
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
    except BaseException as e:
        logging.error(f"Query error: {e}\n{sql}", exception=e)
