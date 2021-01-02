import click
import json
import logging

from . import ProfilesReader
from . import ScanConfigurationReader
from sodasql.scan.warehouse import Warehouse
from sodasql.scan import parse_logs
from ...profile.profile_parse import ProfileParse


class CommandHelper:

    @classmethod
    def scan(cls, project_directory: str, warehouse: str, table: str, profile):
        warehouse_configuration, scan_configuration = cls._read_configuration(profile, warehouse, table,
                                                                              project_directory)
        warehouse = Warehouse(warehouse_configuration)
        cls._output_scan_result(warehouse.create_scan(scan_configuration).execute())
        warehouse.close()

    @classmethod
    def init(cls, project_directory: str, warehouse_name: str, warehouse_type: str, profile_name: str):
        """
        TODO: Implement this stub method.
        """
        click.echo(f"Creating {ProfilesReader.PROFILES_FILE_PATH}...")
        if warehouse_type:
            click.echo(
                f"Adding {warehouse_name} of type {warehouse_type} to profile {profile_name} in "
                f"{ProfilesReader.PROFILES_FILE_PATH}...")
        click.echo(f"Creating {project_directory}/soda_project.yml...")
        click.echo(f"Please review and update the output {warehouse_type} for profile {profile_name} "
                   f"in {ProfilesReader.PROFILES_FILE_PATH} then run 'soda create'.")

    @classmethod
    def verify(cls, warehouse_name: str, profile_name: str):
        """
        TODO: Implement this stub method.
        """
        if warehouse_name:
            click.echo(f"Verifying configuration for {warehouse_name} in profile {profile_name}...")
        else:
            click.echo(f"Verifying configuration for all warehouses in profile {profile_name}...")

    @classmethod
    def create(cls, project_directory: str, warehouse_name: str, profile: str):
        """
        TODO: Implement this stub method.
        """
        click.echo(f"Creating ./{project_directory}/{warehouse_name}/table_name/scan.yaml...")

    @staticmethod
    def _output_scan_result(scan_result):
        click.echo('MEASUREMENTS:')
        for measurement in scan_result.measurements:
            click.echo(measurement)

    @classmethod
    def _read_configuration(cls, profile, warehouse, table, directory):
        profile_parse = ProfileParse(profile)
        profile_parse.parse_logs.log()
        profile_parse.parse_logs.assert_no_warnings_or_errors()
        scan_configuration_reader = ScanConfigurationReader(warehouse, table, directory)
        scan_configuration_reader.parse_logs.log()
        scan_configuration_reader.parse_logs.assert_no_warnings_or_errors()
        return profile_parse.properties, scan_configuration_reader.configuration
