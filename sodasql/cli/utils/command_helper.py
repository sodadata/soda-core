import click
import json
import logging

from . import ProfilesReader
from . import ScanConfigurationReader
from sodasql.scan.warehouse import Warehouse
from sodasql.scan import parse_logs


class CommandHelper:

    @classmethod
    def scan(cls, directory: str, warehouse: str, table: str, profile):
        warehouse_configuration, scan_configuration = cls._read_configuration(profile, warehouse, table, directory)
        warehouse = Warehouse(warehouse_configuration)
        cls._output_scan_result(warehouse.create_scan(scan_configuration).execute())
        warehouse.close()

    @staticmethod
    def _output_scan_result(scan_result):
        click.echo('MEASUREMENTS:')
        for measurement in scan_result.measurements:
            click.echo(measurement)

    @classmethod
    def _read_configuration(cls, profile, warehouse, table, directory):
        profiles_reader = ProfilesReader(profile)
        cls._log_messages(profiles_reader.parse_logs)
        scan_configuration_reader = ScanConfigurationReader(warehouse, table, directory)
        cls._log_messages(scan_configuration_reader.parse_logs)
        return profiles_reader.configuration, scan_configuration_reader.configuration

    @staticmethod
    def _log_messages(logs):
        for log in logs.logs:
            if log.level == parse_logs.ERROR:
                logging.error(log.message)
            elif log.level == parse_logs.WARNING:
                logging.warning(log.message)
            else:
                logging.info(log.message)
