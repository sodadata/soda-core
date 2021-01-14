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
import os
from typing import AnyStr, List

import yaml
from sodasql.cli.file_system import FileSystemSingleton
from sodasql.scan.parser import Parser
from sodasql.scan.scan import Scan
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.sql_metric_yml import SqlMetricYml
from sodasql.scan.warehouse_yml import WarehouseYml


class ScanBuilder:
    """
    Programmatic scan execution based on default dir structure:

    scan_builder = ScanBuilder()
    scan_builder.read_scan_from_dirs('~/my_warehouse_dir', 'my_table_dir')
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_failures():
        print('Scan has test failures, stop the pipeline')

    Programmatic scan execution reading yaml files by path:

    scan_builder = ScanBuilder()
    scan_builder.read_warehouse_yml('./anydir/warehouse.yml')
    scan_builder.read_scan_yml('./anydir/scan.yml')
    scan_builder.read_sql_metrics_from_dir('./anydir/')
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_failures():
        print('Scan has test failures, stop the pipeline')

    Programmatic scan execution using dicts:

    scan_builder = ScanBuilder()
    scan_builder.warehouse_dict({
        'name': 'my_warehouse_name',
        'connection': {
            'type': 'snowflake',
            ...
        }
    })
    scan_builder.scan_dict({...})
    scan_builder.sql_metric_dict({...})
    scan_builder.sql_metric_dict({...})
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_failures():
        print('Scan has test failures, stop the pipeline')
    """

    def __init__(self):
        self.file_system = FileSystemSingleton.INSTANCE
        self.warehouse_yml: WarehouseYml = None
        self.scan_yml: ScanYml = None
        self.variables: dict = {}
        self.sql_metric_ymls: List[SqlMetricYml] = []
        self.parsers: List[Parser] = []
        self.assert_no_warnings_or_errors = True

    def read_scan_from_dirs(self, warehouse_dir_path: str, table_dir_name: str):
        """
        Reads warehouse, scan and sql metrics using the directory structure.
        """
        if not self.__is_valid_type('warehouse_dir_path', warehouse_dir_path, str) or \
           not self.__is_valid_type('table_dir_name', table_dir_name, str):
            return

        if not self.file_system.is_dir(warehouse_dir_path):
            logging.error(f'warehouse_dir_path {warehouse_dir_path} is not a directory')

        warehouse_yml_path = self.file_system.join(warehouse_dir_path, 'warehouse.yml')
        self.read_warehouse_yml(warehouse_yml_path)

        table_dir_path = self.file_system.join(warehouse_dir_path, table_dir_name)
        if not self.file_system.is_dir(table_dir_path):
            logging.error(f'table_dir_path {table_dir_path} is not a directory')

        scan_yml_path = self.file_system.join(table_dir_path, 'scan.yml')
        self.read_scan_yml(scan_yml_path)

        self.read_sql_metrics_from_dir(table_dir_path)

    def read_scan_yml(self, scan_yml_path: str):
        if not self.__is_readable_file_path_str('scan_yml_path', scan_yml_path):
            return

        scan_yaml_str = self.file_system.file_read_as_str(scan_yml_path)
        scan_dict = self.__parse_yaml(scan_yaml_str, scan_yml_path)
        self.scan_dict(scan_dict)

    def read_warehouse_yml(self, warehouse_yml_path: str):
        if not self.__is_readable_file_path_str('warehouse_yml_path', warehouse_yml_path):
            return

        warehouse_yaml_str = self.file_system.file_read_as_str(warehouse_yml_path)

        if warehouse_yaml_str:
            warehouse_dict = self.__parse_yaml(warehouse_yaml_str, warehouse_yml_path)
            self.warehouse_dict(warehouse_dict)
        else:
            logging.info(f'Failed to read warehouse yaml file: {warehouse_yml_path}')

    def read_sql_metrics_from_dir(self, sql_metrics_dir_path):
        if not self.file_system.is_dir(sql_metrics_dir_path):
            logging.error(f'sql_metrics_dir_path {sql_metrics_dir_path} is not a directory')
        for sql_metric_path in self.file_system.list_dir(sql_metrics_dir_path):
            if not sql_metric_path.endswith(os.sep + 'scan.yml'):
                self.read_sql_metric(sql_metric_path)

    def read_sql_metric(self, sql_metric_path):
        if not self.__is_readable_file_path_str('sql_metric_path', sql_metric_path):
            return

        sql_metric_yaml_str = self.file_system.file_read_as_str(sql_metric_path)
        sql_metric_dict = self.__parse_yaml(sql_metric_yaml_str, sql_metric_path)
        return self.sql_metric_dict(sql_metric_dict, sql_metric_path)

    def warehouse_dict(self, warehouse_dict: dict, warehouse_source_description: AnyStr = 'Warehouse'):
        from sodasql.scan.warehouse_yml_parser import WarehouseYmlParser
        warehouse_parser = WarehouseYmlParser(warehouse_dict, warehouse_source_description)
        warehouse_parser.log()
        self.parsers.append(warehouse_parser)
        self.warehouse_yml = warehouse_parser.warehouse_yml

    def scan_dict(self, scan_dict: dict, scan_source_description: AnyStr = 'Scan'):
        from sodasql.scan.scan_yml_parser import ScanYmlParser
        scan_yml_parser = ScanYmlParser(scan_dict, scan_source_description)
        scan_yml_parser.log()
        self.parsers.append(scan_yml_parser)
        self.scan_yml = scan_yml_parser.scan_yml

    def sql_metric_dict(self, sql_metric_dict: dict, sql_metric_source_description: AnyStr = 'SQL Metric'):
        from sodasql.scan.sql_metric_yml_parser import SqlMetricYmlParser
        sql_metric_parser = SqlMetricYmlParser(sql_metric_dict, sql_metric_source_description)
        sql_metric_parser.log()
        if sql_metric_parser.sql_metric:
            self.sql_metric_ymls.append(sql_metric_parser.sql_metric)

    def variable(self, name: str, value):
        self.variables[name] = value

    def build(self):
        for parser in self.parsers:
            parser.assert_no_warnings_or_errors()
        from sodasql.scan.warehouse import Warehouse
        self.warehouse = Warehouse(self.warehouse_yml)
        return Scan(warehouse=self.warehouse,
                    scan_yml=self.scan_yml,
                    variables=self.variables,
                    sql_metrics=self.sql_metric_ymls,
                    soda_client=None)

    def __parse_yaml(self, warehouse_yaml_str: AnyStr, file_name: AnyStr):
        try:
            return yaml.load(warehouse_yaml_str, Loader=yaml.FullLoader)
        except Exception as e:
            logging.error(f'Parsing yaml file {file_name} failed: {str(e)}')

    def __is_valid_type(self, param_name: str, param_value, param_type: type):
        if param_value is None:
            logging.error(f'Parameter {param_name} is required: was None')
            return False
        if not isinstance(param_value, param_type):
            logging.error(f'Parameter {param_name} expected {type(param_type)}: was {type(param_value)}')
            return False
        return True

    def __is_readable_file_path_str(self, param_name: str, file_path_str: str):
        if self.__is_valid_type(param_name, file_path_str, str):
            if not self.file_system.file_exists(file_path_str):
                logging.info(f'{param_name} {file_path_str} does not exist')
                return False
            elif not self.file_system.is_file(file_path_str):
                logging.info(f'{param_name} {file_path_str} is not a file')
                return False
            elif not self.file_system.is_readable(file_path_str):
                logging.info(f'{param_name} {file_path_str} is readable')
                return False
        return True
