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
from typing import List

from sodasql.common.yaml_helper import YamlHelper
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.parser import Parser
from sodasql.scan.scan import Scan
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.warehouse_yml import WarehouseYml
from sodasql.scan.warehouse_yml_parser import read_warehouse_yml_file
from sodasql.soda_server_client.soda_server_client import SodaServerClient


class ScanBuilder:
    """
    Programmatic scan execution based on default dir structure:

    scan_builder = ScanBuilder()
    scan_builder.scan_yml_file = 'tables/my_table.yml'
    # scan_builder will automatically find the warehouse.yml in the parent and same directory as the scan YAML file
    # scan_builder.warehouse_yml_file = '../warehouse.yml'
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_failures():
        print('Scan has test failures, stop the pipeline')

    Programmatic scan execution using dicts:

    scan_builder = ScanBuilder()
    scan_builder.warehouse_dict = {
        'name': 'my_warehouse_name',
        'connection': {
            'type': 'snowflake',
            ...
        }
    })
    scan_builder.scan_dict = {
        ...
    }
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_failures():
        print('Scan has test failures, stop the pipeline')
    """

    def __init__(self):
        self.file_system = FileSystemSingleton.INSTANCE
        self.warehouse_yml_file: str = None
        self.warehouse_yml_dict: dict = None
        self.warehouse_yml: WarehouseYml = None
        self.scan_yml_file: str = None
        self.time: str = None
        self.scan_yml_dict: dict = None
        self.scan_yml: ScanYml = None
        self.variables: dict = {}
        self.parsers: List[Parser] = []
        self.assert_no_warnings_or_errors = True
        self.soda_server_client: SodaServerClient = None

    def build(self):
        self._build_warehouse_yml()
        self._build_scan_yml()

        for parser in self.parsers:
            parser.assert_no_warnings_or_errors()
        if not self.scan_yml or not self.warehouse_yml:
            return

        from sodasql.scan.warehouse import Warehouse
        warehouse = Warehouse(self.warehouse_yml)

        self._create_soda_server_client()
        
        return Scan(warehouse=warehouse,
                    scan_yml=self.scan_yml,
                    variables=self.variables,
                    soda_server_client=self.soda_server_client,
                    time=self.time)

    def _build_warehouse_yml(self):
        if not self.warehouse_yml_file and not self.warehouse_yml_dict and not self.warehouse_yml:
            logging.error(f'No warehouse specified')
            return

        elif self.warehouse_yml_file and not self.warehouse_yml_dict and not self.warehouse_yml:
            if not isinstance(self.warehouse_yml_file, str):
                logging.error(f'scan_builder.warehouse_yml_file must be str, but was {type(self.warehouse_yml_file)}: {self.warehouse_yml_file}')
            else:
                self.warehouse_yml_dict = read_warehouse_yml_file(self.warehouse_yml_file)

        if self.warehouse_yml_dict and not self.warehouse_yml:
            from sodasql.scan.warehouse_yml_parser import WarehouseYmlParser
            warehouse_parser = WarehouseYmlParser(self.warehouse_yml_dict, self.warehouse_yml_file)
            warehouse_parser.log()
            self.parsers.append(warehouse_parser)
            self.warehouse_yml = warehouse_parser.warehouse_yml

    def _build_scan_yml(self):
        file_system = FileSystemSingleton.INSTANCE

        if not self.scan_yml_file and not self.scan_yml_dict and not self.scan_yml:
            logging.error(f'No scan specified')
            return

        elif self.scan_yml_file and not self.scan_yml_dict and not self.scan_yml:
            if not isinstance(self.scan_yml_file, str):
                logging.error(f'scan_builder.scan_yml_file must be str, but was {type(self.scan_yml_file)}: {self.scan_yml_file}')
            elif file_system.is_readable_file(self.scan_yml_file):
                scan_yml_str = self.file_system.file_read_as_str(self.scan_yml_file)
                if scan_yml_str:
                    self.scan_yml_dict = YamlHelper.parse_yaml(scan_yml_str, self.scan_yml_file)
                else:
                    logging.error(f'Failed to file scan yaml file: {self.scan_yml_file}')

        if self.scan_yml_dict and not self.scan_yml:
            from sodasql.scan.scan_yml_parser import ScanYmlParser
            scan_yml_parser = ScanYmlParser(self.scan_yml_dict, self.scan_yml_file)
            scan_yml_parser.log()
            self.parsers.append(scan_yml_parser)
            self.scan_yml = scan_yml_parser.scan_yml

    def _create_soda_server_client(self):
        if not self.soda_server_client:
            if self.warehouse_yml.soda_api_key_id and self.warehouse_yml.soda_api_key_secret:
                host = self.warehouse_yml.soda_host
                api_key_id = self.warehouse_yml.soda_api_key_id
                api_key_secret = self.warehouse_yml.soda_api_key_secret
                port = str(self.warehouse_yml.soda_port)
                protocol = self.warehouse_yml.soda_protocol
            else:
                host = os.getenv('SODA_HOST', 'cloud.soda.io')
                api_key_id = os.getenv('SODA_SERVER_API_KEY_ID', None)
                api_key_secret = os.getenv('SODA_SERVER_API_KEY_SECRET', None)
                port = os.getenv('SODA_PORT', '443')
                protocol = os.getenv('SODA_PROTOCOL', 'https')

            if api_key_id and api_key_secret:
                self.soda_server_client = SodaServerClient(host,
                                                           api_key_id=api_key_id,
                                                           api_key_secret=api_key_secret,
                                                           protocol=protocol,
                                                           port=port)
            else:
                logging.debug("No Soda Cloud account configured")
