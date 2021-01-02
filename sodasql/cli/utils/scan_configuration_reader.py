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
import yaml
import os

from sodasql.scan.parse_logs import ParseLogs
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.cli.utils import BaseReader


class ScanConfigurationReader(BaseReader):

    def __init__(self, warehouse_name, table_name,
                 configuration_folder=BaseReader.BASE_CONFIGURATION_FOLDER):
        self.warehouse_name = warehouse_name
        self.table_name = table_name
        self.configuration_folder = configuration_folder
        self.configuration = None
        self.__load_configuration()
        self.parse_logs: ParseLogs = self.configuration.parse_logs

    def __load_configuration(self):
        configuration_file_path = os.path.join(self.configuration_folder,
                                               self.warehouse_name,
                                               self.table_name,
                                               'scan.yml')
        logging.debug(f'Loading scan configuration file {configuration_file_path}')
        with open(configuration_file_path) as f:
            configuration_dict = yaml.load(f, Loader=yaml.FullLoader)
            configuration_dict['table_name'] = self.table_name
            self.configuration = ScanConfiguration(configuration_dict)
