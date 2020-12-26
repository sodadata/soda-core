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


def read_scan_configuration(cfg_dir: str, warehouse_name: str, table_name: str):
    from sodasql.scan.scan_configuration import ScanConfiguration
    scan_file_name = f'{cfg_dir}/{warehouse_name}/{table_name}/scan.yaml'
    logging.debug(f'Reading {scan_file_name}')
    with open(scan_file_name) as f:
        scan_dict = yaml.load(f, Loader=yaml.FullLoader)
        scan_dict['warehouse_name'] = warehouse_name
        scan_dict['table_name'] = table_name
        return ScanConfiguration(scan_dict)


