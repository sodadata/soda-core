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

from sodasql.common.yaml_helper import YamlHelper
from sodasql.scan.dialect import Dialect
from sodasql.scan.env_vars import EnvVars
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.parser import Parser
from sodasql.scan.warehouse_yml import WarehouseYml

KEY_NAME = 'name'
KEY_CONNECTION = 'connection'
KEY_SODA_ACCOUNT = 'soda_account'

SODA_KEY_HOST = 'host'
SODA_KEY_PORT = 'port'
SODA_KEY_PROTOCOL = 'protocol'
SODA_KEY_API_KEY_ID = 'api_key_id'
SODA_KEY_API_KEY_SECRET = 'api_key_secret'

VALID_WAREHOUSE_KEYS = [KEY_NAME, KEY_CONNECTION, KEY_SODA_ACCOUNT]


def read_warehouse_yml_file(warehouse_yml_file: str):
    file_system = FileSystemSingleton.INSTANCE
    if file_system.is_readable_file(warehouse_yml_file):
        warehouse_yaml_str = file_system.file_read_as_str(warehouse_yml_file)
        if warehouse_yaml_str:
            return YamlHelper.parse_yaml(warehouse_yaml_str, warehouse_yml_file)
        else:
            logging.error(f'Failed to read warehouse yaml file: {warehouse_yml_file}')


class WarehouseYmlParser(Parser):
    """
    Parses warehouse.yml files
    """

    def __init__(self,
                 warehouse_yml_dict: dict,
                 warehouse_yml_path: str = 'warehouse-dict'):
        super().__init__(description=warehouse_yml_path)

        if isinstance(warehouse_yml_dict, dict):
            self._push_context(object=warehouse_yml_dict, name=self.description)

            self.warehouse_yml = WarehouseYml()
            self.warehouse_yml.name = self.get_str_required(KEY_NAME)

            EnvVars.load_env_vars(self.warehouse_yml.name)

            connection_dict = self.get_dict_required(KEY_CONNECTION)
            if connection_dict:
                self._push_context(object=connection_dict, name=KEY_CONNECTION)
                self.warehouse_yml.dialect = Dialect.create(self)
                self._pop_context()

            soda_account_dict = self.get_dict_optional(KEY_SODA_ACCOUNT)
            if soda_account_dict:
                self._push_context(object=soda_account_dict, name=KEY_SODA_ACCOUNT)
                self.warehouse_yml.soda_host = self.get_str_optional(SODA_KEY_HOST, 'cloud.soda.io')
                self.warehouse_yml.soda_port = self.get_int_optional(SODA_KEY_PORT, 443)
                self.warehouse_yml.soda_protocol = self.get_str_optional(SODA_KEY_PROTOCOL, 'https')
                self.warehouse_yml.soda_api_key_id = self.get_str_required_env(SODA_KEY_API_KEY_ID)
                self.warehouse_yml.soda_api_key_secret = self.get_str_required_env(SODA_KEY_API_KEY_SECRET)
                self._pop_context()

            self.check_invalid_keys(VALID_WAREHOUSE_KEYS)

        else:
            self.error('No warehouse configuration provided')
