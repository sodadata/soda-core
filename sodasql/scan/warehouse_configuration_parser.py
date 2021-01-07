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
import os
from pathlib import Path
from typing import Optional

from sodasql.scan.dialect import Dialect
from sodasql.scan.env_vars import EnvVars
from sodasql.scan.parser import Parser
from sodasql.scan.warehouse_configuration import WarehouseConfiguration

DEFAULT_WAREHOUSE_FILE_NAME = 'warehouse.yml'

KEY_NAME = 'name'
KEY_CONNECTION = 'connection'
KEY_SODA_ACCOUNT = 'soda_account'

VALID_PROJECT_KEYS = [KEY_NAME, KEY_CONNECTION, KEY_SODA_ACCOUNT]


class WarehouseConfigurationParser(Parser):
    """
    Parses warehouse.yml files
    """

    def __init__(self,
                 warehouse_dir: Optional[str] = None,
                 env: Optional[str] = None,
                 warehouse_yaml_file_path: Optional[str] = None,
                 connection_dict: Optional[str] = None):
        super().__init__(description=warehouse_yaml_file_path
                         if warehouse_yaml_file_path
                         else DEFAULT_WAREHOUSE_FILE_NAME)

        if connection_dict is None:
            if warehouse_yaml_file_path is None:
                if warehouse_dir is not None:
                    if isinstance(warehouse_dir, str):
                        warehouse_yaml_file_path = os.path.join(warehouse_dir, DEFAULT_WAREHOUSE_FILE_NAME)
                        if not Path(warehouse_yaml_file_path).exists():
                            self.error(f'{warehouse_yaml_file_path} does not exist')
                        elif not Path(warehouse_yaml_file_path).is_file():
                            self.error(f'{warehouse_yaml_file_path} is not a file')
                    else:
                        self.error(f'warehouse_dir is not a string: {str(type(warehouse_dir))}')

            warehouse_yaml_str = None
            if isinstance(warehouse_yaml_file_path, str):
                warehouse_yaml_str = self._read_file_as_string(warehouse_yaml_file_path)

            if isinstance(warehouse_yaml_str, str):
                connection_dict = self._parse_yaml_str(warehouse_yaml_str)

        if isinstance(connection_dict, dict):
            self._push_context(object=connection_dict, name=self.description)

            self.warehouse_configuration = WarehouseConfiguration()
            self.warehouse_configuration.name = self.get_str_required(KEY_NAME)

            EnvVars.load_env_vars(self.warehouse_configuration.name)

            connection_dict = self.get_dict_required(KEY_CONNECTION)
            self._push_context(object=connection_dict, name=KEY_CONNECTION)
            self.warehouse_configuration.dialect = Dialect.create(self)
            self._pop_context()

            self.check_invalid_keys(VALID_PROJECT_KEYS)

        else:
            self.error('No warehouse configuration provided')
