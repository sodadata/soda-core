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
from typing import Optional, AnyStr

from sodasql.scan.dialect import Dialect
from sodasql.scan.env_vars import EnvVars
from sodasql.scan.parser import Parser
from sodasql.scan.warehouse_yml import WarehouseYml

KEY_NAME = 'name'
KEY_CONNECTION = 'connection'
KEY_SODA_ACCOUNT = 'soda_account'

VALID_WAREHOUSE_KEYS = [KEY_NAME, KEY_CONNECTION, KEY_SODA_ACCOUNT]


class WarehouseYmlParser(Parser):
    """
    Parses warehouse.yml files
    """

    def __init__(self,
                 warehouse_dict: dict,
                 warehouse_yaml_file: AnyStr):
        super().__init__(description=warehouse_yaml_file)

        if isinstance(warehouse_dict, dict):
            self._push_context(object=warehouse_dict, name=self.description)

            self.warehouse_yml = WarehouseYml()
            self.warehouse_yml.name = self.get_str_required(KEY_NAME)

            EnvVars.load_env_vars(self.warehouse_yml.name)

            connection_dict = self.get_dict_required(KEY_CONNECTION)
            self._push_context(object=connection_dict, name=KEY_CONNECTION)
            self.warehouse_yml.dialect = Dialect.create(self)
            self._pop_context()

            self.check_invalid_keys(VALID_WAREHOUSE_KEYS)

        else:
            self.error('No warehouse configuration provided')
