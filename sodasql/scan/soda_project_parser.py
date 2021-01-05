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
from sodasql.scan.soda_project import SodaProject

DEFAULT_SODA_PROJECT_FILE_NAME = 'soda.yml'

KEY_NAME = 'name'
KEY_WAREHOUSE = 'warehouse'
KEY_SODA_ACCOUNT = 'soda_account'

VALID_PROJECT_KEYS = [KEY_NAME, KEY_WAREHOUSE, KEY_SODA_ACCOUNT]


class SodaProjectParser(Parser):
    """
    Parses soda.yaml project files
    """

    def __init__(self,
                 soda_project_dir: Optional[str] = None,
                 env: Optional[str] = None,
                 soda_project_yaml_file_path: Optional[str] = None,
                 soda_project_dict: Optional[str] = None):
        super().__init__(description=soda_project_yaml_file_path
                         if soda_project_yaml_file_path
                         else DEFAULT_SODA_PROJECT_FILE_NAME)

        if soda_project_dict is None:
            if soda_project_yaml_file_path is None:
                if soda_project_dir is not None:
                    if isinstance(soda_project_dir, str):
                        soda_project_yaml_file_path = os.path.join(soda_project_dir, DEFAULT_SODA_PROJECT_FILE_NAME)
                        if not Path(soda_project_yaml_file_path).exists():
                            self.error(f'{soda_project_yaml_file_path} does not exist')
                        elif not Path(soda_project_yaml_file_path).is_file():
                            self.error(f'{soda_project_yaml_file_path} is not a file')
                    else:
                        self.error(f'project_dir is not a string: {str(type(soda_project_dir))}')

            project_yaml_str = None
            if isinstance(soda_project_yaml_file_path, str):
                project_yaml_str = self._read_file_as_string(soda_project_yaml_file_path)

            if isinstance(project_yaml_str, str):
                soda_project_dict = self._parse_yaml_str(project_yaml_str)

        if isinstance(soda_project_dict, dict):
            self._push_context(object=soda_project_dict, name=self.description)

            self.soda_project = SodaProject()
            self.soda_project.name = self.get_str_required(KEY_NAME)

            EnvVars.load_env_vars(self.soda_project.name)

            warehouse_dict = self.get_dict_required(KEY_WAREHOUSE)
            self._push_context(object=warehouse_dict, name=KEY_WAREHOUSE)
            self.soda_project.dialect = Dialect.create(self)
            self._pop_context()

            self.check_invalid_keys(VALID_PROJECT_KEYS)

        else:
            self.error('No project configuration provided')
