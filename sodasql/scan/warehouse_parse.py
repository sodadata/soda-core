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

import yaml
from jinja2 import Template

from sodasql.scan.dialect import Dialect, POSTGRES, SNOWFLAKE, REDSHIFT, BIGQUERY, ATHENA
from sodasql.scan.parse_logs import ParseLogs, ParseConfiguration
from sodasql.scan.warehouse_configuration import WarehouseConfiguration


class WarehouseParse:

    DEFAULT_PROFILES_PATH = f'{str(Path.home())}/.soda/profiles.yml'

    def __init__(self,
                 profile: str,
                 target: str = None,
                 profiles_yml_path: str = DEFAULT_PROFILES_PATH,
                 profiles_yaml_str: Optional[str] = None,
                 profiles_dict: Optional[str] = None,
                 warehouse_dict: Optional[dict] = None):
        self.parse_logs = ParseLogs('Profiles')

        warehouse_dict = self._get_warehouse_dict(profile,
                                                  target,
                                                  profiles_yml_path,
                                                  profiles_yaml_str,
                                                  profiles_dict,
                                                  warehouse_dict)

        self.warehouse_configuration = WarehouseConfiguration()
        self.warehouse_configuration.properties = warehouse_dict
        self.warehouse_configuration.name = profile
        self.warehouse_configuration.dialect = Dialect.create(warehouse_dict, self.parse_logs)

    def _get_warehouse_dict(self, profile, target, profiles_yml_path, profiles_yaml_str, profiles_dict, warehouse_dict):
        if warehouse_dict is None:
            if profiles_dict is None:
                if profiles_yaml_str is None:
                    if profiles_yml_path is None:
                        self.parse_logs.error('No warehouse configuration')
                    elif isinstance(profiles_yml_path, str):
                        profiles_yaml_str = self._read_yaml_string_from_file(profiles_yml_path)
                    else:
                        self.parse_logs.error(f'profiles_yml_path not a str: {str(profiles_yml_path)}')

                if isinstance(profiles_yaml_str, str):
                    profiles_dict = self._read_dict_from_yaml_string(profiles_yaml_str)
                else:
                    self.parse_logs.error(f'profiles_yaml_str not a str: {str(profiles_yaml_str)}')

            if isinstance(profiles_dict, dict) \
                    and isinstance(profile, str)\
                    and (target is None or isinstance(target, str)):
                warehouse_dict = self._get_profile_dict(profiles_dict, profile, target)
            else:
                self.parse_logs.error(f'Invalid profiles config: {str(profile)}, {str(target)}, {str(profiles_dict)}')
        return warehouse_dict

    def _read_yaml_string_from_file(self, profiles_yml_path: str) -> str:
        try:
            with open(profiles_yml_path) as f:
                return f.read()
        except Exception as e:
            self.parse_logs.error(f'Error reading profiles file {profiles_yml_path}: {str(e)}')

    def _read_dict_from_yaml_string(self, profile_yaml_str: str) -> dict:
        def env_var(key: str, default=None):
            return os.getenv(key, default)

        template = Template(profile_yaml_str)
        profiles_yaml_str = template.render(env_var=env_var)
        try:
            return yaml.load(profiles_yaml_str, Loader=yaml.FullLoader)
        except Exception as e:
            self.parse_logs.error(f'Error reading profiles string: {str(e)}')

    def _get_profile_dict(self, profiles_dict: dict, profile: str, target: str = None) -> dict:
        profiles_cfg = ParseConfiguration(profiles_dict, 'Profiles', self.parse_logs)
        profile_dict = profiles_cfg.get_dict_required(profile)
        if profile_dict:
            profile_cfg = ParseConfiguration(profile_dict, f'profile {profile}', self.parse_logs)
            outputs_dict = profile_cfg.get_dict_required('outputs')
            if outputs_dict:
                if target is None:
                    target = profile_cfg.get_str_required('target')
                if target is not None:
                    outputs_cfg = ParseConfiguration(outputs_dict, f'outputs {profile}', self.parse_logs)
                    return outputs_cfg.get_dict_required(target)
