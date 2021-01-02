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

from sodasql.scan.parse_logs import ParseLogs


class ProfileParse:

    DEFAULT_PROFILES_PATH = f'{str(Path.home())}/.soda/profiles.yml'

    def __init__(self,
                 profile: str,
                 target: str = None,
                 profiles_yml_path: str = DEFAULT_PROFILES_PATH,
                 profile_yaml_str: Optional[str] = None,
                 profiles_dict: Optional[str] = None):
        self.parse_logs = ParseLogs('Profiles')

        if profile_yaml_str is None and profiles_dict is None:
            profile_yaml_str = self._read_yaml_string_from_file(profiles_yml_path)

        if profiles_dict is None and profile_yaml_str is not None:
            profiles_dict = self._read_dict_from_yaml_string(profile_yaml_str)

        if profiles_dict is not None:
            self.properties = self._read_properties_from_dict(profiles_dict, profile, target)

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

    def _read_properties_from_dict(self, profiles_dict: dict, profile: str, target: str = None) -> dict:
        profile_dict = self._get_property(profiles_dict, 'Profiles configuration', profile, dict)
        if profile_dict:
            outputs = self._get_property(profile_dict, f'profile {profile}', 'outputs', dict)
            if outputs:
                if target is None:
                    target = self._get_property(profile_dict, f'profile {profile}', 'target', str)
                if target is not None:
                    return self._get_property(outputs, f'outputs.{target}', target, dict)

    def _get_property(self,
                      configuration: dict,
                      configuration_description: str,
                      property_name: str,
                      property_type: type):
        value = configuration.get(property_name)
        if value is None:
            self.parse_logs.error(f'Property {property_name} does not exist in {configuration_description}')
            return None
        elif not isinstance(value, property_type):
            self.parse_logs.error(f'Expected {str(property_type)} for property {property_name} in '
                                  f'{configuration_description}, but was {str(type(value))}')
            return None
        return value
