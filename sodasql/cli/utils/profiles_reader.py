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
from pathlib import Path
import yaml
import os

from jinja2 import Template

from sodasql.scan.parse_logs import ParseLogs
from . import BaseReader


class ProfilesReader(BaseReader):

    PROFILES_FILE_PATH = os.path.join(BaseReader.BASE_CONFIGURATION_FOLDER, 'profiles.yml')

    def __init__(self, profile_name: str = None, target: str = None):
        self.parse_logs: ParseLogs = ParseLogs()
        self.configuration: dict = {}
        profile_name = profile_name if profile_name else 'default'

        self.__load_profiles_yaml_dict()
        if self.profiles_yaml_dict:
            profile = self.__get(self.profiles_yaml_dict, self.PROFILES_FILE_PATH, profile_name, dict)
            if profile:
                outputs = self.__get(profile, f'profile {profile_name}', 'outputs', dict)
                if outputs:
                    if target is None:
                        target = self.__get(profile, f'profile {profile_name}', 'target', str)
                    if target is not None:
                        self.configuration = self.__get(outputs, f'outputs.{target}', target, dict)

    def __load_profiles_yaml_dict(self):
        try:
            with open(self.PROFILES_FILE_PATH) as f:
                profiles_yaml_str = f.read()
                template = Template(profiles_yaml_str)

                def env_var(key: str, default=None):
                    return os.getenv(key, default)

                profiles_yaml_str = template.render(env_var=env_var)
                self.profiles_yaml_dict = yaml.load(profiles_yaml_str, Loader=yaml.FullLoader)
        except Exception as e:
            self.parse_logs.error(str(e))
            self.profiles_yaml_dict = None

    def __get(self, configuration: dict, configuration_description: str, property_name: str, property_type):
        value = configuration.get(property_name)
        if value is None:
            self.parse_logs.error(f'Property {property_name} does not exist in {configuration_description}')
            return None
        elif not isinstance(value, property_type):
            self.parse_logs.error(f'Expected {str(property_type)} for property {property_name} in '
                                  f'{configuration_description}, but was {str(type(value))}')
            return None
        return value
