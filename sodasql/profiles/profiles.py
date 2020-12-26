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

from sodasql.scan.parse_logs import ParseLogs


class Profile:

    USER_HOME_PROFILES_YAML_LOCATION = f'{str(Path.home())}/.soda/profiles.yml'

    # Caches the yaml dict in memory for when it is read multiple times (as in test suites)
    profile_yaml_dict: dict = None

    def __init__(self, profile_name: str = 'default', target: str = None):
        self.parse_logs: ParseLogs = ParseLogs()
        self.configuration: dict = {}

        profiles_yaml_dict = self.__read_profiles_yaml_dict()
        if profiles_yaml_dict:
            profile = self.__get(profiles_yaml_dict, Profile.USER_HOME_PROFILES_YAML_LOCATION, profile_name, dict)
            if profile:
                outputs = self.__get(profile, f'profile {profile_name}', 'outputs', dict)
                if outputs:
                    if target is None:
                        target = self.__get(profile, f'profile {profile_name}', 'target', dict)
                    if target is not None:
                        self.configuration = self.__get(outputs, f'outputs.{target}', target, dict)

    def __read_profiles_yaml_dict(self) -> dict:
        if Profile.profile_yaml_dict is None:
            try:
                with open(Profile.USER_HOME_PROFILES_YAML_LOCATION) as f:
                    Profile.profile_yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
            except Exception as e:
                self.parse_logs.error(str(e))
        return Profile.profile_yaml_dict

    def __get(self, configuration: dict, configuration_description: str, property_name: str, type):
        value = configuration.get(property_name)
        if value is None:
            self.parse_logs.error(f'Property {property_name} does not exist in {configuration_description}')
            return None
        elif not isinstance(value, type):
            self.parse_logs.error(f'Expected {str(type)} for property {property_name} '
                                  f'in {configuration_description}, but was {str(type(value))}')
            return None
        return value


