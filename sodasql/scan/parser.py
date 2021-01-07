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
import json
import logging
import os
from collections import deque
from dataclasses import dataclass
from typing import List, Deque

import yaml

from sodasql.scan.aws_credentials import AwsCredentials

ERROR = 'error'
WARNING = 'warning'
INFO = 'info'


class ParseLog:
    def __init__(self, level: str, message: str):
        self.level = level
        self.message = message

    def __str__(self):
        return f'[{self.level}] {self.message}'

    def log(self):
        if self.level == ERROR:
            logging.error(self.message)
        elif self.level == WARNING:
            logging.warning(self.message)
        elif self.level == INFO:
            logging.info(self.message)
        else:
            logging.debug(self.message)


@dataclass
class ParseContext:
    name: str
    object: dict


class Parser:

    def __init__(self, description: str):
        self.description: str = description
        self.contexts: Deque[ParseContext] = deque()
        self.logs: List[ParseLog] = []

    def __str__(self):
        return '\n'.join([str(log) for log in self.logs])

    def _read_file_as_string(self, file_path: str):
        try:
            with open(file_path) as f:
                return f.read()
        except Exception as e:
            self.error(f"Couldn't read file {file_path}: {str(e)}")

    def _parse_yaml_str(self, yaml_str):
        try:
            return yaml.load(yaml_str, Loader=yaml.FullLoader)
        except Exception as e:
            self.error(f"Couldn't parse yaml in {self.description}: {str(e)}")

    def _push_context(self, object=None, name: str = None):
        self.contexts.append(ParseContext(object=object, name=name))

    def _pop_context(self):
        return self.contexts.pop()

    def _get_context_description(self):
        return '.'.join([context.name for context in self.contexts if context.name])

    def error(self, message: str):
        return self.logs.append(ParseLog(ERROR, message))

    def warning(self, message: str):
        return self.logs.append(ParseLog(WARNING, message))

    def info(self, message):
        return self.logs.append(ParseLog(INFO, message))

    def log(self):
        for log in self.logs:
            log.log()

    def check_invalid_keys(self, valid_keys: List[str]):
        """
        Adds a warning for all invalid configured property names
        """
        context_iterable = self._get_current_context_object()
        for invalid_key in [configured_key for configured_key in context_iterable if configured_key not in valid_keys]:
            self.warning(f'Invalid key in {self._get_context_description()} : {invalid_key}')

    def has_warnings_or_errors(self):
        for log in self.logs:
            if log.level == ERROR or log.level == WARNING:
                return True
        return False

    def assert_no_warnings_or_errors(self):
        if self.has_warnings_or_errors():
            raise AssertionError(f'{self.description} configuration errors: \n  '
                                 + ('\n  '.join([str(log) for log in self.logs])))

    def get_str_required(self, property_name: str):
        return self._get(property_name, str, True)

    def get_str_optional(self, property_name: str, default=None):
        return self._get(property_name, str, False, default)

    def get_str_required_env(self, property_name: str):
        return self._get(property_name, str, True, resolve_env=True)

    def get_credential(self, property_name: str):
        return self._get(property_name, str, False, resolve_env=True, env_required=True)

    def get_str_optional_env(self, property_name: str, default=None):
        return self._get(property_name, str, False, default, resolve_env=True)

    def get_int_required(self, property_name: str):
        return self._get(property_name, int, True)

    def get_int_optional(self, property_name: str, default=None):
        return self._get(property_name, int, False, default)

    def get_float_required(self, property_name: str):
        return self._get(property_name, float, True)

    def get_float_optional(self, property_name: str, default=None):
        return self._get(property_name, float, False, default)

    def get_dict_required(self, property_name: str):
        return self._get(property_name, dict, True)

    def get_dict_optional(self, property_name: str, default=None):
        return self._get(property_name, dict, False, default)

    def get_list_required(self, property_name: str):
        return self._get(property_name, list, True)

    def get_list_optional(self, property_name: str, default=None):
        return self._get(property_name, list, False, default)

    def get_aws_credentials_optional(self):
        access_key_id = self.get_str_optional_env('access_key_id')
        if access_key_id:
            return AwsCredentials(
                access_key_id=access_key_id,
                secret_access_key=self.get_credential('secret_access_key'),
                role_arn=self.get_str_optional_env('role_arn'),
                session_token=self.get_credential('session_token'),
                region_name=self.get_str_optional_env('region', 'eu-west-1'))

    def get_file_json_dict_required(self, property_name: str):
        file_str: str = self._read_file_as_string(property_name)
        try:
            return json.loads(file_str)
        except Exception as e:
            self.error(f"Couldn't parse json configuration {property_name} for {self.description}: {str(e)}")

    def _get(self,
             property_name: str,
             return_type: type,
             is_required: bool,
             default=None,
             resolve_env: bool = False,
             env_required: bool = False):
        properties = self._get_current_context_object()
        if property_name in properties:
            value = properties.get(property_name)

            if resolve_env:
                if isinstance(value, str) \
                        and value.strip().startswith('env_var(') \
                        and value.strip().endswith(')'):
                    env_var_name = value.strip()[len('env_var('):-1]
                    value = os.getenv(env_var_name)
                    if not value:
                        self.info(f'Environment variable {env_var_name} is not set')
                elif env_required:
                    self.error(f'{self._get_context_description()}.{property_name} is considered a credential and '
                               f'must be passed as an environment variable eg: env_var(YOUR_ENVIRONMENT_VAR)')

            if value is None or isinstance(value, return_type):
                return value
            try:
                if return_type == int:
                    return int(value)
                if return_type == str:
                    return str(value)
                if return_type == float:
                    return float(value)
                raise ValueError(str(type(value)))
            except ValueError as e:
                self.error(f'Invalid {property_name}: Expected {str(return_type)}, but was {str(value)}: {str(e)}')
                return None

        else:
            if is_required:
                self.error(f'Property {property_name} does not exist in {self._get_context_description()}')
            return default

    def _get_current_context_object(self):
        return self.contexts[-1].object
