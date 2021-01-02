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
from pathlib import Path
from typing import List

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


class ParseLogs:

    def __init__(self, description: str = None):
        self.description = description
        self.logs: List[ParseLog] = []

    def __str__(self):
        return '\n'.join([str(log) for log in self.logs])

    def error(self, message: str):
        return self.logs.append(ParseLog(ERROR, message))

    def warning(self, message: str):
        return self.logs.append(ParseLog(WARNING, message))

    def info(self, message):
        return self.logs.append(ParseLog(INFO, message))

    def log(self):
        for log in self.logs:
            log.log()

    def warning_invalid_elements(self, configured_values, valid_values, message):
        for invalid_key in [configured_key for configured_key in configured_values if configured_key not in valid_values]:
            self.warning(f'{message}: {invalid_key}')

    def has_warnings_or_errors(self):
        for log in self.logs:
            if log.level == ERROR or log.level == WARNING:
                return True
        return False

    def assert_no_warnings_or_errors(self):
        if self.has_warnings_or_errors():
            raise AssertionError(f'{self.description} configuration errors: \n  '
                                 + ('\n  '.join([str(log) for log in self.logs])))


class ParseConfiguration:

    def __init__(self, configuration: dict, description: str, parse_logs: ParseLogs):
        self.configuration: dict = configuration
        self.description: str = description
        self.parse_logs: ParseLogs = parse_logs

    def get_str_required(self, property_name: str):
        return self.get(property_name, str, True)

    def get_str_optional(self, property_name: str, default=None):
        return self.get(property_name, str, False, default)

    def get_int_required(self, property_name: str):
        return self.get(property_name, int, True)

    def get_int_optional(self, property_name: str, default=None):
        return self.get(property_name, int, False, default)

    def get_dict_required(self, property_name: str):
        return self.get(property_name, dict, True)

    def get_aws_credentials_optional(self):
        return AwsCredentials.from_configuration(self.configuration)

    def get(self, property_name: str, type: type, is_required: bool, default=None):
        if property_name in self.configuration:
            value = self.configuration.get(property_name)

            if value is None or isinstance(value, type):
                return value
            try:
                if type == int:
                    return int(value)
                if type == str:
                    return str(value)
                if type == float:
                    return float(value)
            except Exception as e:
                self.parse_logs.error(f'Invalid type for {property_name}: {str(type(value))}')
                return None

        else:
            if is_required:
                self.parse_logs.error(f'Property {property_name} does not exist in {self.description}')
            return default

    def get_file_str_required(self, property_name: str):
        file_path = self.get_str_required(property_name)

        if not os.path.isfile(file_path):
            user_home_file_path = os.path.join(Path.Home(), '.soda', file_path)
            if os.path.isfile(user_home_file_path):
                file_path = user_home_file_path

        if os.path.isfile(file_path):
            with open(file_path) as f:
                return f.read()
        else:
            self.parse_logs.error(f'File {property_name}={file_path} not found for {self.description}')

    def get_file_json_dict_required(self, property_name: str):
        file_str: str = self.get_file_str_required(property_name)
        try:
            return json.loads(file_str)
        except Exception as e:
            self.parse_logs.error(f"Couldn't parse json configuration {property_name} for {self.description}: {str(e)}")
            return None
