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
from typing import List


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

    def assert_no_warnings_or_errors(self, configuration_description: str = None):
        if self.has_warnings_or_errors():
            raise AssertionError(f'{self.description if self.description else configuration_description} configuration errors: \n  '
                                 + ('\n  '.join([str(log) for log in self.logs])))
