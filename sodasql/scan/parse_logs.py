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

from typing import List


class ParseLog:
    def __init__(self, level: str, message: str):
        self.level = level
        self.message = message

    def __str__(self):
        return f'[{self.level}] {self.message}'


class ParseLogs:

    ERROR = 'error'
    WARNING = 'warning'

    def __init__(self):
        self.logs: List[ParseLog] = []

    def __str__(self):
        return '\n'.join([str(log) for log in self.logs])

    def error(self, message: str):
        return self.logs.append(ParseLog(ParseLogs.ERROR, message))

    def warning(self, message: str):
        return self.logs.append(ParseLog(ParseLogs.WARNING, message))

    def warning_invalid_elements(self, configured_values, valid_values, message):
        for invalid_key in [configured_key for configured_key in configured_values if configured_key not in valid_values]:
            self.warning(f'{message} : {invalid_key}')

