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

from typing import Optional

from sodasql.scan.column import Column


class Measurement:

    TYPE_SCHEMA = 'schema'
    TYPE_ROW_COUNT = 'row_count'
    TYPE_MISSING_COUNT = 'missing_count'
    TYPE_INVALID_COUNT = 'invalid_count'
    TYPE_MIN_LENGTH = 'min_length'

    def __init__(self, type: str, column: Optional[Column] = None, value=None):
        self.type = type
        self.column = column
        self.value = value

    def __str__(self):
        return self.type + \
               (f'({self.column.name})' if self.column else '') + \
               (f' -> {self.value}' if self.value is not None else '')
