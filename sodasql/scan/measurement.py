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
from dataclasses import dataclass
from typing import Optional, List

from sodasql.common.json_helper import JsonHelper
from sodasql.scan.group_value import GroupValue


@dataclass
class Measurement:

    metric: str
    column_name: Optional[str] = None
    value: object = None
    group_values: Optional[List[GroupValue]] = None

    def __str__(self):
        column_str = f'({self.column_name})' if self.column_name else ''

        group_values_str = ''
        if self.group_values is not None:
            if len(self.group_values) == 0:
                return f'{self.metric}{column_str}: no groups'
            else:
                values_str = '\n  '.join([f'group{JsonHelper.to_jsonnable(group_value.group)} = {group_value.value}'
                                          for group_value in self.group_values])
                return f'{self.metric}{column_str}: \n  {values_str}'
        else:
            return f'{self.metric}{column_str} = {self.value}'

    def to_json(self):
        json = {
            'metric': self.metric,
        }

        if self.group_values is None:
            json['value'] = JsonHelper.to_jsonnable(self.value)
        else:
            json['groupValues'] = [group_value.to_json() for group_value in self.group_values]

        if self.column_name is not None:
            json['columnName'] = self.column_name

        return json
