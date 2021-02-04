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
from typing import List


@dataclass
class GroupValue:

    group: List[str]
    value: object

    @classmethod
    def from_json(cls, json: dict):
        if json is None:
            return None
        assert isinstance(json, dict)
        return GroupValue(
            group=json.get('group'),
            value=json.get('value')
        )

    @classmethod
    def from_json_list(cls, json_list: list):
        if json_list is None:
            return None
        assert isinstance(json_list, list)
        group_values = []
        for json in json_list:
            group_value = cls.from_json(json)
            if group_value:
                group_values.append(group_value)
        return group_values

    def to_json(self):
        return {
            'group': self.group,
            'value': self.value
        }

