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

from typing import Optional, AnyStr


class Measurement:

    def __init__(self, metric: AnyStr, column_name: Optional[AnyStr] = None, value=None, group_values: Optional[dict] = None):
        self.metric = metric
        self.column_name = column_name
        self.value = value
        self.group_values = group_values

    def __str__(self):
        return self.metric + \
               (f'({self.column_name})' if self.column_name else '') + \
               ('' if self.value is None else ' = '+(', '.join([str(e) for e in self.value]) if isinstance(self.value, list) else str(self.value)))

    def to_json(self):
        if self.column_name is not None:
            return {
                'metric': self.metric,
                'columnName': self.column_name,
                'value': self.value
            }
        else:
            return {
                'metric': self.metric,
                'value': self.value
            }
