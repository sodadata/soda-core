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
from deprecated import deprecated
from sodasql.scan.group_value import GroupValue
from sodasql.scan.measurement import Measurement


@dataclass
class MonitorMeasurement(Measurement):
    metric_id: str = None
    sql: str = None
    query_milliseconds: int = None

    @classmethod
    def from_dict(cls, dictionary: dict) -> 'MonitorMeasurement':
        assert isinstance(dictionary, dict)
        return MonitorMeasurement(
            metric_id=dictionary.get('metricId'),
            metric=dictionary.get('metricType'),
            sql=dictionary.get('sql'),
            column_name=dictionary.get('columnName'),
            value=dictionary.get('value'),
            group_values=GroupValue.from_json_list(dictionary.get('groupValues')),
            query_milliseconds=dictionary.get('queryMilliseconds'))

    @classmethod
    @deprecated(version='2.1.0b19', reason='This function is deprecated, please use to_dict')
    def from_json(cls, dictionary: dict):
        cls.from_dict(cls, dictionary)

    def to_dict(self) -> dict:
        dictionary = super().to_dict()
        dictionary['metricId'] = self.metric_id
        dictionary['sql'] = self.sql
        dictionary['queryMilliseconds'] = self.query_milliseconds
        return dictionary

    @deprecated(version='2.1.0b19', reason='This function is deprecated, please use to_dict')
    def to_json(self):
        return self.to_dict()
