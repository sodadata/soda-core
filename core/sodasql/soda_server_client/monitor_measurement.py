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

from sodasql.scan.group_value import GroupValue
from sodasql.scan.measurement import Measurement


@dataclass
class MonitorMeasurement(Measurement):
    metric_id: str = None
    sql: str = None
    query_milliseconds: int = None

    @classmethod
    def from_json(cls, json: dict):
        assert isinstance(json, dict)
        return MonitorMeasurement(
            metric_id=json.get('metricId'),
            metric=json.get('metricType'),
            sql=json.get('sql'),
            column_name=json.get('columnName'),
            value=json.get('value'),
            group_values=GroupValue.from_json_list(json.get('groupValues')),
            query_milliseconds=json.get('queryMilliseconds'))

    def to_json(self):
        json = super().to_json()
        json['metricId'] = self.metric_id
        json['sql'] = self.sql
        json['queryMilliseconds'] = self.query_milliseconds
        return json
