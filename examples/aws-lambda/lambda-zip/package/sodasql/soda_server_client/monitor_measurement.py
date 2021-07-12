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
