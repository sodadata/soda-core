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
import decimal
from unittest import TestCase

from sodasql.common.json_helper import JsonHelper
from sodasql.scan.group_value import GroupValue
from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric


class TestMeasurementStr(TestCase):

    def test_row_count(self):
        measurement = Measurement(metric=Metric.ROW_COUNT, value=5)
        self.assertEqual('row_count = 5', str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

    def test_column_measurement(self):
        measurement = Measurement(metric=Metric.MIN, column_name='AGE', value=3.4)
        self.assertEqual('min(AGE) = 3.4', str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

    def test_column_measurement_list_value(self):
        measurement = Measurement(metric=Metric.MINS, column_name='chars', value=['a', 'b'])
        self.assertEqual("mins(chars) = ['a', 'b']", str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

    def test_column_group_measurement(self):
        measurement = Measurement(metric=Metric.MIN,
                                  column_name='AGE',
                                  group_values=[GroupValue(group={'country': 'US'}, value=3.4)])
        self.assertEqual("min(AGE): \n  group{'country': 'US'} = 3.4", str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

    def test_column_group_measurement_decimal(self):
        measurement = Measurement(metric=Metric.MIN,
                                  column_name='AGE',
                                  group_values=[GroupValue(group={'country': 'US'}, value=decimal.Decimal(4.5))])
        self.assertEqual("min(AGE): \n  group{'country': 'US'} = 4.5", str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

    def test_column_group_measurement_decimal_value(self):
        measurement = Measurement(metric=Metric.MIN,
                                  column_name='AGE',
                                  group_values=[GroupValue(group={'country': 'US'}, value=decimal.Decimal(3))])
        self.assertEqual("min(AGE): \n  group{'country': 'US'} = 3", str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

    def test_column_group_measurement_empty_list(self):
        measurement = Measurement(metric=Metric.MIN,
                                  column_name='AGE',
                                  group_values=[])
        self.assertEqual('min(AGE): no groups', str(measurement))
        JsonHelper.to_json(JsonHelper.to_jsonnable(measurement.to_json()))

