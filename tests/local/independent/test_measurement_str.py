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
from unittest import TestCase

from sodasql.scan.dialect import Dialect
from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric


class TestMeasurementStr(TestCase):

    def test_row_count(self):
        measurement_str = str(Measurement(metric=Metric.ROW_COUNT, value=5))
        self.assertEqual(measurement_str, 'row_count = 5')

    def test_column_measurement(self):
        measurement_str = str(Measurement(metric=Metric.MIN, column_name='AGE', value=3.4))
        self.assertEqual(measurement_str, 'min(AGE) = 3.4')

    def test_column_measurement_list_value(self):
        measurement_str = str(Measurement(metric=Metric.MINS, column_name='chars', value=['a','b']))
        self.assertEqual(measurement_str, 'mins(chars) = a, b')

    def test_column_group_measurement(self):
        measurement_str = str(Measurement(metric=Metric.MIN, column_name='AGE', value=3.4, group_values={'country': 'US'}))
        self.assertEqual(measurement_str, 'min(AGE){"country": "US"} = 3.4')

