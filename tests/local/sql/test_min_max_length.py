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

from sodasql.scan.metric import Metric
from tests.common.sql_test_case import SqlTestCase


class TestMinMaxLength(SqlTestCase):

    table_name = 'test_table'

    def test_scan_min_max_length(self):
        self.sql_create_table(
            self.table_name,
            ["name VARCHAR(255)",
             "size INTEGER"],
            ["('one',    1)",
             "('two',    2)",
             "('three',  3)",
             "(null,     null)"])

        scan_result = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'min_length',
            'max_length'
          ]
        })

        self.assertEqual(scan_result.get(Metric.MIN_LENGTH, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.MAX_LENGTH, 'name'), 5)

        self.assertIsNone(scan_result.find_measurement(Metric.MIN_LENGTH, 'size'))
        self.assertIsNone(scan_result.find_measurement(Metric.MAX_LENGTH, 'size'))

