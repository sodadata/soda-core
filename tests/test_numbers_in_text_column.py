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
from tests.abstract_scan_test import AbstractScanTest


class TestMissingAndInvalidCustomizations(AbstractScanTest):

    def test_scan_customized_missing_values(self):
        self.create_table(
            'test_table',
            ["score VARCHAR(255)"],
            ["('1')",
             "('2')",
             "('5')",
             "('12')",
             "(null)"])

        scan_result = self.scan({
          'table_name': 'test_table',
          'columns': {
            'score': {
              'metrics': [
                'valid',
                'min',
                'max',
                'avg',
                'sum'
              ],
              'valid_format': 'number_whole'
            }
          }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'score'), 4)
        self.assertEqual(scan_result.get(Metric.MIN, 'score'), 1)
        self.assertEqual(scan_result.get(Metric.MAX, 'score'), 12)
        self.assertEqual(scan_result.get(Metric.AVG, 'score'), 5)
        self.assertEqual(scan_result.get(Metric.SUM, 'score'), 20)
