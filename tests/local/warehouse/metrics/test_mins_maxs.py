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
from datetime import date

from sodasql.scan.metric import Metric
from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_COLUMNS
from tests.common.sql_test_case import SqlTestCase


class TestMinsMaxs(SqlTestCase):

    def test_scan_mins_maxs(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}",
             f"size {self.dialect.data_type_integer}",
             f"width {self.dialect.data_type_varchar_255}",
             f"date {self.dialect.data_type_date}"],
            ["('one',    1,    '11', DATE '2020-01-01')",
             "('two',    2,    '12', DATE '2020-01-01')",
             "('three',  3,    '13', DATE '2020-01-01')",
             "('four',   4,    '14', DATE '2020-01-02')",
             "('five',   5,    '15', DATE '2020-01-02')",
             "('six',    6,    '16', DATE '2020-01-02')",
             "('seven',  7,    '17', DATE '2020-01-03')",
             "('eight',  8,    '18', DATE '2020-01-03')",
             "('ten',    9,    '19', DATE '2020-01-03')",
             "('three',  10,   '20', DATE '2020-01-03')",
             "(null,     null, null, null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.MIN,
                Metric.MAX,
                Metric.MINS,
                Metric.MAXS
            ],
            KEY_COLUMNS: {
                'width': {
                    'valid_format': 'number_whole'
                }
            },
            # default mins_maxs is 20
            'mins_maxs_limit': 7

        })

        self.assertIsNone(scan_result.find_measurement(Metric.MIN, 'name'))
        self.assertIsNone(scan_result.find_measurement(Metric.MAX, 'name'))
        self.assertEqual(scan_result.get(Metric.MINS, 'name'),
                         ['eight', 'five', 'four', 'one', 'seven', 'six', 'ten'])
        self.assertEqual(scan_result.get(Metric.MAXS, 'name'),
                         ['two', 'three', 'ten', 'six', 'seven', 'one', 'four'])

        self.assertEqual(scan_result.find_measurement(Metric.MIN, 'size').value, 1)
        self.assertEqual(scan_result.find_measurement(Metric.MAX, 'size').value, 10)
        self.assertEqual(scan_result.get(Metric.MINS, 'size'),
                         [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(scan_result.get(Metric.MAXS, 'size'),
                         [10, 9, 8, 7, 6, 5, 4])

        self.assertEqual(scan_result.find_measurement(Metric.MIN, 'width').value, 11.0)
        self.assertEqual(scan_result.find_measurement(Metric.MAX, 'width').value, 20.0)
        self.assertEqual(scan_result.get(Metric.MINS, 'width'),
                         ['11', '12', '13', '14', '15', '16', '17'])
        self.assertEqual(scan_result.get(Metric.MAXS, 'width'),
                         ['20', '19', '18', '17', '16', '15', '14'])

        self.assertIsNone(scan_result.find_measurement(Metric.MIN, 'date'))
        self.assertIsNone(scan_result.find_measurement(Metric.MAX, 'date'))
        self.assertEqual(scan_result.get(Metric.MINS, 'date'),
                         [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)])
        self.assertEqual(scan_result.get(Metric.MAXS, 'date'),
                         [date(2020, 1, 3), date(2020, 1, 2), date(2020, 1, 1)])
