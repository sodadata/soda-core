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


class TestMinsMaxs(SqlTestCase):

    table_name = 'test_table'

    def test_scan_mins_maxs(self):
        self.sql_create_table(
            self.table_name,
            ["name VARCHAR(255)",
             "size INTEGER",
             "width VARCHAR(255)"],
            ["('one',    1,    '11')",
             "('two',    2,    '12')",
             "('three',  3,    '13')",
             "('four',   4,    '14')",
             "('five',   5,    '15')",
             "('six',    6,    '16')",
             "('seven',  7,    '17')",
             "('eight',  8,    '18')",
             "('ten',    9,    '19')",
             "('three',  10,   '20')",
             "(null,     null, null)"])

        scan_result = self.scan({
            'table_name': self.table_name,
            'metrics': [
                Metric.MINS,
                Metric.MAXS
            ],
            'columns': {
                'width': {
                    'valid_format': 'number_whole'
                }
            },
            # default mins_maxs is 20
            'mins_maxs_limit': 7

        })

        self.assertIsNone(scan_result.find_measurement(Metric.MINS, 'name'))
        self.assertIsNone(scan_result.find_measurement(Metric.MAXS, 'name'))

        self.assertEqual(scan_result.get(Metric.MINS, 'size'),
                         [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(scan_result.get(Metric.MAXS, 'size'),
                         [10, 9, 8, 7, 6, 5, 4])

        self.assertEqual(scan_result.get(Metric.MINS, 'width'),
                         ['11', '12', '13', '14', '15', '16', '17'])
        self.assertEqual(scan_result.get(Metric.MAXS, 'width'),
                         ['20', '19', '18', '17', '16', '15', '14'])
