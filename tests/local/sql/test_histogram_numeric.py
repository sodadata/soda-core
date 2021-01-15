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


class TestHistogramNumeric(SqlTestCase):

    table_name = 'test_table'

    def test_scan_histogram_numeric(self):
        self.sql_create_table(
            self.table_name,
            [f"size {self.warehouse.dialect.integer_column_type}"],
            ["(1)",
             "(11)",
             "(11)",
             "(11)",
             "(11)",
             "(16)",
             "(17)",
             "(18)",
             "(20)",
             "(20)",
             "(null)"])

        scan_result = self.scan({
            'table_name': self.table_name,
            'metrics': [
                Metric.HISTOGRAM
            ]
        })

        histogram = scan_result.get(Metric.HISTOGRAM, 'size')

        self.assertEqual(histogram['frequencies'],
            [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 1, 1, 1, 0, 2])

        self.assertEqual(histogram['boundaries'],
            [1.0, 1.95, 2.9, 3.85, 4.8, 5.75, 6.7, 7.65, 8.6, 9.55, 10.5,
             11.45, 12.4, 13.35, 14.3, 15.25, 16.2, 17.15, 18.1, 19.05, 20.0])
