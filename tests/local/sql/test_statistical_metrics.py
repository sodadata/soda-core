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


class TestStatisticalMetrics(SqlTestCase):

    def test_scan_statistical_metrics(self):
        self.sql_create_table(
            self.test_table_name,
            [f"score {self.warehouse.dialect.string_column_type}"],
            ["('1')",
             "('2')",
             "('5')",
             "('12')",
             "(null)"])

        scan_result = self.scan({
            'table_name': self.test_table_name,
            'metrics': [
                Metric.MIN,
                Metric.MAX,
                Metric.AVG,
                Metric.SUM,
                Metric.VARIANCE,
                Metric.STDDEV
            ],
            'columns': {
                'score': {
                  'valid_format': 'number_whole'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.MIN, 'score'), 1)
        self.assertEqual(scan_result.get(Metric.MAX, 'score'), 12)
        self.assertEqual(scan_result.get(Metric.AVG, 'score'), 5)
        self.assertEqual(scan_result.get(Metric.SUM, 'score'), 20)
        self.assertMeasurementsPresent(scan_result, 'score', [
            Metric.VARIANCE,
            Metric.STDDEV
        ])

    def test_no_minmax_for_non_numeric_strings(self):
        self.sql_create_table(
            self.test_table_name,
            [f"score {self.warehouse.dialect.string_column_type}"],
            ["('1')",
             "('2')",
             "('5')",
             "('12')",
             "(null)"])

        scan_result = self.scan({
            'table_name': self.test_table_name,
            'metrics': [
                Metric.MIN,
                Metric.MAX,
                Metric.MINS,
                Metric.MAXS,
                Metric.FREQUENT_VALUES
            ]
        })

        self.assertMeasurementsAbsent(scan_result, 'score', [
            Metric.MIN,
            Metric.MAX,
            Metric.MINS,
            Metric.MAXS,
            Metric.FREQUENT_VALUES
        ])
