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
from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_TESTS
from tests.common.sql_test_case import SqlTestCase
from decimal import *


class TestDefaultMetrics(SqlTestCase):

    def test_default_generated_metrics(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}",
             f"score_int {self.dialect.data_type_integer}"],
            ["('1', 1)",
             "('2', 2)",
             "('2', 3)",
             "('3', 4)",
             "('3', 5)",
             "('3', 6)",
             "('3', 6)",
             "('3', 7)",
             "('4', 8)",
             "('4', 9)",
             "('5', 1)",
             "(null, null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.ROW_COUNT,
                Metric.MISSING_COUNT,
                Metric.MISSING_PERCENTAGE,
                Metric.VALUES_COUNT,
                Metric.VALUES_PERCENTAGE,
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
                Metric.AVG_LENGTH,
                Metric.MAX_LENGTH,
                Metric.MIN_LENGTH,
                Metric.AVG,
                Metric.MAX,
                Metric.MIN,
                Metric.STDDEV,
                Metric.VARIANCE
            ]
        })

        with self.assertRaises(AssertionError):
            scan_result.get(Metric.SUM, 'score')
        with self.assertRaises(AssertionError):
            scan_result.get(Metric.SUM, 'score_int')

