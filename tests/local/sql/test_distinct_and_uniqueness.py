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


class TestDistinctAndUniqueness(SqlTestCase):

    def test_distinct(self):
        self.sql_create_test_table(
            [self.warehouse.dialect.declare_string_column_sql("score")],
            ["('1')",
             "('2')",
             "('2')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('4')",
             "('4')",
             "('5')",
             "(null)"])

        scan_result = self.scan({
            'table_name': self.default_test_table_name,
            'metrics': [
                'distinct'
            ]
        })

        self.assertEqual(scan_result.get(Metric.DISTINCT, 'score'), 5)
        self.assertEqual(scan_result.get(Metric.UNIQUE_COUNT, 'score'), 2)

        # (5 - 1) * 100 / (10 valid values - 1)
        self.assertEqual(scan_result.get(Metric.UNIQUENESS, 'score'), 40)

        # values 2, 3 and 4 occur multiple times -> 3 duplicates
        self.assertEqual(scan_result.get(Metric.DUPLICATE_COUNT, 'score'), 3)
