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
from sodasql.scan.scan_yml_parser import KEY_COLUMNS, KEY_METRIC_GROUPS
from tests.common.sql_test_case import SqlTestCase


class TestValidity(SqlTestCase):

    def test_text_column_validity(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
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
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_VALIDITY
            ],
            KEY_COLUMNS: {
                'score': {
                    'valid_format': 'number_whole'
                }
            },
        })

        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'score'), 11)

    def test_numeric_column_validity_format(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_integer}"],
            ["(1)",
             "(2)",
             "(2)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(4)",
             "(4)",
             "(5)",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_VALIDITY
            ],
            KEY_COLUMNS: {
                'score': {
                    'valid_format': 'number_whole'
                }
            },
        })

        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'score'), 11)

    def test_numeric_column_validity_without_format(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
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
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_VALIDITY
            ],
            KEY_COLUMNS: {
                'score': {
                    'valid_format': 'number_whole'
                }
            }
        })
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT), 0)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE), 0.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE), 91.66666666666667)

    def test_date_column_validity(self):
        self.sql_recreate_table(
            [f"xyz {self.dialect.data_type_date}"],
            ["('2021-01-01')",
             "('2021-01-01')",
             "('2021-01-02')",
             "('2021-01-02')",
             "('2021-01-02')",
             "('2021-01-02')",
             "('2021-01-02')", ]
        )

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_VALIDITY
            ],
            KEY_COLUMNS: {
                'xyz': {
                    'valid_format': 'date_iso_8601'
                }
            },
        })

        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'xyz'), 7)
