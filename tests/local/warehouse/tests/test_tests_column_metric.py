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
from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_COLUMNS, COLUMN_KEY_TESTS, KEY_METRIC_GROUPS
from tests.common.sql_test_case import SqlTestCase


class TestColumnMetricTests(SqlTestCase):

    def test_column_metric_test(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

        scan_yml_dict = {
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_MISSING
            ],
            KEY_COLUMNS: {
                'name': {
                    COLUMN_KEY_TESTS: [
                        'missing_count < 2'
                    ]
                }
            }
        }
        scan_result = self.scan(scan_yml_dict)
        self.assertFalse(scan_result.has_test_failures())

        scan_yml_dict[KEY_COLUMNS]['name']['tests'][0] = 'missing_count == 0'

        scan_result = self.scan(scan_yml_dict)
        self.assertTrue(scan_result.has_test_failures())

    def test_column_metric_metric_calculation_test(self):
        self.sql_recreate_table(
            [f"size {self.dialect.data_type_integer}"],
            ["(3)",
             "(3)",
             "(4) ",
             "(12)",
             "(11)"])

        scan_result = self.scan({
            'metrics': [
                'min',
                'max'
            ],
            'columns': {
                'size': {
                    'tests': [
                        'max > 0',
                        'min < 20',
                        'max - min < 10',
                        'max - min < 5'
                    ]
                }
            }
        })
        self.assertTrue(scan_result.has_test_failures())
