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

from tests.common.sql_test_case import SqlTestCase


class TestColumnMetricTests(SqlTestCase):

    def test_column_metric_test(self):
        self.create_test_table(
            [self.sql_declare_string_column("name")],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
            'metrics': [
                'missing'
            ],
            'columns': {
                'name': {
                    'tests': {
                        'no_missing': 'missing_count < 2'
                    }
                }
            }
        })
        self.assertFalse(scan_result.has_failures())

        scan_result = self.scan({
            'metrics': [
                'missing'
            ],
            'columns': {
                'name': {
                    'tests': {
                        'no_missing': 'missing_count == 0'
                    }
                }
            }
        })
        self.assertTrue(scan_result.has_failures())

    def test_column_metric_metric_calculation_test(self):
        self.create_test_table(
            [self.sql_declare_integer_column("size")],
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
                    'tests': {
                        'spread10': 'max - min < 10',
                        'spread05': 'max - min < 5'
                    }
                }
            }
        })
        self.assertTrue(scan_result.has_failures())

