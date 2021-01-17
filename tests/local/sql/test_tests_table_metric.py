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


class TestTestsTableMetric(SqlTestCase):

    def test_tests(self):
        self.sql_create_test_table(
            [self.warehouse.dialect.declare_string_column_sql("name")],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
            'table_name': self.default_test_table_name,
            'metrics': [
                'row_count'
            ],
            'tests': {
                'must have rows': 'row_count > 2'
            }
        })
        self.assertFalse(scan_result.has_failures())

        scan_result = self.scan({
            'table_name': self.default_test_table_name,
            'metrics': [
                'row_count'
            ],
            'tests': {
                'must have rows': 'row_count > 10'
            }
        })
        self.assertTrue(scan_result.has_failures())
