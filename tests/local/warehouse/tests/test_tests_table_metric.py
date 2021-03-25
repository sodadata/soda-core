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
from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_TESTS
from tests.common.sql_test_case import SqlTestCase


class TestTestsTableMetric(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

    def test_tests(self):
        scan_yml_dict = {
            KEY_METRICS: [
                'row_count'
            ],
            'tests': ['2 < row_count < 20']
        }
        scan_result = self.scan(scan_yml_dict)
        self.assertFalse(scan_result.has_test_failures())

        scan_yml_dict['tests'][0] = '10 < row_count < 20'
        scan_result = self.scan(scan_yml_dict)
        self.assertTrue(scan_result.has_test_failures())

    def test_named_tests(self):
        test_name = 'my_test'
        scan_yml_dict = {
            KEY_METRICS: [
                'row_count'
            ],
            KEY_TESTS: {
                test_name: '2 < row_count < 20'
            }
        }
        scan_result = self.scan(scan_yml_dict)
        self.assertFalse(scan_result.has_test_failures())

        scan_yml_dict['tests'][test_name] = '10 < row_count < 20'
        scan_result = self.scan(scan_yml_dict)
        self.assertTrue(scan_result.has_test_failures())
