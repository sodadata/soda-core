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


class TestTestsWithVariables(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

    def test_tests_with_variables(self):
        scan_yml_dict = {
            KEY_METRICS: [
                'row_count'
            ],
            'tests': ['{{ row_count_variable_min }} < row_count < {{ row_count_variable_max }}']
        }

        scan_result = self.scan(scan_yml_dict, variables={
            'row_count_variable_min': 1,
            'row_count_variable_max': 20})
        self.assertFalse(scan_result.has_test_failures())
