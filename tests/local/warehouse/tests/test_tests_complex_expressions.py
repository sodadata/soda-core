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


class TestTestsComplexExpressions(SqlTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three') "])

    def test_complex_expression_result(self):
        test_name = 'my_test'
        scan_yml_dict = {
            KEY_METRICS: [
                'row_count'
            ],
            KEY_TESTS: {
                test_name: '10 - row_count + 1 >= 5'
            }
        }

        scan_result = self.scan(scan_yml_dict)
        self.assertFalse(scan_result.has_test_failures())
        assert 'expression_result' in scan_result.test_results[0].values
        result_values = scan_result.test_results[0].values

        assert list(result_values.keys())[0] == 'expression_result'
        assert result_values['expression_result'] == 8
