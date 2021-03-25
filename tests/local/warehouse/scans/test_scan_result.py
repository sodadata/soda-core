#   Copyright 2020 Soda
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
from sodasql.exceptions.exceptions import ERROR_CODE_TEST_FAILED
from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_TESTS
from tests.common.sql_test_case import SqlTestCase


class TestScanResult(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

    def test_scan_result_with_test_error(self):
        scan_yml_dict = {
            KEY_METRICS: [
                'row_count'
            ],
            KEY_TESTS: [
                '10 < error < 20'
            ]
        }
        scan_result = self.scan(scan_yml_dict)
        self.assertTrue(scan_result.has_test_failures())
        self.assertEqual(len(scan_result.errors), 1)
        error_json = scan_result.errors[0].to_json()
        self.assertEqual(error_json['type'], 'test_execution_error')
        self.assertIsNotNone(error_json['message'])

    def test_scan_result_with_test_errors(self):
        scan_yml_dict = {
            KEY_METRICS: [
                'row_count'
            ],
            KEY_TESTS: [
                '10 < error < 20',
                'lskdfj(lkj)',
            ]
        }
        scan_result = self.scan(scan_yml_dict)
        self.assertTrue(scan_result.has_test_failures())
        self.assertEqual(len(scan_result.errors), 2)

        error_json = scan_result.errors[0].to_json()
        self.assertEqual(error_json['type'], 'test_execution_error')
        self.assertIn('10 < error < 20', error_json['message'])

        error_json = scan_result.errors[1].to_json()
        self.assertEqual(error_json['type'], 'test_execution_error')
        self.assertIn('lskdfj(lkj)', error_json['message'])
