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


class TestTestsWithFiltering(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"start_date {self.dialect.data_type_date}"],
            ["('2022-01-01')",
             "('2022-01-01')",
             "('2022-01-01')"])

    def test_dynamic_filtering(self):
        test_name = 'my_test'
        scan_yml_dict = {
            'filter': "DATE(start_date) = '{{ START_DATE }}'",
            KEY_METRICS: [
                'row_count'
            ],
            KEY_TESTS: {
                test_name: 'row_count > 0'
            }
        }

        scan_result = self.scan(scan_yml_dict, variables={
            'START_DATE': '2022-01-01'})
        self.assertFalse(scan_result.has_test_failures())

    def test_static_filtering(self):
            test_name = 'my_test'
            scan_yml_dict = {
                'filter': "DATE(start_date) = '2022-01-01'",
                KEY_METRICS: [
                    'row_count'
                ],
                KEY_TESTS: {
                    test_name: 'row_count > 0'
                }
            }

            scan_result = self.scan(scan_yml_dict)
            self.assertFalse(scan_result.has_test_failures())
