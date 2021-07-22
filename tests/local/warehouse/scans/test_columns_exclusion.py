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

from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_EXCLUDED_COLUMNS
from tests.common.sql_test_case import SqlTestCase


class TestColumnsExclusion(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"one {self.dialect.data_type_varchar_255}", f"two {self.dialect.data_type_varchar_255}"],
            ["('a', 'alpha')",
             "('b', 'beta')",
             "('c', 'gamma') ",
             "('d', 'delta')",
             "(null, null)"])

    def test_scan_result_with_test_error(self):
        scan_yml_dict = {
            KEY_EXCLUDED_COLUMNS: [
                "two"
            ],
            KEY_METRICS: [
                'row_count',
                'missing_count'
            ]

        }
        scan_result = self.scan(scan_yml_dict)
        self.assertIsNotNone(scan_result.get_measurement('missing_count', 'one'))
        with self.assertRaises(AssertionError):
            scan_result.get_measurement('missing_count', 'two')
