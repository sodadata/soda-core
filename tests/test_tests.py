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

from tests.abstract_scan_test import AbstractScanTest


class TestTests(AbstractScanTest):

    def test_tests(self):
        self.create_table(
            'customers',
            ["name VARCHAR(255)"],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
            'table_name': 'customers',
            'metrics': [
                'missing'
            ],
            'columns': {
                'name': {
                    'tests': [
                        'missing_count < 2'
                    ]
                }
            }
        })
        self.assertFalse(scan_result.has_failures())

        scan_result = self.scan({
            'table_name': 'customers',
            'metrics': [
                'missing'
            ],
            'columns': {
                'name': {
                    'tests': [
                        'missing_count == 0'
                    ]
                }
            }
        })
        self.assertTrue(scan_result.has_failures())
