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
from sodasql.scan.scan import Scan
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.tests.abstract_scan_test import AbstractScanTest


class TestMissingAndInvalidInScan(AbstractScanTest):

    def test_scan_without_configurations(self):
        self.sql_create_table(
            'customers',
            ["name VARCHAR(255)"],
            ["('one')",
             "('two')",
             "('three') ",
             "('no value')",
             "(null)"])

        scan_result = Scan(self.sql_store, scan_configuration=ScanConfiguration({
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
        })).execute()
        self.assertFalse(scan_result.has_failures())

        scan_result = Scan(self.sql_store, scan_configuration=ScanConfiguration({
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
        })).execute()
        self.assertTrue(scan_result.has_failures())



