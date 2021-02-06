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

from unittest import TestCase

from sodasql.scan.parser import ERROR, WARNING
from sodasql.scan.scan_yml_parser import ScanYmlParser, KEY_METRICS, KEY_COLUMNS, KEY_TABLE_NAME


class TestScanConfigurationValidation(TestCase):

    def test_table_name_required(self):
        parser = ScanYmlParser({}, 'Test scan')
        log = parser.logs[0]
        self.assertIn(ERROR, log.level)
        self.assertIn('table_name', log.message)
        self.assertIn('does not exist', log.message)

    def test_metrics_not_a_list(self):
        parser = ScanYmlParser({
            KEY_TABLE_NAME: 't',
            KEY_METRICS: 'txt'
        }, 'Test scan')

        log = parser.logs[0]
        self.assertIn(ERROR, log.level)
        self.assertIn('Invalid metrics', log.message)
        self.assertIn('list', log.message)
        self.assertIn('str', log.message)

    def test_invalid_column_metric(self):
        parser = ScanYmlParser({
            KEY_TABLE_NAME: 't',
            KEY_METRICS: [
                'revenue'
            ]
        }, 'Test scan')

        log = parser.logs[0]
        self.assertIn(WARNING, log.level)
        self.assertIn('Invalid key', log.message)
        self.assertIn('metrics', log.message)
        self.assertIn('revenue', log.message)

    def test_invalid_valid_format(self):
        parser = ScanYmlParser({
            KEY_TABLE_NAME: 't',
            KEY_COLUMNS: {
                'col': {
                    'valid_format': 'buzz'
                }
            }
        }, 'Test scan')

        log = parser.logs[0]
        self.assertIn(WARNING, log.level)
        self.assertIn('Invalid', log.message)
        self.assertIn('valid_format', log.message)
        self.assertIn('buzz', log.message)
