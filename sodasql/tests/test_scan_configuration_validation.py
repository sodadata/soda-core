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

from sodasql.scan.parse_logs import ParseLogs
from sodasql.scan.scan_configuration import ScanConfiguration


class TestScanConfigurationValidation(TestCase):

    def test_table_name_required(self):
        parse_logs = ScanConfiguration({}).parse_logs
        log = parse_logs.logs[0]
        self.assertIn(ParseLogs.ERROR, log.level)
        self.assertIn('table_name is required', log.message)

    def test_metrics_not_a_list(self):
        parse_logs = ScanConfiguration({
            'table_name': 't',
            'metrics': 'txt'
        }).parse_logs

        log = parse_logs.logs[0]
        self.assertIn(ParseLogs.ERROR, log.level)
        self.assertIn('metrics is not a list', log.message)

    def test_invalid_column_metric(self):
        parse_logs = ScanConfiguration({
            'table_name': 't',
            'metrics': [
                'revenue'
            ]
        }).parse_logs

        log = parse_logs.logs[0]
        self.assertIn(ParseLogs.WARNING, log.level)
        self.assertIn('Invalid metrics value: revenue', log.message)

    def test_invalid_valid_format(self):
        parse_logs = ScanConfiguration({
            'table_name': 't',
            'columns': {
                'col': {
                    'valid_format': 'buzz'
                }
            }
        }).parse_logs

        log = parse_logs.logs[0]
        self.assertIn(ParseLogs.WARNING, log.level)
        self.assertIn('Invalid col.valid_format: buzz', log.message)
