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
from tests.common.sql_test_case import SqlTestCase


class TestSodaServerClient(SqlTestCase):

    def test_soda_server_client(self):
        self.use_mock_soda_server_client()

        self.sql_recreate_table(
            [self.sql_declare_string_column("name")],
            ["('one')",
             "('two')",
             "('three')",
             "(null)"])

        self.scan({
            'metrics': [
                Metric.CATEGORY_MISSING,
                Metric.CATEGORY_DUPLICATES
            ],
            'tests': {
                'thegood': f'{Metric.ROW_COUNT} > 0',
                'thebad': f'{Metric.ROW_COUNT} + 1 < 0'
            }
        })

        commands = self.mock_soda_server_client.commands
        scan_measurement_count = 0
        scan_test_result_count = 0
        for i in range(len(commands)):
            command = commands[i]
            command_type = command['type']
            if i == 0:
                # The first command should be a scanStart command
                self.assertEqual('scanStart', command_type)
            elif i == 1:
                # The first non-start command should be a scanMeasurements command with a schema measurement
                self.assertEqual(command_type, 'scanMeasurements')
                self.assertEqual(command['measurements'][0]['metric'], Metric.SCHEMA)
            elif i == len(commands)-1:
                # The last command should be a scanEnd command
                self.assertEqual('scanEnd', command_type)
            else:
                if command_type == 'scanMeasurements':
                    scan_measurement_count += 1
                elif command_type == 'scanTestResults':
                    scan_test_result_count += 1

        # There should at least be one scanMeasurement command
        self.assertGreater(scan_measurement_count, 0)
        # There should at least be one scanTestResults command
        self.assertGreater(scan_test_result_count, 0)
