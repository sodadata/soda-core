#  Copyright 2021 Soda
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
import uuid


class TestNetworkValidityFormats(SqlTestCase):

    def test_uuid(self):
        self.sql_recreate_table(
            [self.sql_declare_string_column("name")],
            [f"('{uuid.uuid1()}')",
             f"('{uuid.uuid3(uuid.NAMESPACE_URL, 'http://python.org/')}')",
             f"('{uuid.uuid4()}')",
             f"('{uuid.uuid5(uuid.NAMESPACE_URL, 'http://python.org/')}')",
             "('88888888-4444-4444-121212121212')",
             "('Nyuk-Nyuk-Nyuk')",
             "('Lambda-Lambda-Lambda')",
             "('Heather-Christina-Pamela-Neil-Patrick-Harris')"])

        scan_result = self.scan({
            'metrics': [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            'columns': {
                'name': {
                    'valid_format': 'uuid'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 50.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 50.0)

    def test_ip_address(self):
        self.sql_recreate_table(
            [self.sql_declare_string_column("name")],
            ["('0.0.0.0')",
             "('127.0.0.1')",
             "('10.1.2.3')",
             "('6.0.0.6.5')"])

        scan_result = self.scan({
            'metrics': [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            'columns': {
                'name': {
                    'valid_format': 'ip_address'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 1)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 25.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 75.0)
