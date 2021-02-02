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


class TestPersonalInfoValidityFormats(SqlTestCase):

    def test_email(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('moe@pantherbrewing.com')",
             "('larry@pantherbrewing.com')",
             "('curly@pantherbrewing.com')",
             "('Soitenly not an email pattern! Nyuk, Nuyk, Nuyk...')",
             "(null)"])

        scan_result = self.scan({
            'metrics': [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            'columns': {
                'name': {
                    'valid_format': 'email'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 1)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 60.0)

    def test_phone(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('+1 123 123 1234')",
             "('+12 123 123 1234')",
             "('123 123 1234')",
             "('+1 123-123-1234')",
             "('+12 123-123-1234')",
             "('+12 123 123-1234')",
             "('555-2368')",
             "('555-ABCD')"])

        scan_result = self.scan({
            'metrics': [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            'columns': {
                'name': {
                    'valid_format': 'phone_number'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 1)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 12.5)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 7)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 87.5)

    def test_credit_card_number(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('1616161616161616')",
             "('4444-4444-4444-4444')",
             "('4444 4444 4444 4444')",
             "('55555 55555 55555 55555')"])

        scan_result = self.scan({
            'metrics': [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            'columns': {
                'name': {
                    'valid_format': 'credit_card_number'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 1)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 25.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 75.0)
