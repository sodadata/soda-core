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

from datetime import datetime, timezone

from sodasql.scan.metric import Metric
from sodasql.scan.scan_yml_parser import KEY_METRICS, KEY_COLUMNS
from tests.common.sql_test_case import SqlTestCase


class TestDateAndTimeValidityFormats(SqlTestCase):

    def test_date_eu(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('21-01-2021')",
             "('21.01.2021')",
             "('21/01/2021')",
             "('21/01/21')",
             "('01/21/2021')",
             "('2021, January 21')",
             "('October 21, 2015')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'date_eu'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 7)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 37.5)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 50.0)

    def test_date_us(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('01-21-2021')",
             "('01.21.2021')",
             "('01/21/2021')",
             "('01/21/21')",
             "('21/01/2021')",
             "('2021, January 21')",
             "('October 21, 2015')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'date_us'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 7)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 37.5)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 50.0)

    def test_date_inverse(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('2021-01-21')",
             "('2021.01.21')",
             "('2021/01/21')",
             "('21/01/21')",
             "('21/01/2021')",
             "('2021, January 21')",
             "('October 21, 2015')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'date_inverse'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 7)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 50)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 37.5)

    def test_time(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('11:59:00,000')",
             "('11:59:00')",
             "('11:59')",
             "('11-59-00,000')",
             "('23:59:00,000')",
             "('Noon')",
             "('1,159')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'time'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 7)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 25.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 5)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 62.5)

    def test_date_iso_8601(self):
        test_date = datetime.now()
        test_date_with_timezone = datetime.now(timezone.utc)
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            [f"('{test_date.isoformat()}')",
             f"('{test_date_with_timezone.isoformat()}')",
             "('2021, January 21')",
             "('October 21, 2015')",
             "(null)"])
    
        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'date_iso_8601'
                }
            }
        })
    
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 40.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 40.0)
