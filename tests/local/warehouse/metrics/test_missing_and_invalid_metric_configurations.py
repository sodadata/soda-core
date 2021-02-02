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


class TestMissingAndInvalidMetricConfigurations(SqlTestCase):
    """
    Due to the use of multiple inheritance, setUp() methods for all inherited classes will be called, thus,
    one should not put anything in setUp() that can affect other test methods, such as creating tables.
    """

    def test_scan_without_configurations(self):
        self._create_test_table()
        scan_result = self.scan()

        self.assertMeasurementsAbsent(scan_result, 'id', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE,
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurementsAbsent(scan_result, 'name', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE,
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurementsAbsent(scan_result, 'size', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE,
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

    def test_scan_missing(self):
        self._create_test_table()
        scan_result = self.scan({
          'metrics': [
            Metric.CATEGORY_MISSING
          ]
        })
        
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'id'), 0)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'id'), 0.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'id'), 5)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE, 'id'), 100.0)

        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'name'), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE, 'name'), 80.0)

        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'size'), 2)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'size'), 40.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'size'), 3)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE, 'size'), 60.0)

        self.assertMeasurementsAbsent(scan_result, 'id', [
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurementsAbsent(scan_result, 'name', [
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurementsAbsent(scan_result, 'size', [
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

    def test_scan_with_two_default_column_metric(self):
        self._create_test_table()
        # validity triggers missing measurements
        scan_result = self.scan({
            'columns': {
                'name': {
                    'metrics': [
                        Metric.CATEGORY_VALIDITY
                    ],
                    'valid_regex': 'one'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.MISSING_COUNT,      'name'), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT,       'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE,  'name'), 80)

        self.assertEqual(scan_result.get(Metric.INVALID_COUNT,      'name'), 3)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 60.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT,        'name'), 1)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE,   'name'), 20.0)

        self.assertMeasurementsAbsent(scan_result, 'id', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE,
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurementsAbsent(scan_result, 'size', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE,
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

    def test_scan_valid_regex(self):
        self._create_test_table()
        scan_result = self.scan({
            'metrics': [
              'missing'
            ],
            'columns': {
                'name': {
                    'metrics': [
                        Metric.CATEGORY_VALIDITY
                    ],
                    'valid_regex': 'one'
                }
            }
        })

        self.assertMeasurements(scan_result, 'id', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE
        ])
        self.assertMeasurementsAbsent(scan_result, 'id', [
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurements(scan_result, 'name', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE,
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

        self.assertMeasurements(scan_result, 'size', [
            Metric.MISSING_COUNT,
            Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT,
            Metric.VALUES_PERCENTAGE
        ])
        self.assertMeasurementsAbsent(scan_result, 'size', [
            Metric.INVALID_COUNT,
            Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT,
            Metric.VALID_PERCENTAGE
        ])

    def _create_test_table(self):
        self.sql_recreate_table(
            [f"id {self.dialect.data_type_varchar_255}",
             f"name {self.dialect.data_type_varchar_255}",
             f"size {self.dialect.data_type_integer}"],
            ["('1', 'one',      1)",
             "('2', '',         2)",
             "('3', '  ',       3)",
             "('4', 'no value', null)",
             "('5', null,       null)"])
