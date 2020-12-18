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
from sodasql.tests.abstract_scan_test import AbstractScanTest


class TestMissingAndInvalidInScan(AbstractScanTest):

    table_name = None

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.table_name = None

    def _create_table_customers(self):
        if not self.table_name == 'customers':
            self.table_name = 'customers'
            self.sql_create_table(
                self.table_name,
                ["id VARCHAR(255)",
                 "name VARCHAR(255)",
                 "size INTEGER"],
                ["('1', 'one',      1)",
                 "('2', 'two',      2)",
                 "('3', 'three',    3) ",
                 "('4', 'no value', null)",
                 "('5', null,       null)"])

    def test_scan_without_configurations(self):
        self._create_table_customers()

        measurements = self.scan({
          'table_name': self.table_name
        })

        measurement = measurements.get(Metric.SCHEMA)
        self.assertIsNotNone(measurement)
        self.assertEqual(measurement.value[0].name, 'id')
        self.assertEqual(measurement.value[1].name, 'name')
        self.assertEqual(measurement.value[2].name, 'size')

        measurements.assertValueDataset(Metric.ROW_COUNT, 5)

    def test_scan_missing(self):
        self._create_table_customers()

        measurements = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'missing'
          ]
        })
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'id'), 0)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'id'), 0.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT, 'id'), 5)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE, 'id'), 100.0)

        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'name'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE, 'name'), 80.0)

        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'size'), 2)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'size'), 40.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT, 'size'), 3)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE, 'size'), 60.0)

    def test_scan_missing_customized(self):
        self._create_table_customers()

        measurements = self.scan({
          'table_name': self.table_name,
          'columns': {
            'name': {
              'metrics': [
                'missing'
              ],
              'missing_values': [
                'no value'
              ]
            }
          }
        })
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'name'), 2)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'name'), 40.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT, 'name'), 3)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE, 'name'), 60.0)

    def test_scan_missing_customized_and_validity(self):
        self._create_table_customers()

        measurements = self.scan({
          'table_name': self.table_name,
          'columns': {
            'name': {
              'metrics': [
                  'invalid_count'
              ],
              'missing_values': [
                'no value'
              ],
              'valid_regex': 'one'
            }
          }
        })
        self.assertEqual(measurements.value(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(measurements.value(Metric.VALID_COUNT, 'name'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'name'), 2)

    def test_scan_min_length(self):
        self._create_table_customers()

        measurements = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'min_length'
          ]
        })

        measurement = measurements.get(Metric.MIN_LENGTH, 'id')
        self.assertEqual(measurement.type, Metric.MIN_LENGTH)
        self.assertEqual(measurement.column, 'id')
        self.assertEqual(measurement.value, 1)

        measurement = measurements.get(Metric.MIN_LENGTH, 'name')
        self.assertEqual(measurement.type, Metric.MIN_LENGTH)
        self.assertEqual(measurement.column, 'name')
        self.assertEqual(measurement.value, 3)

    def test_scan_with_two_default_column_metric(self):
        self._create_table_customers()

        # validity triggers missing measurements
        measurements = self.scan({
          'table_name': self.table_name,
          'columns': {
              'name': {
                  'metrics': [
                      'invalid'
                  ],
                  'valid_regex': 'one'
              }
          }
        })

        self.assertEqual(measurements.value(Metric.MISSING_COUNT,      'name'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT,       'name'), 4)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE,  'name'), 80)

        self.assertEqual(measurements.value(Metric.INVALID_COUNT,      'name'), 3)
        self.assertEqual(measurements.value(Metric.INVALID_PERCENTAGE, 'name'), 60.0)
        self.assertEqual(measurements.value(Metric.VALID_COUNT,        'name'), 1)
        self.assertEqual(measurements.value(Metric.VALID_PERCENTAGE,   'name'), 20.0)

    def test_scan_valid_regex(self):
        self._create_table_customers()

        measurements = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'missing'
          ],
          'columns': {
              'name': {
                  'metrics': [
                      'invalid'
                  ],
                  'valid_regex': 'one'
              }
          }
        })

        self.assertEqual(measurements.value(Metric.MISSING_COUNT,      'id'), 0)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'id'), 0.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT,       'id'), 5)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE,  'id'), 100)

        self.assertEqual(measurements.value(Metric.MISSING_COUNT,      'name'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT,       'name'), 4)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE,  'name'), 80)

        self.assertEqual(measurements.value(Metric.INVALID_COUNT,      'name'), 3)
        self.assertEqual(measurements.value(Metric.INVALID_PERCENTAGE, 'name'), 60.0)
        self.assertEqual(measurements.value(Metric.VALID_COUNT,        'name'), 1)
        self.assertEqual(measurements.value(Metric.VALID_PERCENTAGE,   'name'), 20.0)

        self.assertEqual(measurements.value(Metric.MISSING_COUNT,      'id'), 0)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'id'), 0.0)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT,       'id'), 5)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE,  'id'), 100)

        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'id'), 0)
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'name'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'size'), 2)

    def test_scan_valid_format(self):
        self.table_name = 'customers'

        self.sql_create_table(
            self.table_name,
            ["col VARCHAR(255)"],
            ["('1')",
             "('2')",
             "('3')",
             "('4')",
             "('4')",
             "('4')",
             "('xxx') ",
             "('yyy') ",
             "(null)",
             "('10')"])

        measurements = self.scan({
          'table_name': self.table_name,
          'columns': {
              'col': {
                  'metrics': [
                      'invalid'
                  ],
                  'valid_format': 'number_whole'
              }
          }
        })

        self.assertEqual(measurements.value(Metric.MISSING_COUNT,      'col'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'col'), 10)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT,       'col'), 9)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE,  'col'), 90)

        self.assertEqual(measurements.value(Metric.INVALID_COUNT,      'col'), 2)
        self.assertEqual(measurements.value(Metric.INVALID_PERCENTAGE, 'col'), 20.0)
        self.assertEqual(measurements.value(Metric.VALID_COUNT,        'col'), 7)
        self.assertEqual(measurements.value(Metric.VALID_PERCENTAGE,   'col'), 70.0)

    def test_scan_valid_min_length_max_length(self):
        self.table_name = 'customers'

        self.sql_create_table(
            self.table_name,
            ["col VARCHAR(255)"],
            ["(null)",
             "('')",
             "('1')",
             "('12')",
             "('123')",
             "('1234')",
             "('12345') ",
             "('123456') ",
             "('1234567')",
             "('12345678')"])

        measurements = self.scan({
          'table_name': self.table_name,
          'columns': {
              'col': {
                  'metrics': [
                      'invalid'
                  ],
                  'valid_min_length': 3,
                  'valid_max_length': 7
              }
          }
        })

        self.assertEqual(measurements.value(Metric.MISSING_COUNT,      'col'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_PERCENTAGE, 'col'), 10)
        self.assertEqual(measurements.value(Metric.VALUES_COUNT,       'col'), 9)
        self.assertEqual(measurements.value(Metric.VALUES_PERCENTAGE,  'col'), 90)

        self.assertEqual(measurements.value(Metric.INVALID_COUNT,      'col'), 4)
        self.assertEqual(measurements.value(Metric.INVALID_PERCENTAGE, 'col'), 40.0)
        self.assertEqual(measurements.value(Metric.VALID_COUNT,        'col'), 5)
        self.assertEqual(measurements.value(Metric.VALID_PERCENTAGE,   'col'), 50.0)
