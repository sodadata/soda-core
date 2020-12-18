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
from sodasql.sql_store.sql_store import SqlStore
from sodasql.tests.abstract_scan_test import AbstractScanTest


class TestMissingAndInvalidInScan(AbstractScanTest):
    table_name = 'customers'

    def create_sql_store(self) -> SqlStore:
        return SqlStore.create({
            'name': 'test-postgres-store',
            'type': 'postgres',
            'host': 'localhost',
            'port': '5432',
            'username': 'sodalite',
            'database': 'sodalite',
            'schema': 'public'})

    def setUp(self) -> None:
        super().setUp()
        self.sql_updates([
            f"DROP TABLE IF EXISTS {self.table_name}",

            f"CREATE TABLE {self.table_name} ( "
            f"  id VARCHAR(255), "
            f"  name VARCHAR(255), "
            f"  size INTEGER "
            f")",

            f"INSERT INTO {self.table_name} VALUES "
            f"  ('1', 'one',      1), "
            f"  ('2', 'two',      2), "
            f"  ('3', 'three',    3), "
            f"  ('4', 'no value', null), "
            f"  ('5', null,       null) "])

    def test_scan_without_configurations(self):
        measurements = self.scan({
          'table_name': self.table_name
        })

        measurement = measurements.get(Metric.SCHEMA)
        self.assertIsNotNone(measurement)
        self.assertEqual(measurement.value[0].name, 'id')
        self.assertEqual(measurement.value[1].name, 'name')
        self.assertEqual(measurement.value[2].name, 'size')

        self.assertEqual(measurements.value(Metric.ROW_COUNT), 5)

    def test_scan_missing(self):
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

        measurements.assertValue(Metric.MISSING_COUNT, 'name', 1)
        measurements.assertValue(Metric.MISSING_PERCENTAGE, 'name', 20.0)
        measurements.assertValue(Metric.VALUES_COUNT, 'name', 4)
        measurements.assertValue(Metric.VALUES_PERCENTAGE, 'name', 80)

        measurements.assertValue(Metric.INVALID_COUNT, 'name', 3)
        measurements.assertValue(Metric.INVALID_PERCENTAGE, 'name', 60.0)
        measurements.assertValue(Metric.VALID_COUNT, 'name', 1)
        measurements.assertValue(Metric.VALID_PERCENTAGE, 'name', 20.0)

    def test_scan_with_two_default_column_metric(self):
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

        measurements.assertValue(Metric.MISSING_COUNT, 'id', 0)
        measurements.assertValue(Metric.MISSING_PERCENTAGE, 'id', 0.0)
        measurements.assertValue(Metric.VALUES_COUNT, 'id', 5)
        measurements.assertValue(Metric.VALUES_PERCENTAGE, 'id', 100)

        measurements.assertValue(Metric.MISSING_COUNT, 'name', 1)
        measurements.assertValue(Metric.MISSING_PERCENTAGE, 'name', 20.0)
        measurements.assertValue(Metric.VALUES_COUNT, 'name', 4)
        measurements.assertValue(Metric.VALUES_PERCENTAGE, 'name', 80)

        measurements.assertValue(Metric.INVALID_COUNT, 'name', 3)
        measurements.assertValue(Metric.INVALID_PERCENTAGE, 'name', 60.0)
        measurements.assertValue(Metric.VALID_COUNT, 'name', 1)
        measurements.assertValue(Metric.VALID_PERCENTAGE, 'name', 20.0)

        measurements.assertValue(Metric.MISSING_COUNT, 'id', 0)
        measurements.assertValue(Metric.MISSING_PERCENTAGE, 'id', 0.0)
        measurements.assertValue(Metric.VALUES_COUNT, 'id', 5)
        measurements.assertValue(Metric.VALUES_PERCENTAGE, 'id', 100)

        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'id'), 0)
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'name'), 1)
        self.assertEqual(measurements.value(Metric.MISSING_COUNT, 'size'), 2)

    def test_fail_on_purpose(self):
        self.assertTrue(False)
