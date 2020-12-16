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


class TestMetricsAsCode(AbstractScanTest):
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

        measurement = measurements.find(Metric.SCHEMA)
        self.assertIsNotNone(measurement)
        self.assertEqual(measurement.value[0].name, 'id')
        self.assertEqual(measurement.value[1].name, 'name')
        self.assertEqual(measurement.value[2].name, 'size')
        self.assertEqual(measurements.find(Metric.ROW_COUNT).value, 5)

    def test_scan_missing(self):
        measurements = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'missing'
          ]
        })
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'id').value, 0)
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'name').value, 1)
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'size').value, 2)

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
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'name').value, 2)

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
        self.assertEqual(measurements.find(Metric.INVALID_COUNT, 'name').value, 2)
        self.assertEqual(measurements.find(Metric.VALID_COUNT, 'name').value, 1)
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'name').value, 2)

    def test_scan_min_length(self):
        measurements = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'min_length'
          ]
        })

        measurement = measurements.find(Metric.MIN_LENGTH, 'id')
        self.assertEqual(measurement.type, Metric.MIN_LENGTH)
        self.assertEqual(measurement.column, 'id')
        self.assertEqual(measurement.value, 1)

        measurement = measurements.find(Metric.MIN_LENGTH, 'name')
        self.assertEqual(measurement.type, Metric.MIN_LENGTH)
        self.assertEqual(measurement.column, 'name')
        self.assertEqual(measurement.value, 3)

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
                  ]
              }
          }
        })

        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'id').value, 0)
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'name').value, 1)
        self.assertEqual(measurements.find(Metric.MISSING_COUNT, 'size').value, 2)
