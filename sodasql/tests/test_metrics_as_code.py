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

from sodasql.scan.measurement import Measurement
from sodasql.scan.scan import Scan
from sodasql.scan.scan_configuration import ScanConfiguration
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
            'username': 'sodasql',
            'database': 'sodasql',
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
            f"  ('1', 'one',   1), "
            f"  ('2', null,    2), "
            f"  ('3', 'three', null) "])

    def test_scan_without_configurations(self):
        measurements = Scan(self.sql_store, scan_configuration=ScanConfiguration({
          'table_name': self.table_name
        })).execute()

        measurement = measurements[0]
        self.assertEqual(measurement.type, Measurement.TYPE_SCHEMA)
        self.assertEqual(measurement.value[0].name, 'id')
        self.assertEqual(measurement.value[1].name, 'name')
        self.assertEqual(measurement.value[2].name, 'size')

        measurement = measurements[1]
        self.assertEqual(measurement.type, Measurement.TYPE_ROW_COUNT)
        self.assertEqual(measurement.value, 3)

        self.assertEqual(len(measurements), 2)

    def test_scan_missing(self):
        measurements = Scan(self.sql_store, scan_configuration=ScanConfiguration({
          'table_name': self.table_name,
          'column_metrics': [
            'missing'
          ]
        })).execute()

        measurement = measurements[2]
        self.assertEqual(measurement.type, Measurement.TYPE_MISSING_COUNT)
        self.assertEqual(measurement.column.name, 'id')
        self.assertEqual(measurement.value, 0)

        measurement = measurements[3]
        self.assertEqual(measurement.type, Measurement.TYPE_MISSING_COUNT)
        self.assertEqual(measurement.column.name, 'name')
        self.assertEqual(measurement.value, 1)

        measurement = measurements[4]
        self.assertEqual(measurement.type, Measurement.TYPE_MISSING_COUNT)
        self.assertEqual(measurement.column.name, 'size')
        self.assertEqual(measurement.value, 1)

        self.assertEqual(len(measurements), 5)

    def test_scan_min_length(self):
        measurements = Scan(self.sql_store, scan_configuration=ScanConfiguration({
          'table_name': self.table_name,
          'column_metrics': [
            'min_length'
          ]
        })).execute()

        measurement = measurements[2]
        self.assertEqual(measurement.type, Measurement.TYPE_MIN_LENGTH)
        self.assertEqual(measurement.column.name, 'id')
        self.assertEqual(measurement.value, 1)

        measurement = measurements[3]
        self.assertEqual(measurement.type, Measurement.TYPE_MIN_LENGTH)
        self.assertEqual(measurement.column.name, 'name')
        self.assertEqual(measurement.value, 3)

        self.assertEqual(len(measurements), 4)

    def test_scan_with_two_default_column_metric(self):
        measurements = Scan(self.sql_store, scan_configuration=ScanConfiguration({
          'table_name': self.table_name,
          'column_metrics': [
            'missing'
          ],
          'columns': {
              'name': {
                  'metrics': [
                      'invalid'
                  ]
              }
          }
        })).execute()

        self.assertEqual(len(self.find_measurements_by_column_name(measurements, 'id')), 1)
        self.assertEqual(len(self.find_measurements_by_column_name(measurements, 'name')), 2)
        self.assertEqual(len(self.find_measurements_by_column_name(measurements, 'size')), 1)

    def find_measurements_by_column_name(self, measurements, column_name):
        return list(filter(lambda measurement: measurement.column and measurement.column.name == column_name, measurements))
