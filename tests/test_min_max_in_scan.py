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
from tests.abstract_scan_test import AbstractScanTest


class TestMinMaxInScan(AbstractScanTest):

    table_name = 'customers'

    def test_scan_min_length(self):
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

        scan_result = self.scan({
          'table_name': self.table_name,
          'metrics': [
            'min_length'
          ]
        })

        measurement = scan_result.get_measurement(Metric.MIN_LENGTH, 'id')
        self.assertEqual(measurement.type, Metric.MIN_LENGTH)
        self.assertEqual(measurement.column, 'id')
        self.assertEqual(measurement.value, 1)

        measurement = scan_result.get_measurement(Metric.MIN_LENGTH, 'name')
        self.assertEqual(measurement.type, Metric.MIN_LENGTH)
        self.assertEqual(measurement.column, 'name')
        self.assertEqual(measurement.value, 3)
