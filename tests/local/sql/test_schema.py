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


class TestSchema(SqlTestCase):

    table_name = 'test_table'

    def test_schema_measurement(self):
        self.sql_create_table(
            self.table_name,
            ["id VARCHAR(255)",
             "name VARCHAR(255)",
             "size INTEGER"],
            ["('1', 'one',      1)"])

        scan_result = self.scan({
            'table_name': self.table_name
        })

        measurement = scan_result.find_measurement(Metric.SCHEMA)
        self.assertIsNotNone(measurement)
        column = measurement.value[0]
        self.assertEqual(column.name, 'id')
        self.assertEqual(column.type, 'character varying')
        self.assertEqual(column.nullable, True)

        column = measurement.value[1]
        self.assertEqual(column.name, 'name')
        self.assertEqual(column.type, 'character varying')
        self.assertEqual(column.nullable, True)

        column = measurement.value[2]
        self.assertEqual(column.name, 'size')
        self.assertEqual(column.type, 'integer')
        self.assertEqual(column.nullable, True)

        self.assertEqual(scan_result.get(Metric.ROW_COUNT), 1)
