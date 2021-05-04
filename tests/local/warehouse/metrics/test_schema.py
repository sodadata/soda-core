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

    def test_schema_measurement(self):
        dialect = self.warehouse.dialect

        self.sql_recreate_table(
            [f"id {self.dialect.data_type_varchar_255}",
             f"name {self.dialect.data_type_varchar_255}",
             f"size {self.dialect.data_type_integer}"],
            ["('1', 'one',      1)"])

        scan_result = self.scan()

        measurement = scan_result.find_measurement(Metric.SCHEMA)
        self.assertIsNotNone(measurement)

        columns_by_name_lower = {column['name'].lower(): column for column in measurement.value}

        column = columns_by_name_lower['id']
        self.assertTrue(dialect.is_text(column['dataType']))

        column = columns_by_name_lower['name']
        self.assertTrue(dialect.is_text(column['dataType']))

        column = columns_by_name_lower['size']
        self.assertTrue(dialect.is_number(column['dataType']))

        self.assertIsNone(scan_result.find_measurement(Metric.ROW_COUNT))
