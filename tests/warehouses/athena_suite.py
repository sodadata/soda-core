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

from os import path
from typing import List

from sodasql.scan.metric import Metric
from sodasql.scan.scan_yml_parser import KEY_COLUMNS, KEY_METRIC_GROUPS
from tests.common.sql_test_case import TARGET_ATHENA
from tests.common.sql_test_suite import SqlTestSuite


class AthenaSuite(SqlTestSuite):

    def setUp(self) -> None:
        self.target = TARGET_ATHENA
        super().setUp()

    def test_skip_column_with_unknown_type_struct(self):
        # goal of this test is to ensure that unknown types like the struct are skipped during the scan

        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}",
             f"structcolumn struct<partone:string,parttwo:string>"],
            ["('one',      null)",
             "('',         null)",
             "('  ',       null)",
             "('no value', null)",
             "(null,       null)"])

        scan_result = self.scan({
          'columns': {
            'name': {
              'metric_groups': [
                'missing'
              ]
            }
          }
        })
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'name'), 1)

        self.assertIsNone(scan_result.find_measurement(Metric.MISSING_COUNT, 'structcolumn'))
