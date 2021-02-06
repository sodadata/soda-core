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
from sodasql.scan.scan_yml_parser import KEY_COLUMNS, KEY_METRICS, COLUMN_KEY_VALID_FORMAT, KEY_METRIC_GROUPS, \
    COLUMN_KEY_VALID_VALUES
from tests.common.sql_test_case import SqlTestCase


class TestValidValues(SqlTestCase):

    def test_valid_values(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('a')",
             "('b')",
             "('c')"])

        scan_result = self.scan({
          KEY_METRICS: [
              Metric.INVALID_COUNT
          ],
          KEY_COLUMNS: {
            'name': {
              COLUMN_KEY_VALID_VALUES: [
                'one',
                'two'
              ]
            }
          }
        })
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 2)
