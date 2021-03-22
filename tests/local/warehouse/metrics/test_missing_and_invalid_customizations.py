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
from sodasql.scan.scan_yml_parser import KEY_COLUMNS, KEY_METRICS, COLUMN_KEY_VALID_FORMAT, KEY_METRIC_GROUPS
from tests.common.sql_test_case import SqlTestCase


class TestMissingAndInvalidCustomizations(SqlTestCase):

    def test_scan_customized_missing_values(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('')",
             "('  ')",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
          KEY_COLUMNS: {
            'name': {
              KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_MISSING
              ],
              'missing_values': [
                'no value'
              ]
            }
          }
        })
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'name'), 40.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE, 'name'), 60.0)

    def test_scan_customized_missing_format_empty(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three')",
             "('four')",
             "('')",
             "('  ')",
             "('  ')",
             "(null)",
             "(null)",
             "(null)"])

        scan_result = self.scan({
          KEY_COLUMNS: {
            'name': {
              KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_MISSING
              ],
              'missing_format': 'empty'
            }
          }
        })
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'name'), 40.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 6)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE, 'name'), 60.0)

    def test_scan_customized_missing_format_whitespace(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('two')",
             "('three')",
             "('four')",
             "('')",
             "('  ')",
             "('  ')",
             "(null)",
             "(null)",
             "(null)"])

        scan_result = self.scan({
            KEY_COLUMNS: {
                'name': {
                    KEY_METRIC_GROUPS: [
                        Metric.METRIC_GROUP_MISSING
                    ],
                    'missing_format': 'whitespace'
                }
            }
        })
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'name'), 6)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'name'), 60.0)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE, 'name'), 40.0)

    def test_scan_missing_customized_and_validity(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('')",
             "('  ')",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
          KEY_COLUMNS: {
            'name': {
              KEY_METRICS: [
                  Metric.INVALID_COUNT
              ],
              'missing_values': [
                'no value'
              ],
              'valid_regex': 'one'
            }
          }
        })
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'name'), 2)

    def test_scan_valid_regex(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('one')",
             "('')",
             "('  ')",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
          KEY_COLUMNS: {
              'name': {
                  KEY_METRIC_GROUPS: [
                      Metric.METRIC_GROUP_VALIDITY
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

    def test_scan_valid_format(self):
        self.sql_recreate_table(
            [f"col {self.dialect.data_type_varchar_255}"],
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

        scan_result = self.scan({
          KEY_COLUMNS: {
              'col': {
                  KEY_METRIC_GROUPS: [
                      Metric.METRIC_GROUP_VALIDITY
                  ],
                  COLUMN_KEY_VALID_FORMAT: 'number_whole'
              }
          }
        })

        self.assertEqual(scan_result.get(Metric.MISSING_COUNT,      'col'), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE, 'col'), 10)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT,       'col'), 9)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE,  'col'), 90)

        self.assertEqual(scan_result.get(Metric.INVALID_COUNT,      'col'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'col'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT,        'col'), 7)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE,   'col'), 70.0)

    def test_scan_valid_format(self):
        self.sql_recreate_table(
            [f"col_pct {self.dialect.data_type_varchar_255}"],
            ["( '8%' )",
             "( null )",
             "( '3--%' )",
             "( 'nopct' )",
             "( '6%' )",
             "( '6%' )",
             "( '6%' )",
             "( '77 %' )"])

        scan_result = self.scan({
          'columns': {
              'col_pct': {
                  'metric_groups': [
                      'validity'
                  ],
                  'valid_format': 'number_percentage'
              }
          }
        })

        self.assertEqual(scan_result.get(Metric.MISSING_COUNT, 'col_pct'), 1)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'col_pct'), 2)
