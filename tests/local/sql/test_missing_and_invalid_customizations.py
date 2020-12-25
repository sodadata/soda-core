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
from tests.common.abstract_scan_test import AbstractScanTest


class TestMissingAndInvalidCustomizations(AbstractScanTest):

    table_name = 'test_table'

    def test_scan_customized_missing_values(self):
        self.create_table(
            self.table_name,
            ["name VARCHAR(255)"],
            ["('one')",
             "('')",
             "('  ')",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
          'table_name': self.table_name,
          'columns': {
            'name': {
              'metrics': [
                Metric.CATEGORY_MISSING
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
        self.create_table(
            self.table_name,
            ["name VARCHAR(255)"],
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
          'table_name': self.table_name,
          'columns': {
            'name': {
              'metrics': [
                Metric.CATEGORY_MISSING
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
        self.create_table(
            self.table_name,
            ["name VARCHAR(255)"],
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
            'table_name': self.table_name,
            'columns': {
                'name': {
                    'metrics': [
                        Metric.CATEGORY_MISSING
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
        self.create_table(
            self.table_name,
            ["name VARCHAR(255)"],
            ["('one')",
             "('')",
             "('  ')",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
          'table_name': self.table_name,
          'columns': {
            'name': {
              'metrics': [
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
        self.create_table(
            self.table_name,
            ["name VARCHAR(255)"],
            ["('one')",
             "('')",
             "('  ')",
             "('no value')",
             "(null)"])

        scan_result = self.scan({
          'table_name': self.table_name,
          'columns': {
              'name': {
                  'metrics': [
                      Metric.CATEGORY_VALIDITY
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
        self.create_table(
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

        scan_result = self.scan({
          'table_name': self.table_name,
          'columns': {
              'col': {
                  'metrics': [
                      Metric.CATEGORY_VALIDITY
                  ],
                  'valid_format': 'number_whole'
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
