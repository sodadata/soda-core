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
import logging

from tests.common.sql_test_case import SqlTestCase


class FilterAndGroupByTest(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_create_test_table(
            [self.warehouse.dialect.declare_string_column_sql("name"),
             self.warehouse.dialect.declare_integer_column_sql("size")],
            ["('one',    1)",
             "('two',    1)",
             "('two',    2)",
             "('two',    3)",
             "('two',    4)",
             "('three',  1)",
             "('three',  2)",
             "('three',  3)",
             "('three',  4)",
             "('three',  5)",
             "('three',  6)",
             "('three',  7)",
             "('four',   1)",
             "('four',   2)",
             "(null,     1)"])

    def test_row_count_with_filter_and_group_by(self):
        metric = {
            'type': 'row_count',
            'filter': {
                'type': 'and',
                'andExpressions': [{
                    'type': 'not',
                    'expression': {
                            'type': 'equals',
                            'left': {
                                'type': 'columnValue',
                                'columnName': 'name'
                            },
                            'right': {
                                'type': 'string',
                                'value': 'one'
                            }
                        }
                    },
                    {
                        'type': 'lessThan',
                        'left': {
                            'type': 'columnValue',
                            'columnName': 'size'
                        },
                        'right': {
                            'type': 'number',
                            'value': 6
                        }
                    }
                ]
            },
            'groupBy': ['name']
        }
        rows = self.execute_metric(self.warehouse, metric)

        logging.debug(str(rows))

        count_by_name = {row[0]: row[1] for row in rows}

        self.assertEqual(count_by_name['two'],   4)
        self.assertEqual(count_by_name['three'], 5)
        self.assertEqual(count_by_name['four'],  2)

    def test_sum_with_filter_and_group_by(self):
        metric = {
            'type': 'sum',
            'columnName': 'size',
            'filter': {
                'type': 'contains',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 't'
                }
            },
            'groupBy': ['name']
        }
        rows = self.execute_metric(self.warehouse, metric)

        logging.debug(str(rows))

        sum_by_name = {row[0]: row[1] for row in rows}

        self.assertEqual(sum_by_name['two'],   10)
        self.assertEqual(sum_by_name['three'], 28)

    def test_sum_with_filter_and_group_by_and_custom_missing(self):
        metric = {
            'type': 'sum',
            'columnName': 'size',
            'filter': {
                'type': 'lessThan',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'size'
                },
                'right': {
                    'type': 'number',
                    'value': 4
                }
            },
            'groupBy': ['name']
        }
        rows = self.execute_metric(self.warehouse, metric, {
            'columns': {
                'size': {
                    'missing_values': [1, 100]
                }
            }
        })

        logging.debug(str(rows))

        sum_by_name = {row[0]: row[1] for row in rows}

        self.assertEqual(sum_by_name['two'],   5)
        self.assertEqual(sum_by_name['three'], 5)
        self.assertEqual(sum_by_name['four'],  2)

        self.assertIsNone(sum_by_name.get('one'))

    def test_contains_expression(self):
        where_expr = self.warehouse.dialect.sql_expression({
                'type': 'contains',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 'ou'
                }
            })
        rows = self.warehouse.sql_fetchall(
            f'SELECT * \n'
            f'FROM {self.default_test_table_name} \n'
            f'WHERE {where_expr}')

        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row[0], 'four')

        where_expr = self.warehouse.dialect.sql_expression({
                'type': 'startsWith',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 'thr'
                }
            })
        rows = self.warehouse.sql_fetchall(
            f'SELECT * \n'
            f'FROM {self.default_test_table_name} \n'
            f'WHERE {where_expr}')

        self.assertEqual(len(rows), 7)
        for row in rows:
            self.assertEqual(row[0], 'three')

        where_expr = self.warehouse.dialect.sql_expression({
                'type': 'endsWith',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 'ee'
                }
            })
        rows = self.warehouse.sql_fetchall(
            f'SELECT * \n'
            f'FROM {self.default_test_table_name} \n'
            f'WHERE {where_expr}')

        self.assertEqual(len(rows), 7)
        for row in rows:
            self.assertEqual(row[0], 'three')
