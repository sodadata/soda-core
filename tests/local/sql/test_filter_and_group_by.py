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
from typing import List

from sodasql.scan.metric import Metric
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan_parse import ScanParse
from sodasql.scan.warehouse import Warehouse
from tests.common.sql_test_case import SqlTestCase


def execute_metric(warehouse: Warehouse, metric: dict, scan_dict):
    dialect = warehouse.dialect

    scan_parse = ScanParse(scan_dict=scan_dict)
    scan_parse.parse_logs.assert_no_warnings_or_errors()
    scan = warehouse.create_scan(scan_parse.scan_configuration)
    scan.execute()

    fields: List[str] = []
    group_by_column_names: List[str] = metric.get('groupBy')
    if group_by_column_names:
        for group_by_column in group_by_column_names:
            fields.append(dialect.qualify_column_name(group_by_column))

    column_name: str = metric.get('columnName')
    qualified_column_name = dialect.qualify_column_name(column_name)

    type = metric['type']
    if type == Metric.ROW_COUNT:
        fields.append('COUNT(*)')
    if type == Metric.MIN:
        fields.append(f'MIN({qualified_column_name})')
    elif type == Metric.MAX:
        fields.append(f'MAX({qualified_column_name})')
    elif type == Metric.SUM:
        fields.append(f'SUM({qualified_column_name})')

    sql = 'SELECT \n  ' + ',\n  '.join(fields) + ' \n' \
          'FROM ' + scan.qualified_table_name

    where_clauses = []

    filter = metric.get('filter')
    if filter:
        where_clauses.append(dialect.sql_expression(filter))

    scan_column: ScanColumn = scan.scan_columns.get(column_name)
    if scan_column and scan_column.non_missing_and_valid_condition:
        where_clauses.append(scan_column.non_missing_and_valid_condition)

    if where_clauses:
        sql += '\nWHERE ' + '\n      AND '.join(where_clauses)

    if group_by_column_names:
        sql += '\nGROUP BY ' + ', '.join(group_by_column_names)

    return warehouse.sql_fetchall(sql)


class FilterAndGroupByTest(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_create_table(
            self.default_test_table_name,
            ["name VARCHAR(255)",
             "size INTEGER"],
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
        rows = execute_metric(self.warehouse, metric, {
            'table_name': self.default_test_table_name
        })

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
        rows = execute_metric(self.warehouse, metric, {
            'table_name': self.default_test_table_name
        })

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
        rows = execute_metric(self.warehouse, metric, {
            'table_name': self.default_test_table_name,
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
