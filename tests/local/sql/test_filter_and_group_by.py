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

from sodasql.scan.warehouse import Warehouse
from tests.common.sql_test_case import SqlTestCase


def filter_sql_expression(expression: dict):
    type = expression['type']
    if type == 'not':
        sql = 'NOT ( ' + filter_sql_expression(expression['expression']) + ' )'
    elif type == 'and':
        sql = '\n      AND '.join([filter_sql_expression(e) for e in expression['andExpressions']])
    elif type == 'or':
        sql = '( ' + ('\n        OR '.join([filter_sql_expression(e) for e in expression['orExpressions']])) + ' )'
    elif type == 'equals':
        sql = filter_sql_expression(expression['left']) + ' = ' + filter_sql_expression(expression['right'])
    elif type == 'lessThan':
        sql = filter_sql_expression(expression['left']) + ' < ' + filter_sql_expression(expression['right'])
    elif type == 'contains':
        substring = expression['right']['value']
        sql = filter_sql_expression(expression['left']) + " like '%" + substring + "%'"
    elif type == 'number':
        sql = str(expression['value'])
    elif type == 'string':
        sql = f"'{expression['value']}'"
    elif type == 'columnValue':
        sql = expression['columnName']
    else:
        raise RuntimeError(f'Unsupported expression type: {type}')
    logging.debug('expr sql: '+sql)
    return sql


def execute_metric(warehouse: Warehouse, metric: dict, table_name: str):
    dialect = warehouse.dialect

    qualified_table_name = dialect.qualify_table_name(table_name)

    fields: List[str] = []
    group_by_column_names: List[str] = metric.get('groupBy')
    if group_by_column_names:
        for group_by_column in group_by_column_names:
            fields.append(dialect.qualify_column_name(group_by_column))

    column_name: str = metric.get('columnName')
    qualified_column_name = dialect.qualify_column_name(column_name)

    type = metric['type']
    if type == 'row_count':
        fields.append('COUNT(*)')
    elif type == 'min':
        fields.append(f'MIN({qualified_column_name})')
    elif type == 'max':
        fields.append(f'MAX({qualified_column_name})')
    elif type == 'sum':
        fields.append(f'SUM({qualified_column_name})')

    sql = 'SELECT \n  ' + ',\n  '.join(fields) + ' \n' \
          'FROM ' + qualified_table_name

    filter = metric.get('filter')

    if filter:
        sql += '\nWHERE ' + filter_sql_expression(filter)

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
        rows = execute_metric(self.warehouse, metric, self.default_test_table_name)

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
        rows = execute_metric(self.warehouse, metric, self.default_test_table_name)

        logging.debug(str(rows))

        sum_by_name = {row[0]: row[1] for row in rows}

        self.assertEqual(sum_by_name['two'],   10)
        self.assertEqual(sum_by_name['three'], 28)


