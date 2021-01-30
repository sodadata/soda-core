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
from unittest import TestCase

from sodasql.scan.dialect import Dialect, POSTGRES


class TestSqlExpressions(TestCase):

    dialect = Dialect('test')

    def test_string(self):
        self.assertEqual("'hello'", self.dialect.sql_expression({
                'type': 'string',
                'value': 'hello'
            }))

    def test_number(self):
        self.assertEqual("93", self.dialect.sql_expression({
                'type': 'number',
                'value': 93
            }))

        self.assertEqual("93.11", self.dialect.sql_expression({
            'type': 'number',
            'value': 93.11
        }))

    def test_column_value(self):
        self.assertEqual("col_name", self.dialect.sql_expression({
                'type': 'columnValue',
                'columnName': 'col_name'
            }))

    def test_collection(self):
        self.assertEqual("('a', 'b', 3, 4)", self.dialect.sql_expression({
                'type': 'collection',
                'value': ['a', 'b', 3, 4]
            }))

    def test_equals(self):
        self.assertEqual("name = 't'", self.dialect.sql_expression({
                'type': 'equals',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 't'
                }
            }))

    def test_less_than(self):
        self.assertEqual("name < 3", self.dialect.sql_expression({
                'type': 'lessThan',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'number',
                    'value': 3
                }
            }))

    def test_less_than_or_equals(self):
        self.assertEqual("name <= 3", self.dialect.sql_expression({
                'type': 'lessThanOrEqual',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'number',
                    'value': 3
                }
            }))

    def test_greater_than(self):
        self.assertEqual("name > 3", self.dialect.sql_expression({
                'type': 'greaterThan',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'number',
                    'value': 3
                }
            }))

    def test_greater_than_or_equals(self):
        self.assertEqual("name >= 3", self.dialect.sql_expression({
                'type': 'greaterThanOrEqual',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'number',
                    'value': 3
                }
            }))

    def test_between(self):
        self.assertEqual("3 <= name AND name < 44", self.dialect.sql_expression({
                'type': 'between',
                'gte': 3,
                'value': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'lt': 44
            }))

        self.assertEqual("3 < name", self.dialect.sql_expression({
                'type': 'between',
                'gt': 3,
                'value': {
                    'type': 'columnValue',
                    'columnName': 'name'
                }
            }))

        self.assertEqual("name <= 44", self.dialect.sql_expression({
                'type': 'between',
                'value': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'lte': 44
            }))

    def test_in(self):
        self.assertEqual("name IN ('a', 'b', 'c')", self.dialect.sql_expression({
                'type': 'in',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'collection',
                    'value': ['a', 'b', 'c']
                }
            }))

    def test_contains(self):
        self.assertEqual("name LIKE '%t%'", self.dialect.sql_expression({
                'type': 'contains',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 't'
                }
            }))

    def test_starts_with(self):
        self.assertEqual("name LIKE 't%'", self.dialect.sql_expression({
                'type': 'startsWith',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 't'
                }
            }))

    def test_ends_with(self):
        self.assertEqual("name LIKE '%t'", self.dialect.sql_expression({
                'type': 'endsWith',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 't'
                }
            }))

    def test_not(self):
        self.assertEqual("NOT ( name = 't' )", self.dialect.sql_expression({
                'type': 'not',
                'expression': {
                    'type': 'equals',
                    'left': {
                        'type': 'columnValue',
                        'columnName': 'name'
                    },
                    'right': {
                        'type': 'string',
                        'value': 't'
                    }
                }
            }))

    def test_and(self):
        self.assertEqual("( name = 't' ) AND ( size = 3 )", self.dialect.sql_expression({
                'type': 'and',
                'andExpressions': [{
                        'type': 'equals',
                        'left': {
                            'type': 'columnValue',
                            'columnName': 'name'
                        },
                        'right': {
                            'type': 'string',
                            'value': 't'
                        }
                    }, {
                        'type': 'equals',
                        'left': {
                            'type': 'columnValue',
                            'columnName': 'size'
                        },
                        'right': {
                            'type': 'number',
                            'value': 3
                        }
                    }
            ]}))

    def test_or(self):
        self.assertEqual("( name = 't' ) OR ( size = 3 )", self.dialect.sql_expression({
            'type': 'or',
            'orExpressions': [{
                'type': 'equals',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'name'
                },
                'right': {
                    'type': 'string',
                    'value': 't'
                }
            }, {
                'type': 'equals',
                'left': {
                    'type': 'columnValue',
                    'columnName': 'size'
                },
                'right': {
                    'type': 'number',
                    'value': 3
                }
            }
            ]}))

