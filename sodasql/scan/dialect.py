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
import re
from datetime import date
from numbers import Number
from typing import List

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.parser import Parser

KEY_WAREHOUSE_TYPE = 'type'

POSTGRES = 'postgres'
SNOWFLAKE = 'snowflake'
REDSHIFT = 'redshift'
BIGQUERY = 'bigquery'
ATHENA = 'athena'
SQLSERVER = 'sqlserver'

ALL_WAREHOUSE_TYPES = [POSTGRES,
                       SNOWFLAKE,
                       REDSHIFT,
                       BIGQUERY,
                       ATHENA,
                       SQLSERVER]


class Dialect:

    data_type_varchar_255 = "VARCHAR(255)"
    data_type_integer = "INTEGER"
    data_type_bigint = "BIGINT"
    data_type_decimal = "REAL"
    data_type_date = "DATE"

    def __init__(self, type: str):
        self.type = type

    @classmethod
    def create(cls, parser: Parser):
        warehouse_type = parser.get_str_optional(KEY_WAREHOUSE_TYPE)
        if warehouse_type == POSTGRES:
            from sodasql.dialects.postgres_dialect import PostgresDialect
            return PostgresDialect(parser)
        if warehouse_type == SNOWFLAKE:
            from sodasql.dialects.snowflake_dialect import SnowflakeDialect
            return SnowflakeDialect(parser)
        if warehouse_type == REDSHIFT:
            from sodasql.dialects.redshift_dialect import RedshiftDialect
            return RedshiftDialect(parser)
        if warehouse_type == BIGQUERY:
            from sodasql.dialects.bigquery_dialect import BigQueryDialect
            return BigQueryDialect(parser)
        if warehouse_type == ATHENA:
            from sodasql.dialects.athena_dialect import AthenaDialect
            return AthenaDialect(parser)
        if warehouse_type == SQLSERVER:
            from sodasql.dialects.sqlserver_dialect import SQLServerDialect
            return SQLServerDialect(parser)

    @classmethod
    def create_for_warehouse_type(cls, warehouse_type):
        from sodasql.scan.dialect_parser import DialectParser
        return cls.create(DialectParser(warehouse_connection_dict={KEY_WAREHOUSE_TYPE: warehouse_type}))

    def default_connection_properties(self, params: dict):
        pass

    def default_env_vars(self, params: dict):
        pass

    def sql_connection_test(self):
        return "select 1"

    def create_connection(self, *args, **kwargs):
        raise RuntimeError('TODO override and implement this abstract method')

    def create_scan(self, *args, **kwargs):
        # Purpose of this method is to enable dialects to override and customize the scan implementation
        from sodasql.scan.scan import Scan
        return Scan(*args, **kwargs)

    def is_text(self, column_type: str):
        for text_type in self._get_text_types():
            if column_type and text_type.upper() in column_type.upper():
                return True
        return False

    def _get_text_types(self):
        return ['CHAR', 'TEXT', 'STRING']

    def is_number(self, column_type: str):
        for number_type in self._get_number_types():
            if column_type and number_type.upper() in column_type.upper():
                return True
        return False

    def _get_number_types(self):
        return ['INT', 'REAL', 'PRECISION', 'NUMBER', 'DECIMAL']

    def is_time(self, column_type: str):
        for time_type in self._get_time_types():
            if column_type and time_type.upper() in column_type.upper():
                return True
        return False

    def _get_time_types(self):
        return ['TIMESTAMP', 'DATE', 'TIME']

    def sql_columns_metadata_query(self, table_name: str) -> str:
        raise RuntimeError('TODO override and implement this abstract method')

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        raise RuntimeError('TODO override and implement this abstract method')

    def sql_create_table(self, table_name: str, column_declarations: List[str]):
        columns_sql = ",\n  ".join(column_declarations)
        return f"CREATE TABLE " \
               f"{self.qualify_writable_table_name(table_name)} ( \n" \
               f"  {columns_sql} )"

    def sql_insert_into(self, table_name, rows: list):
        rows_sql = ',\n  '.join(rows)
        return (f'INSERT INTO '
                f"{self.qualify_writable_table_name(table_name)} VALUES \n"
                f"  {rows_sql}")

    def sql_drop_table(self, table_name):
        return f"DROP TABLE IF EXISTS {self.qualify_writable_table_name(table_name)}"

    def sql_expr_count_all(self) -> str:
        return 'COUNT(*)'

    def sql_expr_count_conditional(self, condition: str):
        return f'COUNT(CASE WHEN {condition} THEN 1 END)'

    def sql_expr_conditional(self, condition: str, expr: str):
        return f'CASE WHEN {condition} THEN {expr} END'

    def sql_expr_count(self, expr):
        return f'COUNT({expr})'

    def sql_expr_distinct(self, expr):
        return f'DISTINCT({expr})'

    def sql_expr_min(self, expr):
        return f'MIN({expr})'

    def sql_expr_length(self, expr):
        return f'LENGTH({expr})'

    def sql_expr_min(self, expr: str):
        return f'MIN({expr})'

    def sql_expr_max(self, expr: str):
        return f'MAX({expr})'

    def sql_expr_avg(self, expr: str):
        return f'AVG({expr})'

    def sql_expr_sum(self, expr: str):
        return f'SUM({expr})'

    def sql_expr_variance(self, expr: str):
        return f'VARIANCE({expr})'

    def sql_expr_stddev(self, expr: str):
        return f'STDDEV({expr})'

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"REGEXP_LIKE({expr}, '{self.qualify_regex(pattern)}')"

    def sql_expr_list(self, column: ColumnMetadata, values: List[str]) -> str:
        if self.is_text(column.type):
            sql_values = [self.literal_string(value) for value in values]
        elif self.is_number(column.type):
            sql_values = [self.literal_number(value) for value in values]
        else:
            raise RuntimeError(f"Couldn't format list {str(values)} for column {str(column)}")
        return '('+','.join(sql_values)+')'

    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS {self.data_type_decimal})"
        not_number_pattern = self.qualify_regex(r"[^-\d\.\,]")
        comma_pattern = self.qualify_regex(r"\,")
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, '{not_number_pattern}', ''), "\
               f"'{comma_pattern}', '.') AS {self.data_type_decimal})"

    def literal_number(self, value: Number):
        if value is None:
            return None
        return str(value)

    def literal_string(self, value: str):
        if value is None:
            return None
        return "'"+self.escape_metacharacters(value)+"'"

    def literal_list(self, l: list):
        if l is None:
            return None
        return '(' + (','.join([self.literal(e) for e in l])) + ')'

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE '{date_string}'"

    def literal(self, o: object):
        if isinstance(o, Number):
            return self.literal_number(o)
        elif isinstance(o, str):
            return self.literal_string(o)
        elif isinstance(o, list) or isinstance(o, set) or isinstance(o, tuple):
            return self.literal_list(o)
        raise RuntimeError(f'Cannot convert type {type(o)} to a SQL literal: {o}')

    def qualify_table_name(self, table_name: str) -> str:
        return table_name

    def qualify_column_name(self, column_name: str):
        return column_name

    def qualify_writable_table_name(self, table_name: str) -> str:
        return table_name

    def qualify_regex(self, regex):
        return regex

    def qualify_string(self, value: str):
        return value

    @staticmethod
    def escape_metacharacters(value: str):
        return re.sub(r'(\\.)', r'\\\1', value)

    def sql_declare_string_column(self, column_name):
        return f"{column_name} {self.data_type_varchar_255}"

    def sql_declare_integer_column(self, column_name):
        return f"{column_name} {self.data_type_integer}"

    def sql_declare_decimal_column(self, column_name):
        return f"{column_name} {self.data_type_decimal}"

    def sql_declare_big_integer_column(self, column_name):
        return f"{column_name} {self.data_type_bigint}"

    def sql_expression(self, expression_dict: dict):
        if expression_dict is None:
            return None
        type = expression_dict['type']
        if type == 'number':
            sql = self.literal_number(expression_dict['value'])
        elif type == 'string':
            sql = self.literal_string(expression_dict['value'])
        elif type == 'columnValue':
            sql = expression_dict['columnName']
        elif type == 'collection':
            # collection of string or number literals
            value = expression_dict['value']
            sql = self.literal_list(value)
        elif type == 'equals':
            left = self.sql_expression(expression_dict['left'])
            right = self.sql_expression(expression_dict['right'])
            sql = self.sql_expr_equal(left, right)
        elif type == 'lessThan':
            left = self.sql_expression(expression_dict['left'])
            right = self.sql_expression(expression_dict['right'])
            sql = self.sql_expr_less_than(left, right)
        elif type == 'lessThanOrEqual':
            left = self.sql_expression(expression_dict['left'])
            right = self.sql_expression(expression_dict['right'])
            sql = self.sql_expr_less_than_or_equal(left, right)
        elif type == 'greaterThan':
            left = self.sql_expression(expression_dict['left'])
            right = self.sql_expression(expression_dict['right'])
            sql = self.sql_expr_greater_than(left, right)
        elif type == 'greaterThanOrEqual':
            left = self.sql_expression(expression_dict['left'])
            right = self.sql_expression(expression_dict['right'])
            sql = self.sql_expr_greater_than_or_equal(left, right)
        elif type == 'between':
            clauses = []
            value = self.sql_expression(expression_dict['value'])
            gte = self.literal_number(expression_dict.get('gte'))
            gt = self.literal_number(expression_dict.get('gt'))
            lte = self.literal_number(expression_dict.get('lte'))
            lt = self.literal_number(expression_dict.get('lt'))
            if gte:
                clauses.append(self.sql_expr_less_than_or_equal(gte, value))
            elif gt:
                clauses.append(self.sql_expr_less_than(gt, value))
            if lte:
                clauses.append(self.sql_expr_less_than_or_equal(value, lte))
            elif lt:
                clauses.append(self.sql_expr_less_than(value, lt))
            sql = ' AND '.join(clauses)
        elif type == 'in':
            left = self.sql_expression(expression_dict['left'])
            right = self.sql_expression(expression_dict['right'])
            sql = self.sql_expr_in(left, right)
        elif type == 'contains':
            value = self.sql_expression(expression_dict['left'])
            substring = self.escape_metacharacters(expression_dict['right']['value'])
            sql = self.sql_expr_contains(value, substring)
        elif type == 'startsWith':
            value = self.sql_expression(expression_dict['left'])
            substring = self.escape_metacharacters(expression_dict['right']['value'])
            sql = self.sql_expr_starts_with(value, substring)
        elif type == 'endsWith':
            value = self.sql_expression(expression_dict['left'])
            substring = self.escape_metacharacters(expression_dict['right']['value'])
            sql = self.sql_expr_ends_with(value, substring)
        elif type == 'not':
            sql = 'NOT (' + self.sql_expression(expression_dict['expression']) + ')'
        elif type == 'and':
            sql = '(' + (') AND ('.join([self.sql_expression(e) for e in expression_dict['andExpressions']])) + ')'
        elif type == 'or':
            sql = '(' + (') OR ('.join([self.sql_expression(e) for e in expression_dict['orExpressions']])) + ')'
        else:
            raise RuntimeError(f'Unsupported expression type: {type}')
        return sql

    def sql_expr_equal(self, left, right):
        return f'{left} = {right}'

    def sql_expr_less_than(self, left, right):
        return f'{left} < {right}'

    def sql_expr_less_than_or_equal(self, left, right):
        return f'{left} <= {right}'

    def sql_expr_greater_than(self, left, right):
        return f'{left} > {right}'

    def sql_expr_greater_than_or_equal(self, left, right):
        return f'{left} >= {right}'

    def sql_expr_in(self, left, right):
        return f'{left} IN {right}'

    def sql_expr_contains(self, value, substring):
        return value + " LIKE '%" + substring + "%'"

    def sql_expr_starts_with(self, value, substring):
        return value + " LIKE '" + substring + "%'"

    def sql_expr_ends_with(self, value, substring):
        return value + " LIKE '%" + substring + "'"

    def get_type_name(self, column_description):
        return str(column_description[1])

    def is_connection_error(self, exception):
        return False

    def is_authentication_error(self, exception):
        return False
