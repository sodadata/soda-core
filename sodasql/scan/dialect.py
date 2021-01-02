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
import re
from typing import List

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.parse_logs import ParseLogs, ParseConfiguration
from sodasql.scan.scan_configuration import ScanConfiguration

POSTGRES = 'postgres'
SNOWFLAKE = 'snowflake'
REDSHIFT = 'redshift'
BIGQUERY = 'bigquery'
ATHENA = 'athena'

ALL_WAREHOUSE_TYPES = [POSTGRES,
                       SNOWFLAKE,
                       REDSHIFT,
                       BIGQUERY,
                       ATHENA]


class Dialect:

    @classmethod
    def create(cls, warehouse_dict: dict, parse_logs: ParseLogs):
        warehouse_cfg = ParseConfiguration(warehouse_dict, 'warehouse', parse_logs)
        warehouse_type = warehouse_cfg.get_str_required('type')
        if warehouse_type == POSTGRES:
            from sodasql.dialects.postgres_dialect import PostgresDialect
            return PostgresDialect(warehouse_cfg)
        if warehouse_type == SNOWFLAKE:
            from sodasql.dialects.snowflake_dialect import SnowflakeDialect
            return SnowflakeDialect(warehouse_cfg)
        if warehouse_type == REDSHIFT:
            from sodasql.dialects.redshift_dialect import RedshiftDialect
            return RedshiftDialect(warehouse_cfg)
        if warehouse_type == BIGQUERY:
            from sodasql.dialects.bigquery_dialect import BigQueryDialect
            return BigQueryDialect(warehouse_cfg)
        if warehouse_type == ATHENA:
            from sodasql.dialects.athena_dialect import AthenaDialect
            return AthenaDialect(warehouse_cfg)
        else:
            parse_logs.error(f'Unsupported sql warehouse type {warehouse_type}')

    @classmethod
    def is_valid_warehouse_type(cls, warehouse_type):
        return warehouse_type in ALL_WAREHOUSE_TYPES

    @classmethod
    def create_default_configuration_dict(cls, warehouse_type):
        if warehouse_type == POSTGRES:
            from sodasql.dialects.postgres_dialect import PostgresDialect
            return PostgresDialect.create_default_configuration_dict(warehouse_type)
        if warehouse_type == SNOWFLAKE:
            from sodasql.dialects.snowflake_dialect import SnowflakeDialect
            return SnowflakeDialect.create_default_configuration_dict(warehouse_type)
        if warehouse_type == REDSHIFT:
            from sodasql.dialects.redshift_dialect import RedshiftDialect
            return RedshiftDialect.create_default_configuration_dict(warehouse_type)
        if warehouse_type == BIGQUERY:
            from sodasql.dialects.bigquery_dialect import BigQueryDialect
            return BigQueryDialect.create_default_configuration_dict(warehouse_type)
        if warehouse_type == ATHENA:
            from sodasql.dialects.athena_dialect import AthenaDialect
            return AthenaDialect.create_default_configuration_dict(warehouse_type)
        else:
            raise RuntimeError(f'Unsupported sql warehouse type {warehouse_type}')

    def __init__(self):
        self.parse_logs = ParseLogs()

    # TODO
    def sql_connection_test(self):
        pass

    def create_connection(self):
        raise RuntimeError('TODO override and implement this abstract method')

    def create_scan(self, warehouse, scan_configuration):
        # Purpose of this method is to enable dialects to override and customize the scan implementation
        from sodasql.scan.scan import Scan
        return Scan(warehouse, scan_configuration)

    def is_text(self, column):
        for text_type in self._get_text_types():
            if text_type.upper() in column.type.upper():
                return True
        return False

    def _get_text_types(self):
        return ['CHAR', 'TEXT']

    def is_number(self, column):
        for number_type in self._get_number_types():
            if number_type.upper() in column.type.upper():
                return True
        return False

    def _get_number_types(self):
        return ['INT', 'REAL', 'PRECISION', 'NUMBER']

    def sql_columns_metadata_query(self, scan_configuration: ScanConfiguration) -> str:
        raise RuntimeError('TODO override and implement this abstract method')

    def sql_expr_count_all(self) -> str:
        return 'COUNT(*)'

    def sql_expr_count_conditional(self, condition: str):
        return f'COUNT(CASE WHEN {condition} THEN 1 END)'

    def sql_expr_count_column(self, qualified_column_name):
        return f'COUNT({qualified_column_name})'

    def sql_expr_min(self, expr):
        return f'MIN({expr})'

    def sql_expr_length(self, expr):
        return f'LENGTH({expr})'

    def sql_expr_conditional(self, condition: str, expr: str):
        return f'CASE WHEN {condition} THEN {expr} END'

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
        if self.is_text(column):
            sql_values = [self.literal_string(value) for value in values]
        elif self.is_number(column):
            sql_values = [self.literal_number(value) for value in values]
        else:
            raise RuntimeError(f"Couldn't format list {str(values)} for column {str(column)}")
        return '('+','.join(sql_values)+')'

    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS REAL)"
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, '[^-\\d\\.\\,]', '', 'g'), ',', '.', 'g') AS REAL)"

    def literal_number(self, value: str):
        return str(value)

    def literal_string(self, value: str):
        return "'"+str(value).replace("'", "\'")+"'"

    def qualify_table_name(self, table_name: str) -> str:
        return table_name

    def qualify_regex(self, regex):
        return regex

    def qualify_column_name(self, column_name: str):
        return column_name

    def escape_regex_metacharacters(self, regex):
        return re.sub(r'(\\.)', r'\\\1', regex)

    def sql_expression(self, expression_dict: dict):
        type = expression_dict['type']
        if type == 'number':
            sql = str(expression_dict['value'])
        elif type == 'string':
            sql = f"'{expression_dict['value']}'"
        elif type == 'columnValue':
            sql = expression_dict['columnName']
        elif type == 'collection':
            value = expression_dict['value']
            sql = '(' + (', '.join([f"'{x}'" if isinstance(x, str) else str(x) for x in value])) + ')'
        elif type == 'equals':
            sql = self.sql_expression(expression_dict['left']) + ' = ' + self.sql_expression(expression_dict['right'])
        elif type == 'lessThan':
            sql = self.sql_expression(expression_dict['left']) + ' < ' + self.sql_expression(expression_dict['right'])
        elif type == 'lessThanOrEqual':
            sql = self.sql_expression(expression_dict['left']) + ' <= ' + self.sql_expression(expression_dict['right'])
        elif type == 'greaterThan':
            sql = self.sql_expression(expression_dict['left']) + ' > ' + self.sql_expression(expression_dict['right'])
        elif type == 'greaterThanOrEqual':
            sql = self.sql_expression(expression_dict['left']) + ' >= ' + self.sql_expression(expression_dict['right'])
        elif type == 'between':
            clauses = []
            value = self.sql_expression(expression_dict['value'])
            gte = expression_dict.get('gte')
            gt = expression_dict.get('gt')
            lte = expression_dict.get('lte')
            lt = expression_dict.get('lt')
            if gte:
                clauses.append(f'{gte} <= {value}')
            elif gt:
                clauses.append(f'{gt} < {value}')
            if lte:
                clauses.append(f'{value} <= {lte}')
            elif lt:
                clauses.append(f'{value} < {lt}')
            sql = ' AND '.join(clauses)
        elif type == 'in':
            sql = self.sql_expression(expression_dict['left']) + ' IN ' + self.sql_expression(expression_dict['right'])
        elif type == 'contains':
            substring = expression_dict['right']['value']
            sql = self.sql_expression(expression_dict['left']) + " like '%" + substring + "%'"
        elif type == 'startsWith':
            substring = expression_dict['right']['value']
            sql = self.sql_expression(expression_dict['left']) + " like '" + substring + "%'"
        elif type == 'endsWith':
            substring = expression_dict['right']['value']
            sql = self.sql_expression(expression_dict['left']) + " like '%" + substring + "'"
        elif type == 'not':
            sql = 'NOT ( ' + self.sql_expression(expression_dict['expression']) + ' )'
        elif type == 'and':
            sql = '( ' + (' ) AND ( '.join([self.sql_expression(e) for e in expression_dict['andExpressions']])) + ' )'
        elif type == 'or':
            sql = '( ' + (' ) OR ( '.join([self.sql_expression(e) for e in expression_dict['orExpressions']])) + ' )'
        else:
            raise RuntimeError(f'Unsupported expression type: {type}')
        logging.debug('expr sql: '+sql)
        return sql

