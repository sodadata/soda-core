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
from typing import List

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.parse_logs import ParseLogs
from sodasql.scan.scan_configuration import ScanConfiguration

POSTGRES = 'postgres'
SNOWFLAKE = 'snowflake'
REDSHIFT = 'redshift'
BIGQUERY = 'bigquery'


class Dialect:

    @classmethod
    def create(cls, warehouse_configuration: dict, parse_logs: ParseLogs):
        warehouse_type = warehouse_configuration['type']
        if warehouse_type == POSTGRES:
            from sodasql.dialects.postgres_dialect import PostgresDialect
            return PostgresDialect(warehouse_configuration, parse_logs)
        if warehouse_type == SNOWFLAKE:
            from sodasql.dialects.snowflake_dialect import SnowflakeDialect
            return SnowflakeDialect(warehouse_configuration, parse_logs)
        if warehouse_type == REDSHIFT:
            from sodasql.dialects.redshift_dialect import RedshiftDialect
            return RedshiftDialect(warehouse_configuration, parse_logs)
        if warehouse_type == BIGQUERY:
            from sodasql.dialects.bigquery_dialect import BigQueryDialect
            return BigQueryDialect(warehouse_configuration, parse_logs)
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
        return ['INT', 'REAL', 'PRECISION']

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
