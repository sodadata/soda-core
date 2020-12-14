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

from sodasql.scan.column import Column
from sodasql.scan.custom_metric import CustomMetric
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.measurement import Measurement
from sodasql.soda_client.soda_client import SodaClient
from sodasql.sql_store.sql_statement_logger import log_sql


class SqlStore:

    def __init__(self, connection_dict: dict):
        self.name = connection_dict.get('name')
        self.connection = None

    @classmethod
    def create(cls, connection_dict: dict):
        sql_store_type = connection_dict['type']
        if sql_store_type == 'postgres':
            from sodasql.sql_store.postgres_sql_store import PostgresSqlStore
            return PostgresSqlStore(connection_dict)
        else:
            raise RuntimeError(f'Unsupported sql store type {sql_store_type}')

    def create_connection(self):
        raise RuntimeError('TODO override and implement this abstract method')

    def sql_columns_metadata_query(self, scan_configuration: ScanConfiguration) -> str:
        raise RuntimeError('TODO override and implement this abstract method')

    def scan(self,
             scan_configuration: ScanConfiguration = None,
             custom_metrics: List[CustomMetric] = None,
             soda_client: SodaClient = None):

        assert self.name, 'name is required'
        assert scan_configuration.table_name, 'scan_configuration.table_name is required'
        scan_reference = {
            'sql_store_name': self.name,
            'table_name': scan_configuration.table_name
        }

        measurements: List[Measurement] = []

        columns: List[Column] = self.query_columns(scan_configuration)
        measurements.append(Measurement(Measurement.TYPE_SCHEMA, value=columns))
        if soda_client:
            soda_client.send_columns(scan_reference, columns)

        if scan_configuration:
            columns_aggregation_measurements: List[Measurement] = \
                self.query_aggregations(scan_configuration, columns)
            measurements.extend(columns_aggregation_measurements)
            if soda_client:
                soda_client.send_column_aggregation_measurements(scan_reference, columns_aggregation_measurements)

        return measurements

    def query_columns(self, scan_configuration: ScanConfiguration) -> List[Column]:
        sql = self.sql_columns_metadata_query(scan_configuration)
        column_tuples = self.execute_query_all(sql)
        columns = []
        for column_tuple in column_tuples:
            name = column_tuple[0]
            type = column_tuple[1]
            nullable = 'YES' == column_tuple[2].upper()
            columns.append(Column(name, type, nullable))
        logging.debug(str(len(columns))+' columns:')
        for column in columns:
            logging.debug(f'  {column.name} {column.type} {"" if column.nullable else "not null"}')
        return columns

    def query_aggregations(
            self,
            scan_configuration: ScanConfiguration,
            columns: List[Column]) -> List[Measurement]:

        fields: List[str] = []
        measurements: List[Measurement] = []

        if scan_configuration.is_row_count_enabled():
            fields.append(self.sql_expr_count_all())
            measurements.append(Measurement(Measurement.TYPE_ROW_COUNT))

        for column in columns:
            quoted_column_name = self.qualify_column_name(column.name)

            missing_condition = self.get_missing_condition(column, scan_configuration)
            non_missing_condition = 'NOT ' + missing_condition
            non_missing_and_valid_condition = non_missing_condition
            valid_condition = self.get_valid_condition(column, scan_configuration)
            invalid_condition = None
            if valid_condition:
                invalid_condition = f'NOT {valid_condition}'
                non_missing_and_valid_condition = f'({non_missing_condition}) AND ({valid_condition})'

            if scan_configuration.is_missing_enabled(column):
                fields.append(f'{self.sql_expr_count_conditional(missing_condition)}')
                measurements.append(Measurement(Measurement.TYPE_MISSING_COUNT, column))

            if scan_configuration.is_invalid_enabled(column):
                if invalid_condition:
                    fields.append(f'{self.sql_expr_count_conditional(invalid_condition)}')
                    measurements.append(Measurement(Measurement.TYPE_INVALID_COUNT, column))
                else:
                    fields.append(f'{self.sql_expr_count_conditional(non_missing_condition)}')
                    measurements.append(Measurement(Measurement.TYPE_INVALID_COUNT, column))

            if scan_configuration.is_min_length_enabled(column):
                if self.is_text(column):
                    fields.append(self.sql_expr_min_conditional(
                        non_missing_and_valid_condition,
                        self.sql_expr_length(quoted_column_name)))
                    measurements.append(Measurement(Measurement.TYPE_MIN_LENGTH, column))

        sql = 'SELECT ' + ',\n  '.join(fields) + ' \n' \
              'FROM '+self.qualify_table_name(scan_configuration.table_name)
        if scan_configuration.sample_size:
            sql += f'\nLIMIT {scan_configuration.sample_size}'

        query_result_tuple = self.execute_query_one(sql)

        for i in range(0, len(measurements)):
            measurement = measurements[i]
            measurement.value = query_result_tuple[i]
            logging.debug(measurement)

        return measurements

    def get_missing_condition(self, column: Column, scan_configuration: ScanConfiguration):
        quoted_column_name = self.qualify_column_name(column.name)
        condition = f'{quoted_column_name} IS NULL'
        if self.is_text(column) or self.is_number(column):
            missing_values = scan_configuration.get_missing_values(column)
            if missing_values:
                sql_expr_missing_values = self.sql_expr_list(column, missing_values)
                condition = f'({condition} AND {quoted_column_name} NOT IN {sql_expr_missing_values})'
        return condition

    def get_valid_condition(self, column: Column, scan_configuration: ScanConfiguration):
        if self.is_text(column):
            valid_regex = scan_configuration.get_valid_regex(column)
            if valid_regex:
                quoted_column_name = self.qualify_column_name(column.name)
                return self.sql_expr_regexp_like(quoted_column_name, valid_regex)
        return None

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

    def sql_expr_count_all(self) -> str:
        return 'COUNT(*)'

    def sql_expr_min(self, expr):
        return f'MIN({expr})'

    def sql_expr_length(self, expr):
        return f'LENGTH({expr})'

    def sql_expr_count_conditional(self, condition: str):
        return f'COUNT(CASE WHEN {condition} THEN 1 END)'

    def sql_expr_min_conditional(self, condition: str, expr: str):
        return f'MIN(CASE WHEN {condition} THEN {expr} END)'

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"{expr} ~* '{self.qualify_regex(pattern)}'"

    def sql_expr_list(self, column: Column, values: List[str]) -> str:
        if self.is_text(column):
            sql_values = [self.literal_string(value) for value in values]
        elif self.is_number(column):
            sql_values = [self.literal_string(value) for value in values]
        else:
            raise RuntimeError(f"Couldn't format list {str(values)} for column {str(column)}")
        return '('+','.join(sql_values)+')'

    def literal_number(self, value: str):
        return value

    def literal_string(self, value: str):
        return "'"+str(value).replace("'", "\'")+"'"

    def qualify_table_name(self, table_name: str) -> str:
        return table_name

    def qualify_regex(self, regex):
        return regex

    def qualify_column_name(self, column_name: str):
        return column_name

    def execute_query_one(self, sql):
        with self.get_connection().cursor() as cursor, log_sql(sql):
            cursor.execute(sql)
            return cursor.fetchone()

    def execute_query_all(self, sql):
        with self.get_connection().cursor() as cursor, log_sql(sql):
            cursor.execute(sql)
            return cursor.fetchall()

    def get_connection(self):
        if not self.connection:
            self.connection = self.create_connection()
        return self.connection

