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
from sodasql.scan.metric import Metric
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.measurement import Measurement
from sodasql.scan.valid_format import VALID_FORMATS
from sodasql.scan.validity import Validity
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
        measurements.append(Measurement(Metric.SCHEMA, value=columns))
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

        fields.append(self.sql_expr_count_all())
        measurements.append(Measurement(Metric.ROW_COUNT))

        # maps db column names to missing and invalid metric indices in the measurements
        # eg { 'colname': {'missing': 2, 'invalid': 3}, ...}
        column_metric_indices = {}

        for column in columns:
            metric_indices = {}
            column_metric_indices[column.name] = metric_indices

            quoted_column_name = self.qualify_column_name(column.name)

            missing_values = scan_configuration.get_missing_values(column)
            validity = scan_configuration.get_validity(column)

            is_valid_enabled = validity is not None \
                and scan_configuration.is_valid_enabled(column)

            is_missing_enabled = \
                is_valid_enabled \
                or scan_configuration.is_missing_enabled(column)

            missing_condition = self.get_missing_condition(column, missing_values)
            valid_condition = self.get_valid_condition(column, validity)

            non_missing_and_valid_condition = \
                f'NOT {missing_condition} AND {valid_condition}' if valid_condition else f'NOT {missing_condition}'

            if is_missing_enabled:
                metric_indices['missing'] = len(measurements)
                fields.append(f'{self.sql_expr_count_conditional(missing_condition)}')
                measurements.append(Measurement(Metric.MISSING_COUNT, column.name))

            if is_valid_enabled:
                metric_indices['valid'] = len(measurements)
                fields.append(f'{self.sql_expr_count_conditional(non_missing_and_valid_condition)}')
                measurements.append(Measurement(Metric.VALID_COUNT, column.name))

            if scan_configuration.is_min_length_enabled(column):
                if self.is_text(column):
                    fields.append(self.sql_expr_min_conditional(
                        non_missing_and_valid_condition,
                        self.sql_expr_length(quoted_column_name)))
                    measurements.append(Measurement(Metric.MIN_LENGTH, column.name))

        sql = 'SELECT ' + ',\n  '.join(fields) + ' \n' \
              'FROM '+self.qualify_table_name(scan_configuration.table_name)
        if scan_configuration.sample_size:
            sql += f'\nLIMIT {scan_configuration.sample_size}'

        query_result_tuple = self.execute_query_one(sql)

        for i in range(0, len(measurements)):
            measurement = measurements[i]
            measurement.value = query_result_tuple[i]
            logging.debug(f'Query measurement: {measurement}')

        # Calculating derived measurements
        derived_measurements = []
        row_count = measurements[0].value
        for column in columns:
            metric_indices = column_metric_indices[column.name]
            missing_index = metric_indices.get('missing')
            if missing_index is not None:
                missing_count = measurements[missing_index].value
                missing_percentage = missing_count * 100 / row_count
                values_count = row_count - missing_count
                values_percentage = values_count * 100 / row_count
                derived_measurements.append(Measurement(Metric.MISSING_PERCENTAGE, column.name, missing_percentage))
                derived_measurements.append(Measurement(Metric.VALUES_COUNT, column.name, values_count))
                derived_measurements.append(Measurement(Metric.VALUES_PERCENTAGE, column.name, values_percentage))

                valid_index = metric_indices.get('valid')
                if valid_index is not None:
                    valid_count = measurements[valid_index].value
                    invalid_count = row_count - missing_count - valid_count
                    invalid_percentage = invalid_count * 100 / values_count
                    valid_percentage = values_count * 100 / values_count
                    derived_measurements.append(Measurement(Metric.INVALID_PERCENTAGE, column.name, invalid_percentage))
                    derived_measurements.append(Measurement(Metric.INVALID_COUNT, column.name, invalid_count))
                    derived_measurements.append(Measurement(Metric.VALID_PERCENTAGE, column.name, valid_percentage))

        for derived_measurement in derived_measurements:
            logging.debug(f'Derived measurement: {derived_measurement}')

        measurements.extend(derived_measurements)

        return measurements

    def get_missing_condition(self, column: Column, missing_values):
        quoted_column_name = self.qualify_column_name(column.name)
        if missing_values is not None:
            sql_expr_missing_values = self.sql_expr_list(column, missing_values)
            return f'({quoted_column_name} IS NULL OR {quoted_column_name} IN {sql_expr_missing_values})'
        return f'{quoted_column_name} IS NULL'

    def get_valid_condition(self, column: Column, validity: Validity):
        quoted_column_name = self.qualify_column_name(column.name)
        if validity is None:
            return None
        validity_clauses = []
        if validity.format:
            format_regex = VALID_FORMATS.get(validity.format)
            validity_clauses.append(self.sql_expr_regexp_like(quoted_column_name, format_regex))
        if validity.regex:
            validity_clauses.append(self.sql_expr_regexp_like(quoted_column_name, validity.regex))
        if validity.min_length:
            validity_clauses.append(f'{self.sql_expr_length(quoted_column_name)} >= {validity.min_length}')
        if validity.max_length:
            validity_clauses.append(f'{self.sql_expr_length(quoted_column_name)} <= {validity.max_length}')
        # TODO add min and max clauses
        return '(' + ' AND '.join(validity_clauses) + ')'

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
