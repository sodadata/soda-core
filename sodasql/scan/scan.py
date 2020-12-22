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
from typing import List, Optional

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.custom_metric import CustomMetric
from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.test_result import TestResult
from sodasql.soda_client.soda_client import SodaClient
from sodasql.warehouse.dialect import Dialect
from sodasql.warehouse.warehouse import Warehouse


class Scan:

    def __init__(self,
                 warehouse: Warehouse,
                 scan_configuration: ScanConfiguration = None,
                 custom_metrics: List[CustomMetric] = None,
                 soda_client: SodaClient = None):
        self.soda_client: SodaClient = soda_client
        self.warehouse: Warehouse = warehouse
        self.dialect: Dialect = warehouse.dialect
        self.configuration: ScanConfiguration = scan_configuration
        self.custom_metrics: List[CustomMetric] = custom_metrics

        self.scan_result = ScanResult()

        self.qualified_table_name = self.dialect.qualify_table_name(scan_configuration.table_name)
        self.table_sample_clause = \
            f'\nTABLESAMPLE {scan_configuration.sample_method}({scan_configuration.sample_percentage})' \
            if scan_configuration.sample_percentage \
            else ''

        self.scan_reference = {
            'warehouse': self.warehouse.name,
            'table_name': self.configuration.table_name,
            'scan_id': 'TODO-generate-an-ID'
        }
        self.columns: List[ColumnMetadata] = []
        self.column_names: List[str] = []
        # maps column names to ScanColumn's
        self.scan_columns: dict = {}

    def execute(self):
        assert self.warehouse.name, 'warehouse.name is required'
        assert self.configuration.table_name, 'scan_configuration.table_name is required'

        self.columns: List[ColumnMetadata] = self.query_columns_metadata()
        self.column_names: List[str] = [column_metadata.name for column_metadata in self.columns]

        schema_measurements = [Measurement(Metric.SCHEMA, value=self.columns)]
        self.add_measurements(schema_measurements)

        self.scan_columns: dict = \
            {column.name: ScanColumn(self, column) for column in self.columns}

        aggregation_measurements: List[Measurement] = \
            self.query_aggregations()
        self.add_measurements(aggregation_measurements)

        group_by_measurements: List[Measurement] = \
            self.query_group_by_value()
        self.add_measurements(group_by_measurements)

        test_results: List[TestResult] = self.run_tests()
        self.scan_result.test_results.extend(test_results)

        return self.scan_result

    def query_columns_metadata(self) -> List[ColumnMetadata]:
        sql = self.warehouse.dialect.sql_columns_metadata_query(self.configuration)
        column_tuples = self.warehouse.execute_query_all(sql)
        columns = []
        for column_tuple in column_tuples:
            name = column_tuple[0]
            type = column_tuple[1]
            nullable = 'YES' == column_tuple[2].upper()
            columns.append(ColumnMetadata(name, type, nullable))
        logging.debug(str(len(columns))+' columns:')
        for column in columns:
            logging.debug(f'  {column.name} {column.type} {"" if column.nullable else "not null"}')
        return columns

    def query_aggregations(self) -> List[Measurement]:
        measurements: List[Measurement] = []

        fields: List[str] = []

        dialect = self.warehouse.dialect
        fields.append(dialect.sql_expr_count_all())
        measurements.append(Measurement(Metric.ROW_COUNT))

        # maps db column names to missing and invalid metric indices in the measurements
        # eg { 'colname': {'missing': 2, 'invalid': 3}, ...}
        column_metric_indices = {}

        for column_name in self.column_names:
            metric_indices = {}
            column_metric_indices[column_name] = metric_indices

            scan_column: ScanColumn = self.scan_columns[column_name]

            if scan_column.is_missing_enabled:
                metric_indices['missing'] = len(measurements)
                fields.append(f'{dialect.sql_expr_count_conditional(scan_column.missing_condition)}')
                measurements.append(Measurement(Metric.MISSING_COUNT, column_name))

            if scan_column.is_valid_enabled:
                metric_indices['valid'] = len(measurements)
                fields.append(f'{dialect.sql_expr_count_conditional(scan_column.non_missing_and_valid_condition)}')
                measurements.append(Measurement(Metric.VALID_COUNT, column_name))

            if scan_column.is_text:
                if self.configuration.is_metric_enabled(column_name, Metric.MIN_LENGTH):
                    length_expr = dialect.sql_expr_conditional(
                        scan_column.non_missing_and_valid_condition,
                        dialect.sql_expr_length(scan_column.qualified_column_name))
                    fields.append(dialect.sql_expr_min(length_expr))
                    measurements.append(Measurement(Metric.MIN_LENGTH, column_name))

                if self.configuration.is_metric_enabled(column_name, Metric.MAX_LENGTH):
                    length_expr = dialect.sql_expr_conditional(
                        scan_column.non_missing_and_valid_condition,
                        dialect.sql_expr_length(scan_column.qualified_column_name))
                    fields.append(dialect.sql_expr_max(length_expr))
                    measurements.append(Measurement(Metric.MAX_LENGTH, column_name))

            if scan_column.is_number or scan_column.is_column_numeric_text_format:
                if self.configuration.is_metric_enabled(column_name, Metric.MIN):
                    fields.append(dialect.sql_expr_min(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.MIN, column_name))

                if self.configuration.is_metric_enabled(column_name, Metric.MAX):
                    fields.append(dialect.sql_expr_max(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.MAX, column_name))

                if self.configuration.is_metric_enabled(column_name, Metric.AVG):
                    fields.append(dialect.sql_expr_avg(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.AVG, column_name))

                if self.configuration.is_metric_enabled(column_name, Metric.SUM):
                    fields.append(dialect.sql_expr_sum(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.SUM, column_name))

        sql = 'SELECT \n  ' + ',\n  '.join(fields) + ' \n' \
              'FROM ' + self.qualified_table_name
        if self.table_sample_clause:
            sql += f'\n{self.table_sample_clause}'

        query_result_tuple = self.warehouse.execute_query_one(sql)

        for i in range(0, len(measurements)):
            measurement = measurements[i]
            measurement.value = query_result_tuple[i]
            logging.debug(f'Query measurement: {measurement}')

        # Calculating derived measurements
        derived_measurements = []
        row_count = measurements[0].value
        for column_name in self.column_names:
            metric_indices = column_metric_indices[column_name]
            missing_index = metric_indices.get('missing')
            if missing_index is not None:
                missing_count = measurements[missing_index].value
                missing_percentage = missing_count * 100 / row_count
                values_count = row_count - missing_count
                values_percentage = values_count * 100 / row_count
                derived_measurements.append(Measurement(Metric.MISSING_PERCENTAGE, column_name, missing_percentage))
                derived_measurements.append(Measurement(Metric.VALUES_COUNT, column_name, values_count))
                derived_measurements.append(Measurement(Metric.VALUES_PERCENTAGE, column_name, values_percentage))

                valid_index = metric_indices.get('valid')
                if valid_index is not None:
                    valid_count = measurements[valid_index].value
                    invalid_count = row_count - missing_count - valid_count
                    invalid_percentage = invalid_count * 100 / row_count
                    valid_percentage = valid_count * 100 / row_count
                    derived_measurements.append(Measurement(Metric.INVALID_PERCENTAGE, column_name, invalid_percentage))
                    derived_measurements.append(Measurement(Metric.INVALID_COUNT, column_name, invalid_count))
                    derived_measurements.append(Measurement(Metric.VALID_PERCENTAGE, column_name, valid_percentage))

        for derived_measurement in derived_measurements:
            logging.debug(f'Derived measurement: {derived_measurement}')

        measurements.extend(derived_measurements)

        return measurements

    def query_group_by_value(self):
        measurements: List[Measurement] = []

        for column_name in self.column_names:
            scan_column: ScanColumn = self.scan_columns[column_name]

            if scan_column.is_any_metric_enabled([
                    Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT,
                    Metric.MINS, Metric.MAXS, Metric.FREQUENT_VALUES]):

                group_by_cte = scan_column.get_group_by_cte()

                numeric_value_expr = 'value' \
                    if scan_column.is_number \
                    else self.dialect.sql_expr_cast_text_to_number('value', scan_column.validity_format)

                if self.configuration.is_any_metric_enabled(column_name, [
                        Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT]):

                    sql = (f'{group_by_cte} \n'
                           f'SELECT COUNT(*), \n'
                           f'       COUNT(CASE WHEN frequency = 1 THEN 1 END) \n'
                           f'FROM group_by_value')

                    query_result_tuple = self.warehouse.execute_query_one(sql)
                    distinct_count = query_result_tuple[0]
                    measurement = Measurement(Metric.DISTINCT, column_name, distinct_count)
                    measurements.append(measurement)
                    logging.debug(f'Query measurement: {measurement}')

                    unique_count = query_result_tuple[1]
                    measurement = Measurement(Metric.UNIQUE_COUNT, column_name, unique_count)
                    measurements.append(measurement)
                    logging.debug(f'Query measurement: {measurement}')

                    # uniqueness
                    valid_count = self.scan_result.get(Metric.VALID_COUNT, column_name)
                    uniqueness = (distinct_count - 1) * 100 / (valid_count - 1)
                    measurement = Measurement(Metric.UNIQUENESS, column_name, uniqueness)
                    measurements.append(measurement)
                    logging.debug(f'Derived measurement: {measurement}')

                if scan_column.is_metric_enabled(Metric.MINS) and scan_column.numeric_expr is not None:

                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY {numeric_value_expr} ASC \n'
                           f'LIMIT {scan_column.mins_maxs_limit} \n')

                    rows = self.warehouse.execute_query_all(sql)
                    measurement = Measurement(Metric.MINS, column_name, [row[0] for row in rows])
                    measurements.append(measurement)
                    logging.debug(f'Query measurement: {measurement}')

                if self.configuration.is_metric_enabled(column_name, Metric.MAXS) \
                        and (scan_column.is_number or scan_column.is_column_numeric_text_format):

                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY {numeric_value_expr} DESC \n'
                           f'LIMIT {scan_column.mins_maxs_limit} \n')

                    rows = self.warehouse.execute_query_all(sql)
                    measurement = Measurement(Metric.MAXS, column_name, [row[0] for row in rows])
                    measurements.append(measurement)
                    logging.debug(f'Query measurement: {measurement}')

                if self.configuration.is_metric_enabled(column_name, Metric.FREQUENT_VALUES) \
                        and (scan_column.is_number or scan_column.is_column_numeric_text_format):

                    frequent_values_limit = self.configuration.get_frequent_values_limit(column_name)
                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY frequency DESC \n'
                           f'LIMIT {frequent_values_limit} \n')

                    rows = self.warehouse.execute_query_all(sql)
                    measurement = Measurement(Metric.FREQUENT_VALUES, column_name, [row[0] for row in rows])
                    measurements.append(measurement)
                    logging.debug(f'Query measurement: {measurement}')

        return measurements

    def run_tests(self):
        test_results = []
        for column_name in self.column_names:
            scan_column: ScanColumn = self.scan_columns[column_name]
            tests = scan_column.get_tests()
            if tests:
                column_measurement_values = {
                    measurement.metric: measurement.value
                    for measurement in self.scan_result.measurements
                    if measurement.column_name == column_name
                }
                for test in tests:
                    test_values = {metric: value for metric, value in column_measurement_values.items() if
                                   metric in test}
                    test_outcome = True if eval(test, test_values) else False
                    test_results.append(TestResult(test_outcome, test, test_values, column_name))
        return test_results

    def add_measurements(self, measurements):
        if measurements:
            self.scan_result.measurements.extend(measurements)
            if self.soda_client:
                self.soda_client.send_measurements(self.scan_reference, measurements)
