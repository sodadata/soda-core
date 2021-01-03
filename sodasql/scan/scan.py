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
from math import floor, ceil
from typing import List

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.custom_metric import CustomMetric
from sodasql.scan.dialect import Dialect
from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.test_result import TestResult
from sodasql.scan.warehouse import Warehouse
from sodasql.soda_client.soda_client import SodaClient


class Scan:

    def __init__(self,
                 warehouse: Warehouse,
                 scan_configuration: ScanConfiguration = None,
                 custom_metrics: List[CustomMetric] = None,
                 time: str = None,
                 soda_client: SodaClient = None,
                 variables: dict = None):
        self.soda_client: SodaClient = soda_client
        self.warehouse: Warehouse = warehouse
        self.dialect = warehouse.dialect

        self.configuration: ScanConfiguration = scan_configuration
        self.custom_metrics: List[CustomMetric] = custom_metrics

        # caches measurements that are not yet sent to soda and not yet added to self.scan_result
        # see also self.flush_measurements()
        self.measurements = []
        self.scan_result = ScanResult()

        self.qualified_table_name = self.dialect.qualify_table_name(scan_configuration.table_name)
        self.table_sample_clause = \
            f'\nTABLESAMPLE {scan_configuration.sample_method}({scan_configuration.sample_percentage})' \
            if scan_configuration.sample_percentage \
            else ''

        self.time_filter_sql = None
        if scan_configuration.time_filter_template:
            if not variables:
                raise RuntimeError(f'No variables provided while time_filter "{str(scan_configuration.time_filter)}" specified')
            self.time_filter_sql = scan_configuration.time_filter_template.render(variables)

        self.scan_reference = {
            'warehouseName': self.warehouse.name,
            'tableName': self.configuration.table_name,
            'time': time
        }

        self.columns: List[ColumnMetadata] = []
        self.column_names: List[str] = []
        # maps column names (lower case) to ScanColumn's
        self.scan_columns: dict = {}

    def execute(self) -> ScanResult:
        assert self.configuration.table_name, 'scan_configuration.table_name is required'

        self.query_columns_metadata()
        self.flush_measurements()

        self.query_aggregations()
        self.flush_measurements()

        self.query_group_by_value()
        self.flush_measurements()

        self.query_histograms()
        self.flush_measurements()

        self.run_tests()

        return self.scan_result

    def query_columns_metadata(self):
        sql = self.warehouse.dialect.sql_columns_metadata_query(self.configuration)
        column_tuples = self.warehouse.sql_fetchall(sql)
        self.columns = []
        for column_tuple in column_tuples:
            name = column_tuple[0]
            type = column_tuple[1]
            nullable = 'YES' == column_tuple[2].upper()
            self.columns.append(ColumnMetadata(name, type, nullable))
        logging.debug(str(len(self.columns))+' columns:')
        for column in self.columns:
            logging.debug(f'  {column.name} {column.type} {"" if column.nullable else "not null"}')

        self.column_names: List[str] = [column_metadata.name for column_metadata in self.columns]
        self.scan_columns: dict = {column.name.lower(): ScanColumn(self, column) for column in self.columns}

        self.add_query(Measurement(Metric.SCHEMA, value=self.columns))

    def query_aggregations(self):
        # This measurements list is used to match measurements with the query field order.
        # After query execution, the value of the measurements will be extracted from the query result and
        # the measurements will be added with self.add_query(measurement)
        measurements: List[Measurement] = []

        fields: List[str] = []

        dialect = self.warehouse.dialect

        if self.configuration.is_metric_enabled(Metric.ROW_COUNT):
            fields.append(dialect.sql_expr_count_all())
            measurements.append(Measurement(Metric.ROW_COUNT))

        # maps db column names (lower) to missing and invalid metric indices in the measurements
        # eg { 'colname': {'missing': 2, 'invalid': 3}, ...}
        column_metric_indices = {}

        for column_name_lower in self.scan_columns:
            metric_indices = {}
            column_metric_indices[column_name_lower] = metric_indices
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

            if scan_column.is_missing_enabled:
                metric_indices['non_missing'] = len(measurements)
                if scan_column.non_missing_condition:
                    fields.append(dialect.sql_expr_count_conditional(scan_column.non_missing_condition))
                else:
                    fields.append(dialect.sql_expr_count_column(scan_column.qualified_column_name))
                measurements.append(Measurement(Metric.VALUES_COUNT, column_name))

            if scan_column.is_valid_enabled:
                metric_indices['valid'] = len(measurements)
                if scan_column.non_missing_and_valid_condition:
                    fields.append(dialect.sql_expr_count_conditional(scan_column.non_missing_and_valid_condition))
                else:
                    fields.append(dialect.sql_expr_count_column(scan_column.qualified_column_name))
                measurements.append(Measurement(Metric.VALID_COUNT, column_name))

            if scan_column.is_text:
                length_expr = dialect.sql_expr_conditional(
                        scan_column.non_missing_and_valid_condition,
                        dialect.sql_expr_length(scan_column.qualified_column_name)) \
                    if scan_column.non_missing_and_valid_condition \
                    else dialect.sql_expr_length(scan_column.qualified_column_name)

                if self.configuration.is_metric_enabled(Metric.MIN_LENGTH, column_name):
                    fields.append(dialect.sql_expr_min(length_expr))
                    measurements.append(Measurement(Metric.MIN_LENGTH, column_name))

                if self.configuration.is_metric_enabled(Metric.MAX_LENGTH, column_name):
                    fields.append(dialect.sql_expr_max(length_expr))
                    measurements.append(Measurement(Metric.MAX_LENGTH, column_name))

            if scan_column.has_numeric_values:
                if scan_column.is_metric_enabled(Metric.MIN):
                    fields.append(dialect.sql_expr_min(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.MIN, column_name))

                if scan_column.is_metric_enabled(Metric.MAX):
                    fields.append(dialect.sql_expr_max(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.MAX, column_name))

                if scan_column.is_metric_enabled(Metric.AVG):
                    fields.append(dialect.sql_expr_avg(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.AVG, column_name))

                if scan_column.is_metric_enabled(Metric.SUM):
                    fields.append(dialect.sql_expr_sum(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.SUM, column_name))

                if scan_column.is_metric_enabled(Metric.VARIANCE):
                    fields.append(dialect.sql_expr_variance(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.VARIANCE, column_name))

                if scan_column.is_metric_enabled(Metric.STDDEV):
                    fields.append(dialect.sql_expr_stddev(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.STDDEV, column_name))

        if len(fields) > 0:
            sql = 'SELECT \n  ' + ',\n  '.join(fields) + ' \n' \
                  'FROM ' + self.qualified_table_name
            if self.table_sample_clause:
                sql += f'\n{self.table_sample_clause}'
            if self.time_filter_sql:
                sql += f'\nWHERE {self.time_filter_sql}'

            query_result_tuple = self.warehouse.sql_fetchone(sql)

            for i in range(0, len(measurements)):
                measurement = measurements[i]
                measurement.value = query_result_tuple[i]
                self.add_query(measurement)

            # Calculating derived measurements
            derived_measurements = []
            row_count_measurement = next((m for m in measurements if m.metric == Metric.ROW_COUNT), None)
            if row_count_measurement:
                row_count = row_count_measurement.value
                for column_name_lower in self.scan_columns:
                    scan_column = self.scan_columns[column_name_lower]
                    column_name = scan_column.column_name
                    metric_indices = column_metric_indices[column_name_lower]
                    non_missing_index = metric_indices.get('non_missing')
                    if non_missing_index is not None:
                        values_count = measurements[non_missing_index].value
                        missing_count = row_count - values_count
                        missing_percentage = missing_count * 100 / row_count
                        values_percentage = values_count * 100 / row_count
                        self.add_derived(Measurement(Metric.MISSING_PERCENTAGE, column_name, missing_percentage))
                        self.add_derived(Measurement(Metric.MISSING_COUNT, column_name, missing_count))
                        self.add_derived(Measurement(Metric.VALUES_PERCENTAGE, column_name, values_percentage))

                        valid_index = metric_indices.get('valid')
                        if valid_index is not None:
                            valid_count = measurements[valid_index].value
                            invalid_count = row_count - missing_count - valid_count
                            invalid_percentage = invalid_count * 100 / row_count
                            valid_percentage = valid_count * 100 / row_count
                            self.add_derived(Measurement(Metric.INVALID_PERCENTAGE, column_name, invalid_percentage))
                            self.add_derived(Measurement(Metric.INVALID_COUNT, column_name, invalid_count))
                            self.add_derived(Measurement(Metric.VALID_PERCENTAGE, column_name, valid_percentage))

    def query_group_by_value(self):
        for column_name_lower in self.scan_columns:
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

            if scan_column.is_any_metric_enabled(
                    [Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT,
                     Metric.MINS, Metric.MAXS, Metric.FREQUENT_VALUES, Metric.DUPLICATE_COUNT]):

                group_by_cte = scan_column.get_group_by_cte()
                numeric_value_expr = scan_column.get_group_by_cte_numeric_value_expression()

                if self.configuration.is_any_metric_enabled(
                        [Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT, Metric.DUPLICATE_COUNT],
                        column_name):

                    sql = (f'{group_by_cte} \n'
                           f'SELECT COUNT(*), \n'
                           f'       COUNT(CASE WHEN frequency = 1 THEN 1 END), \n'
                           f'       SUM(frequency) \n'
                           f'FROM group_by_value')

                    query_result_tuple = self.warehouse.sql_fetchone(sql)
                    distinct_count = query_result_tuple[0]
                    unique_count = query_result_tuple[1]
                    valid_count = query_result_tuple[2] if query_result_tuple[2] else 0
                    duplicate_count = distinct_count - unique_count
                    uniqueness = (distinct_count - 1) * 100 / (valid_count - 1)

                    self.add_query(Measurement(Metric.DISTINCT, column_name, distinct_count))
                    self.add_query(Measurement(Metric.UNIQUE_COUNT, column_name, unique_count))
                    self.add_derived(Measurement(Metric.DUPLICATE_COUNT, column_name, duplicate_count))
                    self.add_derived(Measurement(Metric.UNIQUENESS, column_name, uniqueness))

                if scan_column.is_metric_enabled(Metric.MINS) and scan_column.numeric_expr is not None:

                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY {numeric_value_expr} ASC \n'
                           f'LIMIT {scan_column.mins_maxs_limit} \n')

                    rows = self.warehouse.sql_fetchall(sql)
                    mins = [row[0] for row in rows]
                    self.add_query(Measurement(Metric.MINS, column_name, mins))

                if self.configuration.is_metric_enabled(Metric.MAXS, column_name) \
                        and (scan_column.is_number or scan_column.is_column_numeric_text_format):

                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY {numeric_value_expr} DESC \n'
                           f'LIMIT {scan_column.mins_maxs_limit} \n')

                    rows = self.warehouse.sql_fetchall(sql)
                    maxs = [row[0] for row in rows]
                    self.add_query(Measurement(Metric.MAXS, column_name, maxs))

                if self.configuration.is_metric_enabled(Metric.FREQUENT_VALUES, column_name) \
                        and (scan_column.is_number or scan_column.is_column_numeric_text_format):

                    frequent_values_limit = self.configuration.get_frequent_values_limit(column_name)
                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY frequency DESC \n'
                           f'LIMIT {frequent_values_limit} \n')

                    rows = self.warehouse.sql_fetchall(sql)
                    frequent_values = [row[0] for row in rows]
                    self.add_query(Measurement(Metric.FREQUENT_VALUES, column_name, frequent_values))

    def query_histograms(self):
        for column_name_lower in self.scan_columns:
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

            if scan_column.is_metric_enabled(Metric.HISTOGRAM):

                buckets: int = scan_column.get_histogram_buckets()

                min_value = scan_column.get_metric_value(Metric.MIN)
                max_value = scan_column.get_metric_value(Metric.MAX)

                if scan_column.has_numeric_values and min_value and max_value and min_value < max_value:
                    # Build the histogram query
                    min_value = floor(min_value * 1000) / 1000
                    max_value = ceil(max_value * 1000) / 1000
                    bucket_width = (max_value - min_value) / buckets

                    boundary = min_value
                    boundaries = [min_value]
                    for i in range(0, buckets):
                        boundary += bucket_width
                        boundaries.append(round(boundary, 3))

                    group_by_cte = scan_column.get_group_by_cte()
                    numeric_value_expr = scan_column.get_group_by_cte_numeric_value_expression()

                    field_clauses = []
                    for i in range(0, buckets):
                        lower_bound = '' if i == 0 else f'{boundaries[i]} <= {numeric_value_expr}'
                        upper_bound = '' if i == buckets - 1 else f'{numeric_value_expr} < {boundaries[i + 1]}'
                        optional_and = '' if lower_bound == '' or upper_bound == '' else ' and '
                        field_clauses.append(f'SUM(CASE WHEN {lower_bound}{optional_and}{upper_bound} THEN frequency END)')

                    fields = ',\n  '.join(field_clauses)

                    sql = (f'{group_by_cte} \n'
                           f'SELECT \n'
                           f'  {fields} \n'
                           f'FROM group_by_value')

                    if self.time_filter_sql:
                        sql += f' \nWHERE {self.scan.time_filter_sql}'

                    row = self.warehouse.sql_fetchone(sql)

                    # Process the histogram query
                    frequencies = []
                    for i in range(0, buckets):
                        frequency = row[i]
                        frequencies.append(0 if not frequency else int(frequency))
                    histogram = {
                        'boundaries': boundaries,
                        'frequencies': frequencies
                    }

                    self.add_query(Measurement(Metric.HISTOGRAM, column_name, histogram))

    def run_tests(self):
        test_results = []
        self.scan_result.test_results = test_results

        for column_name_lower in self.scan_columns:
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

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

    def add_query(self, measurement: Measurement):
        return self.add_measurement('Query', measurement)

    def add_derived(self, measurement: Measurement):
        return self.add_measurement('Derived', measurement)

    def add_measurement(self, measurement_type: str, measurement: Measurement):
        self.measurements.append(measurement)
        logging.debug(f'{measurement_type} measurement: {measurement}')

    def flush_measurements(self):
        if len(self.measurements) > 0:
            self.scan_result.measurements.extend(self.measurements)
            if self.soda_client:
                measurements_json = [measurement.to_json() for measurement in self.measurements]
                self.soda_client.send_measurements(self.scan_reference, measurements_json)
            self.measurements = []
